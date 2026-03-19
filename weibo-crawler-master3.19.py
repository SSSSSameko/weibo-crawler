# -*- coding: utf-8 -*-
import argparse
import html
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("weibo_monitor")

ACTIVE_TIMEOUT = 3600

# ========== config ==========

@dataclass
class Config:
    cookie: str = ""
    priority_uid: str = ""
    normal_uids: list[str] = field(default_factory=list)
    proxy: str = ""
    wechat_corp_id: str = ""
    wechat_agent_id: int = 0
    wechat_app_secret: str = ""
    wechat_to_user: str = "@all"
    poll_interval_min: int = 61
    poll_interval_max: int = 70
    comment_delay_min: float = 1.0
    comment_delay_max: float = 3.0
    weibo_fetch_count: int = 30
    max_monitored_weibos: int = 100
    comment_max_pages: int = 5
    push_days_threshold: int = 6
    request_timeout: int = 15
    retry_total: int = 3
    retry_backoff: float = 1.0
    data_dir: str = "data"
    risk_control: bool = True
    heartbeat_interval: int = 60


def load_config(path="config.json"):
    cfg = Config()
    cp = Path(path)
    if not cp.exists():
        log.warning("配置文件 %s 不存在，使用默认值", path)
        return cfg
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        wc = data.get("wechat", {})
        cfg.cookie = data.get("cookie", "")
        cfg.priority_uid = data.get("priority_uid", "")
        cfg.normal_uids = data.get("normal_uids", [])
        cfg.proxy = data.get("proxy", "")
        cfg.wechat_corp_id = wc.get("corp_id", "")
        cfg.wechat_agent_id = wc.get("agent_id", 0)
        cfg.wechat_app_secret = wc.get("app_secret", "")
        cfg.wechat_to_user = wc.get("to_user", "@all")
        for k in ("poll_interval_min", "poll_interval_max", "comment_delay_min",
                   "comment_delay_max", "weibo_fetch_count", "max_monitored_weibos",
                   "comment_max_pages", "push_days_threshold",
                   "risk_control", "heartbeat_interval"):
            if k in data:
                setattr(cfg, k, data[k])
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log.warning("配置文件炸了: %s", e)

    env_override = {
        "WEIBO_COOKIE": "cookie", "MONITOR_PRIORITY_UID": "priority_uid",
        "WECHAT_CORP_ID": "wechat_corp_id", "WECHAT_AGENT_ID": "wechat_agent_id",
        "WECHAT_APP_SECRET": "wechat_app_secret", "WECHAT_TO_USER": "wechat_to_user",
        "WEIBO_PROXY": "proxy",
    }
    for env_k, attr in env_override.items():
        v = os.environ.get(env_k)
        if not v:
            continue
        if attr == "wechat_agent_id":
            try:
                v = int(v)
            except ValueError:
                continue
        setattr(cfg, attr, v)

    uids_env = os.environ.get("MONITOR_NORMAL_UIDS")
    if uids_env:
        try:
            cfg.normal_uids = json.loads(uids_env)
        except json.JSONDecodeError:
            pass
    return cfg


# ========== throttle ==========

class Throttler:
    def __init__(self, base_min, base_max, enabled=True):
        self.base_min = base_min
        self.base_max = base_max
        self.enabled = enabled
        self._mul = 1.0
        self._errs = 0
        self._cool_until = 0

    @property
    def cooling(self):
        return time.time() < self._cool_until

    def delay(self):
        return min(random.uniform(self.base_min, self.base_max) * self._mul, 300)

    def ok(self):
        if not self.enabled:
            return
        self._errs = 0
        if self._mul > 1.0:
            self._mul = max(1.0, self._mul * 0.9)

    def ratelimited(self):
        if not self.enabled:
            return
        self._errs += 1
        self._mul = min(32.0, 2 ** self._errs)
        self._cool_until = time.time() + min(300, 10 * (2 ** self._errs))
        log.warning("被风控了，降频 %.1fx 冷却 %ds", self._mul, int(self._cool_until - time.time()))

    def err(self):
        if not self.enabled:
            return
        self._errs += 1
        if self._errs >= 3:
            self._mul = min(16.0, self._mul * 2)
            log.warning("连续挂了 %d 次，降频 %.1fx", self._errs, self._mul)

    def wait_if_cooling(self):
        if not self.enabled or not self.cooling:
            return
        left = self._cool_until - time.time()
        if left > 0:
            log.info("还在冷却，等 %.0fs", left)
            time.sleep(left)

    def status(self):
        if not self.enabled:
            return "关闭"
        return "正常" if self._mul <= 1.0 else f"降频{self._mul:.1f}x"


# ========== db ==========

_TAG_RE = re.compile(r"<[^>]+>")

class DB:
    def __init__(self, data_dir):
        self.path = Path(data_dir) / "weibo_monitor.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._lk = threading.Lock()
        self._init()

    def _init(self):
        with self._lk:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    uid TEXT NOT NULL, weibo_id TEXT NOT NULL, comment_id TEXT NOT NULL,
                    comment_type TEXT NOT NULL, parent_comment_id TEXT DEFAULT '',
                    user_name TEXT DEFAULT '', content TEXT DEFAULT '',
                    create_time TEXT DEFAULT '', like_count INTEGER DEFAULT 0,
                    crawl_time TEXT NOT NULL, UNIQUE(uid, comment_id));
                CREATE TABLE IF NOT EXISTS push_cache (
                    uid TEXT NOT NULL, cache_key TEXT NOT NULL,
                    item_id TEXT NOT NULL, push_time TEXT NOT NULL,
                    PRIMARY KEY(uid, cache_key, item_id));
                CREATE TABLE IF NOT EXISTS weibo_state (
                    uid TEXT NOT NULL, state_key TEXT NOT NULL,
                    item_id TEXT NOT NULL, PRIMARY KEY(uid, state_key, item_id));
                CREATE TABLE IF NOT EXISTS heartbeat (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    last_beat TEXT NOT NULL, loop_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running');
                CREATE INDEX IF NOT EXISTS idx_comments_uid ON comments(uid);
                CREATE INDEX IF NOT EXISTS idx_push_cache_uid ON push_cache(uid);
            """)
            self._conn.commit()

    def save_comment(self, uid, weibo_id, info, ctype, parent=""):
        cid = info.get("comment_id") or info.get("sub_comment_id")
        if not cid:
            return False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lk:
            try:
                self._conn.execute(
                    "INSERT INTO comments (uid,weibo_id,comment_id,comment_type,"
                    "parent_comment_id,user_name,content,create_time,like_count,crawl_time) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (uid, weibo_id, cid, ctype, parent, info["user_name"],
                     html.unescape(_TAG_RE.sub("", str(info.get("content", "")))).strip(),
                     info.get("create_time", ""), info.get("like_count", 0), now))
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def is_pushed(self, uid, key, item_id):
        if not item_id:
            return False
        with self._lk:
            return self._conn.execute(
                "SELECT 1 FROM push_cache WHERE uid=? AND cache_key=? AND item_id=?",
                (uid, key, item_id)).fetchone() is not None

    def mark_pushed(self, uid, key, item_id):
        if not item_id:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lk:
            self._conn.execute(
                "INSERT OR REPLACE INTO push_cache (uid,cache_key,item_id,push_time) VALUES (?,?,?,?)",
                (uid, key, item_id, now))
            self._conn.commit()

    def load_state(self, uid, key):
        with self._lk:
            return {r[0] for r in self._conn.execute(
                "SELECT item_id FROM weibo_state WHERE uid=? AND state_key=?", (uid, key))}

    def save_state(self, uid, key, ids):
        with self._lk:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("DELETE FROM weibo_state WHERE uid=? AND state_key=?", (uid, key))
                self._conn.executemany(
                    "INSERT INTO weibo_state (uid,state_key,item_id) VALUES (?,?,?)",
                    [(uid, key, wid) for wid in ids])
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def heartbeat(self, loop=0, status="running"):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lk:
            self._conn.execute(
                "INSERT INTO heartbeat (id,last_beat,loop_count,status) VALUES (1,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET last_beat=?,loop_count=?,status=?",
                (now, loop, status, now, loop, status))
            self._conn.commit()

    def get_heartbeat(self):
        with self._lk:
            row = self._conn.execute("SELECT last_beat,loop_count,status FROM heartbeat WHERE id=1").fetchone()
            return {"last_beat": row[0], "loop_count": row[1], "status": row[2]} if row else None

    def trim_comments(self, uid, keep):
        with self._lk:
            cnt = self._conn.execute("SELECT COUNT(*) FROM comments WHERE uid=?", (uid,)).fetchone()[0]
            if cnt > keep:
                self._conn.execute(
                    "DELETE FROM comments WHERE uid=? AND id IN "
                    "(SELECT id FROM comments WHERE uid=? ORDER BY id ASC LIMIT ?)",
                    (uid, uid, cnt - keep))
                self._conn.commit()

    def trim_pushes(self, uid, key, keep):
        with self._lk:
            cnt = self._conn.execute(
                "SELECT COUNT(*) FROM push_cache WHERE uid=? AND cache_key=?", (uid, key)).fetchone()[0]
            if cnt > keep:
                self._conn.execute(
                    "DELETE FROM push_cache WHERE uid=? AND cache_key=? AND item_id IN "
                    "(SELECT item_id FROM push_cache WHERE uid=? AND cache_key=? ORDER BY push_time ASC LIMIT ?)",
                    (uid, key, uid, key, cnt - keep))
                self._conn.commit()

    def trim_state(self, uid, key, keep):
        with self._lk:
            cnt = self._conn.execute(
                "SELECT COUNT(*) FROM weibo_state WHERE uid=? AND state_key=?", (uid, key)).fetchone()[0]
            if cnt > keep:
                self._conn.execute(
                    "DELETE FROM weibo_state WHERE uid=? AND state_key=? AND item_id NOT IN "
                    "(SELECT item_id FROM weibo_state WHERE uid=? AND state_key=? ORDER BY rowid DESC LIMIT ?)",
                    (uid, key, uid, key, keep))
                self._conn.commit()

    def close(self):
        self._conn.close()


# ========== utils ==========

_FMTS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%a %b %d %H:%M:%S %z %Y", "%a %b %d %H:%M:%S %Y"]
_TZ_PAT = re.compile(r"\s*[+-]\d{4}\s*$")
_REL_PAT = re.compile(r"(\d+)\s*(分钟|小时|天|秒)前")
_UNIT = {"分钟": "minutes", "小时": "hours", "天": "days", "秒": "seconds"}


def parse_time(raw):
    if not raw or raw == "未知时间":
        return "未知时间"
    s = str(raw).strip()
    for fmt in _FMTS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    s = _TZ_PAT.sub("", s).replace("GMT", "").replace("UTC", "").strip()
    for fmt in _FMTS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    if re.match(r"^\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}$", s):
        try:
            return datetime.strptime(f"{datetime.now().year}-{s}", "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    m = _REL_PAT.search(str(raw))
    if m:
        n, unit = int(m.group(1)), _UNIT.get(m.group(2))
        if unit:
            return (datetime.now() - timedelta(**{unit: n})).strftime("%Y-%m-%d %H:%M:%S")
    if "刚刚" in str(raw):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.warning("无法解析时间格式: %s", raw)
    return "未知时间"


def strip_html(text):
    if not text:
        return ""
    return html.unescape(_TAG_RE.sub("", str(text))).strip()


def make_session(cfg):
    s = requests.Session()
    if cfg.proxy:
        s.proxies = {"http": cfg.proxy, "https": cfg.proxy}
        log.info("走代理: %s", cfg.proxy)
    retry = Retry(total=cfg.retry_total, backoff_factor=cfg.retry_backoff,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    adp = HTTPAdapter(max_retries=retry)
    s.mount("https://", adp)
    s.mount("http://", adp)
    return s


# ========== 企微推送 ==========

class Wechat:
    def __init__(self, cfg, session):
        self.cfg = cfg
        self.s = session
        self._tok = ""
        self._exp = 0
        self._fails = 0
        self._cd = 0

    def _fetch_token(self):
        if time.time() < self._cd:
            return ""
        try:
            r = self.s.post("https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                json={"corpid": self.cfg.wechat_corp_id, "corpsecret": self.cfg.wechat_app_secret},
                timeout=self.cfg.request_timeout)
            d = r.json()
            if d.get("errcode") == 0:
                self._tok = d["access_token"]
                self._exp = time.time() + d.get("expires_in", 7200) - 300
                self._fails = 0
                return self._tok
            log.error("企微token拿不到 errcode=%s", d.get("errcode"))
            self._fails += 1
            self._cd = time.time() + min(300, 10 * 2 ** self._fails)
        except requests.RequestException as e:
            log.error("企微token异常: %s", e)
            self._fails += 1
            self._cd = time.time() + min(300, 10 * self._fails)
        return ""

    @property
    def token(self):
        return self._tok if self._tok and time.time() < self._exp else self._fetch_token()

    def send(self, title, body, retries=2):
        for i in range(retries + 1):
            tok = self.token
            if not tok:
                return False
            try:
                r = self.s.post(
                    f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={tok}",
                    json={"touser": self.cfg.wechat_to_user, "agentid": self.cfg.wechat_agent_id,
                          "msgtype": "text", "text": {"content": f"【{title}】\n{body}"}},
                    timeout=self.cfg.request_timeout)
                d = r.json()
                if d.get("errcode") == 0:
                    return True
                if d.get("errcode") in (40014, 42001) and i < retries:
                    self._tok, self._exp = "", 0
                    continue
                log.warning("推送凉了 errcode=%s: %s", d.get("errcode"), d.get("errmsg"))
                return False
            except requests.RequestException as e:
                log.warning("推送炸了: %s", e)
                if i < retries:
                    time.sleep(1)
                    continue
                return False
        return False


# ========== 监控 ==========

class WeiboMonitor:
    LIST_URL = "https://weibo.com/ajax/statuses/mymblog"
    CMT_URL = "https://weibo.com/ajax/statuses/buildComments"
    PROF_URL = "https://weibo.com/ajax/profile/info"

    _UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    def __init__(self, cfg):
        self.cfg = cfg

        if not cfg.cookie or not cfg.cookie.strip():
            log.error("Cookie 为空")
            sys.exit(1)
        has_sub = "SUB=" in cfg.cookie
        has_scf = "SCF=" in cfg.cookie
        has_subp = "SUBP=" in cfg.cookie
        log.info("Cookie 已加载 (len=%d) | SUB=%s SCF=%s SUBP=%s",
                 len(cfg.cookie), has_sub, has_scf, has_subp)

        self.ses = make_session(cfg)
        self.wc = Wechat(cfg, self.ses)
        self.db = DB(cfg.data_dir)
        self.thr = Throttler(cfg.poll_interval_min, cfg.poll_interval_max, cfg.risk_control)

        log_path = str(Path(cfg.data_dir) / "weibo_monitor.log")
        if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == log_path
                   for h in log.handlers):
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
            log.addHandler(fh)

        self.monitored: dict[str, set] = {}
        self.last_seen: dict[str, set] = {}
        self.nicknames: dict[str, str] = {}
        self._active_weibos: dict[str, dict[str, float]] = {}
        self._loop = 0
        self._cold_start: set[str] = set()

        self.prio = cfg.priority_uid.strip()
        self.uids = self._dedup_uids()
        if not self.uids and not self.prio:
            log.error("一个 UID 都没配")
            sys.exit(1)

        all_uids = self.uids[:]
        if self.prio and self.prio not in all_uids:
            all_uids.append(self.prio)

        for uid in all_uids:
            self.monitored[uid] = self.db.load_state(uid, "monitored")
            self.last_seen[uid] = self.db.load_state(uid, "last_seen")
            self._active_weibos[uid] = {}
            if not self.last_seen[uid]:
                self._cold_start.add(uid)
            self._nickname(uid)

        log.info("开始视奸")
        if cfg.proxy:
            log.info("代理: %s", cfg.proxy)

    def _dedup_uids(self):
        seen, out = set(), []
        p = self.cfg.priority_uid.strip()
        if p:
            seen.add(p)
        for uid in self.cfg.normal_uids:
            uid = uid.strip()
            if uid and uid not in seen:
                seen.add(uid)
                out.append(uid)
        return out

    def _build_queue(self):
        p = self.prio
        normal = [u.strip() for u in self.cfg.normal_uids if u.strip()]
        active = self._get_active_entries()

        if not p and not normal:
            return [(uid, None) for uid in self.uids]

        if not normal:
            queue = [(a_uid, a_wid) for a_uid, a_wid in active]
            queue.append((p, None) if p else (self.uids[0], None))
            return queue

        if not p:
            queue = [(a_uid, a_wid) for a_uid, a_wid in active]
            for uid in self.uids:
                queue.append((uid, None))
            return queue

        queue = []
        for uid in normal:
            for a_uid, a_wid in active:
                queue.append((a_uid, a_wid))
            queue.append((p, None))
            queue.append((uid, None))
        return queue

    # -- active weibo --

    def _touch_active(self, uid, wid):
        if uid not in self._active_weibos:
            self._active_weibos[uid] = {}
        self._active_weibos[uid][wid] = time.time()

    def _prune_stale_active(self):
        now = time.time()
        for uid in list(self._active_weibos):
            expired = [wid for wid, ts in self._active_weibos[uid].items()
                       if now - ts > ACTIVE_TIMEOUT]
            for wid in expired:
                del self._active_weibos[uid][wid]
                log.info("活跃超时移出: uid=%s wid=%s", uid, wid)

    def _get_active_entries(self):
        return [(uid, wid) for uid, weibos in self._active_weibos.items() for wid in weibos]

    # -- network --

    def _hdrs(self, uid=None, mid=None):
        if mid and uid:
            ref = f"https://weibo.com/{uid}/weibo?mid={mid}"
        elif uid:
            ref = f"https://weibo.com/{uid}"
        else:
            ref = "https://weibo.com/"
        return {
            "User-Agent": self._UA,
            "Cookie": self.cfg.cookie,
            "Referer": ref,
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        }

    def _get(self, url, params=None, uid=None, mid=None):
        self.thr.wait_if_cooling()
        try:
            r = self.ses.get(url, params=params, headers=self._hdrs(uid, mid), timeout=self.cfg.request_timeout)
        except requests.RequestException as e:
            self.thr.err()
            log.warning("请求挂了: %s", e)
            raise
        if r.status_code in (403, 418):
            self.thr.ratelimited()
        elif r.status_code == 200:
            self.thr.ok()
        else:
            self.thr.err()
        return r

    def _nickname(self, uid):
        if uid in self.nicknames:
            return self.nicknames[uid]
        try:
            r = self._get(f"{self.PROF_URL}?uid={uid}", uid=uid)
            d = r.json() if r.text.strip() else {}
            nick = d.get("data", {}).get("user", {}).get("screen_name", f"用户{uid}")
        except requests.RequestException:
            nick = f"用户{uid}"
        self.nicknames[uid] = nick
        return nick

    # -- comments --

    def _pull_comments(self, uid, weibo_id, page=1):
        url = f"{self.CMT_URL}?is_show_bulletin=2&is_mix=0&id={weibo_id}&is_show_cmt_num=0&comment_type=0&page={page}&count=20&uid={uid}"
        try:
            r = self._get(url, uid=uid, mid=weibo_id)
            log.info("[一级评论] weibo_id=%s status=%d", weibo_id, r.status_code)
            if r.status_code != 200:
                log.warning("[一级评论] 非200 status=%d body=%s", r.status_code, r.text[:300])
                return [], False
            d = r.json() if r.text.strip() else {}
            raw_data = d.get("data", [])
            log.info("[一级评论] weibo_id=%s 条数=%d has_more=%s", weibo_id, len(raw_data), d.get("has_more"))
            items = [{
                "comment_id": str(c.get("id", "")),
                "user_name": c.get("user", {}).get("screen_name", ""),
                "content": c.get("text_raw") or c.get("text", ""),
                "create_time": c.get("created_at", "未知时间"),
                "like_count": c.get("like_counts", 0),
            } for c in raw_data]
            return items, bool(d.get("has_more", False))
        except requests.RequestException as e:
            log.error("一级评论请求失败 weibo=%s: %s", weibo_id, e)
            return [], False

    def _pull_replies(self, uid, weibo_id, cid):
        url = "https://m.weibo.cn/comments/hotflowChild"
        params = {"cid": cid, "mid": weibo_id, "max_id_type": 0}
        try:
            self.thr.wait_if_cooling()
            r = self.ses.get(url, params=params, headers={
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
                "Cookie": self.cfg.cookie,
                "Referer": f"https://m.weibo.cn/detail/{weibo_id}",
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=self.cfg.request_timeout)
            log.info("[二级评论] cid=%s status=%d", cid, r.status_code)
            if r.status_code != 200:
                self.thr.err()
                log.warning("[二级评论] 非200 cid=%s status=%d body=%s", cid, r.status_code, r.text[:300])
                return []
            self.thr.ok()
            d = r.json() if r.text.strip() else {}
            raw_data = d.get("data", [])
            log.info("[二级评论] cid=%s 条数=%d", cid, len(raw_data))
            if not raw_data:
                return []
            return [{
                "sub_comment_id": str(c.get("id", "")),
                "user_name": c.get("user", {}).get("screen_name", ""),
                "content": c.get("text_raw") or c.get("text", ""),
                "create_time": c.get("created_at", "未知时间"),
                "like_count": c.get("like_count", 0),
            } for c in raw_data]
        except requests.RequestException as e:
            self.thr.err()
            log.error("二级评论请求失败 cid=%s: %s", cid, e)
            return []

    def _all_comments(self, uid, wid):
        out = []
        for pg in range(1, self.cfg.comment_max_pages + 1):
            items, more = self._pull_comments(uid, wid, pg)
            if not items:
                break
            out.extend(items)
            if not more:
                break
            time.sleep(0.3)
        return out

    def _watch_comments(self, uid, wid):
        nick = self._nickname(uid)
        lo, hi = self.cfg.comment_delay_min, self.cfg.comment_delay_max
        has_new = False

        for fc in self._all_comments(uid, wid):
            cid = fc["comment_id"]

            if not self.db.is_pushed(uid, "first_comment", cid):
                ft = parse_time(fc["create_time"])
                if self._fresh(ft):
                    self.db.save_comment(uid, wid, fc, "一级评论")
                    self.wc.send(f"{nick} 新一级评论",
                        f"用户：{nick}\n微博ID：{wid}\n评论用户：{fc['user_name']}\n时间：{ft}\n"
                        f"内容：{strip_html(fc['content'])}")
                    self.db.mark_pushed(uid, "first_comment", cid)
                    has_new = True

            time.sleep(random.uniform(lo, hi) / 2)
            replies = self._pull_replies(uid, wid, cid)
            log.info("[二级评论流程] cid=%s 抓到%d条", cid, len(replies))

            for sc in replies:
                sid = sc["sub_comment_id"]
                if self.db.is_pushed(uid, "second_comment", sid):
                    continue
                st = parse_time(sc["create_time"])
                if not self._fresh(st):
                    continue
                self.db.save_comment(uid, wid, sc, "二级评论", cid)
                self.wc.send(f"{nick} 新二级评论",
                    f"用户：{nick}\n微博ID：{wid}\n父评论ID：{cid}\n评论用户：{sc['user_name']}\n"
                    f"时间：{st}\n内容：{strip_html(sc['content'])}")
                self.db.mark_pushed(uid, "second_comment", sid)
                has_new = True
                time.sleep(random.uniform(lo, hi) / 2)

            time.sleep(random.uniform(lo, hi))

        if has_new:
            self._touch_active(uid, wid)
            log.info("新活动，加入优先队列: %s/%s", nick, wid)

    # -- weibo --

    def _fresh(self, ts):
        if not ts or ts == "未知时间":
            return False
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") >= \
                   datetime.now() - timedelta(days=self.cfg.push_days_threshold)
        except ValueError:
            return False

    def _fetch_weibos(self, uid):
        try:
            r = self._get(self.LIST_URL, params={"uid": uid, "page": 1, "count": self.cfg.weibo_fetch_count})
            if r.status_code != 200:
                log.warning("拉微博列表失败 uid=%s status=%d", uid, r.status_code)
                return

            d = r.json() if r.text.strip() else {}
            weibos = [w for w in d.get("data", {}).get("list", [])
                      if str(w.get("original_author_uid", w.get("user", {}).get("id", ""))) == uid]

            current = set()
            nick = self._nickname(uid)
            is_cold = uid in self._cold_start

            for w in weibos:
                wid = str(w.get("id", ""))
                if not wid:
                    continue
                current.add(wid)

                if is_cold:
                    continue

                wt = parse_time(w.get("created_at", "未知时间"))
                if self.db.is_pushed(uid, "weibo", wid) or not self._fresh(wt):
                    continue

                content = strip_html(w.get("text_raw") or w.get("text", ""))
                self.wc.send(f"{nick} 新微博",
                    f"用户：{nick}\n微博ID：{wid}\n时间：{wt}\n内容：{content}\n"
                    f"赞{w.get('attitudes_count', 0)} 评{w.get('comments_count', 0)} 转{w.get('reposts_count', 0)}")
                self.db.mark_pushed(uid, "weibo", wid)
                time.sleep(random.uniform(self.cfg.comment_delay_min, self.cfg.comment_delay_max))
                self._watch_comments(uid, wid)

            self.last_seen[uid] = current
            self.db.save_state(uid, "last_seen", current)
            self.monitored[uid].update(current)
            self.db.save_state(uid, "monitored", self.monitored[uid])

            if is_cold:
                log.info("冷启动 %s (%s)，记了 %d 条基线", nick, uid, len(current))
                self._cold_start.discard(uid)

        except requests.RequestException as e:
            log.error("抓微博失败 %s: %s", self.nicknames.get(uid, uid), e)

    def _sweep_old(self, uid):
        lo, hi = self.cfg.comment_delay_min, self.cfg.comment_delay_max
        active_wids = set(self._active_weibos.get(uid, {}).keys())
        for wid in list(self.monitored.get(uid, set())):
            if wid in active_wids:
                continue
            time.sleep(random.uniform(lo, hi) / 2)
            try:
                self._watch_comments(uid, wid)
            except Exception as e:
                log.warning("扫旧微博出错 weibo=%s: %s", wid, e)

    def _gc(self, uid):
        cap = self.cfg.max_monitored_weibos * 10
        self.db.trim_comments(uid, cap)
        for k in ("weibo", "first_comment", "second_comment"):
            self.db.trim_pushes(uid, k, cap)
        ids = self.monitored.get(uid, set())
        if len(ids) > self.cfg.max_monitored_weibos:
            kept = set(list(ids)[-self.cfg.max_monitored_weibos:])
            self.monitored[uid] = kept
            self.db.save_state(uid, "monitored", kept)
            self.db.trim_state(uid, "monitored", self.cfg.max_monitored_weibos)
            self.db.trim_state(uid, "last_seen", self.cfg.max_monitored_weibos)

    # -- heartbeat --

    def _heartbeat_loop(self):
        while True:
            try:
                self.db.heartbeat(self._loop, "running")
            except Exception:
                pass
            time.sleep(self.cfg.heartbeat_interval)

    # -- main loop --

    def run(self):
        log.info("开始视奸，活跃超时: %ds", ACTIVE_TIMEOUT)
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        while True:
            self._loop += 1
            active_count = sum(len(v) for v in self._active_weibos.values())
            log.info("=== 第 %d 轮 [%s] 活跃: %d ===", self._loop, self.thr.status(), active_count)

            self._prune_stale_active()
            queue = self._build_queue()
            log.info("队列: %s", [(self.nicknames.get(uid, uid), wid) for uid, wid in queue])

            for uid, wid in queue:
                nick = self._nickname(uid)
                try:
                    if wid:
                        log.info("优先检查 %s/%s 评论", nick, wid)
                        self._watch_comments(uid, wid)
                        d = self.thr.delay() / 2
                        log.info("风控延迟 %.0fs", d)
                        time.sleep(d)
                    else:
                        log.info("轮到 %s (%s)", nick, uid)
                        self._fetch_weibos(uid)
                        self._sweep_old(uid)
                        self._gc(uid)
                        d = self.thr.delay()
                        log.info("风控延迟 %.0fs", d)
                        time.sleep(d)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    log.error("出事了 %s: %s", nick, e)
                    self.thr.err()
                    time.sleep(self.thr.delay())


# ========== main ==========

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", handlers=[logging.StreamHandler()])

    ap = argparse.ArgumentParser(description="微博监控 + 企微推送")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()

    cfg = load_config(args.config)
    mon = None

    def bye(signum, _):
        log.info(" %d停止运行，保存", signum)
        if mon:
            mon.db.heartbeat(mon._loop, "stopped")
            mon.db.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, bye)
    signal.signal(signal.SIGTERM, bye)

    mon = WeiboMonitor(cfg)
    mon.run()


if __name__ == "__main__":
    main()
