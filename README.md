# weibo-crawler

实现监控指定用户的微博动态和评论并push。

## 基于 weibo-crawler 重写

## 修复-优化


- TIME_THRESHOLD
- 代理支持
- SQLite 存储
- 评论抓取分页
- HTTP 自动重试
- 封装为 WeiboMonitor 类
- 使用 logging 模块替代 print
- 请求加入自动重试
- 轮询大量优化_build_queue
- 内存定期清理
- 心跳监控
- 风控自适应降频（好像没什么用，一般出问题基本上就是cookie过期了


## 安装

```bash
git clone https://github.com/SSSSSameko/weibo-crawler.git
cd weibo-crawler
pip install -r requirements.txt
```



## 配置

复制示例配置并编辑：

```bash
cp config.example.json config.json
```

### config.json

```json
{
  "cookie": "你的微博 Cookie",
  "priority_uid": "优先监控的用户 UID",
  "normal_uids": ["普通用户UID1", "普通用户UID2"],
  "proxy": "",
  "wechat": {
    "corp_id": "企业微信 CorpID",
    "agent_id": 1000002,
    "app_secret": "应用 Secret",
    "to_user": "@all"
  },
  "poll_interval_min": 61,
  "poll_interval_max": 70,
  "comment_delay_min": 1,
  "comment_delay_max": 3,
  "weibo_fetch_count": 30,
  "max_monitored_weibos": 100,
  "comment_max_pages": 5,
  "push_days_threshold": 6,
  "risk_control": true,
  "heartbeat_interval": 60
}
```

### 字段说明

| 字段 | 说明 |
|------|------|
| `cookie` | 微博 Cookie（必须包含 `SUB=` `SCF=` `SUBP=`） |
| `priority_uid` | 优先监控的 UID，轮询频率更高（出现两次） |
| `normal_uids` | 普通监控 UID 列表 |
| `proxy` | HTTP 代理地址，留空不走代理 |
| `wechat.corp_id` | 企业微信 CorpID |
| `wechat.agent_id` | 应用 AgentId |
| `wechat.app_secret` | 应用 Secret |
| `wechat.to_user` | 接收人，`@all` 表示全部 |
| `poll_interval_min/max` | 轮询间隔（秒），随机取值 |
| `comment_delay_min/max` | 评论抓取延迟（秒） |
| `weibo_fetch_count` | 每次拉取微博数量 |
| `max_monitored_weibos` | 最大监控微博数，超出自动清理 |
| `comment_max_pages` | 一级评论最大翻页数 |
| `push_days_threshold` | 只推送 N 天内的内容 |
| `risk_control` | 是否启用风控降频 |
| `heartbeat_interval` | 心跳写入间隔（秒） |



## 运行

```bash
python weibo-crawler-master3.19.py
```

指定配置文件：

```bash
python weibo-crawler-master3.19.py --config /path/to/config.json
```

### 获取微博 Cookie

1. 用浏览器登录 [weibo.com](https://weibo.com)
2. 打开开发者工具 → Application → Cookies
3. 复制完整 Cookie 字符串（至少需要 `SUB=` `SCF=` `SUBP=`）

### 获取企业微信配置

1. 登录[企业微信管理后台](https://work.weixin.qq.com/)
2. 创建应用，获取 `AgentId` 和 `Secret`
3. 在应用的「接收消息」中配置接收人



## 数据存储

所有数据存储在 `data/` 目录下：

- `weibo_monitor.db` — SQLite 数据库
- `weibo_monitor.log` — 运行日志

## 风控机制

- 正常轮询间隔 61-70 秒（随机化）
- 遇到 403/418 自动降频，最高 32 倍
- 连续错误自动退避，冷却期最长 5 分钟
- 恢复正常后逐步降低倍率
- 可在 config 中关闭：`"risk_control": false`


## 📄 免责声明

**1. 使用目的**
本项目仅供学习参考，请勿用于商业用途或牟利。

**2. 版权说明**
- 项目内资源版权归原作者所有
- 如有侵权请在 Issues 反馈，核实后立即删除
- 禁止任何公众号、自媒体转载

**3. 责任限制**
- 本项目不对任何内容承担法律责任
- 使用本项目造成的任何后果由使用者自行承担
- 数据真实性、准确性由填写者负责

**4. 使用规范**
- 仅供学习和研究使用
- 禁止用于任何违法行为
- 24小时内完成学习后请及时删除

**5. 其他**
- 本项目保留随时修改免责声明的权利
- 使用本项目即视为接受本声明

## 许可

MIT
