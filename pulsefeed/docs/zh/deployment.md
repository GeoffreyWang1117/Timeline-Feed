# 真机部署指南

从一台裸的 Linux 机器开始，到一个持久化、带认证、有监控、可承接真实流量的
PulseFeed——包括可选的 GPU 机器（跑本地模型 + 训练打分器）。英文版见
[../deployment.md](../deployment.md)。

本文的每一条行为都对照仓库里的代码核实过；凡是承重的行为（持久化、重启恢复、
摄入失败关闭），在 `tests/test_deploy_wiring.py` 里都有一条以它命名的测试。

---

## 0. 先选拓扑

PulseFeed 沿一条阶梯降级，部署形态也一样。从能回答你问题的最小一级开始，
每一级都是前一级的严格超集：

| 级 | 跑什么 | 重启后状态还在吗 | 适合 |
|---|---|---|---|
| **T0 开发** | 只有 `uvicorn` | 否 | 试 API、演示 |
| **T1 单机持久化** | uvicorn + Redis + Postgres | 是 | 一个团队的真实部署 |
| **T2 分离 LLM** | T1 + 一台跑本地模型的 GPU 机器 | 是 | 不依赖外部供应商、数据不出内网 |

刻意没有"T3：N 个无状态 API 副本"这一级——在把多个实例挂到负载均衡后面之前，
必读 [§10 扩展的边界](#10-扩展的边界加副本之前必读)。

**LLM 从来不是部署依赖。** 每一级在不配置任何 provider 的情况下都能启动并服务
（由确定性 mock 代替增强）。什么时候准备好了再配真实供应商，其余一切不变。

---

## 1. 机器要求

以下是测出来的行为，不是许愿：参考回放在单核 CPU、内存后端下约 27 秒推完
6,396 个事件——廉价路径只是字典查找和 256 维哈希嵌入，昂贵路径被准入控制
刻意限流。

| 组件 | 最低 | 舒适 | 说明 |
|---|---|---|---|
| API 机 CPU | 2 核 | 4 核 | 管线是单进程，超过 ~4 核收益很小 |
| API 机内存 | 2 GB | 8 GB | 内存状态有界：按保留上限约 5 万事件 + 注解 + 摘要 |
| Postgres | 同机即可 | 2 核 / 4 GB | 只有 pgvector 建索引是重活 |
| Redis | 同机即可 | 512 MB `maxmemory` | 流是摄入缓冲，不是记录本身 |
| GPU 机（可选） | — | 1× 24 GB 显存 | vLLM 下跑 7B 级模型足够；小卡跑小模型 |
| 操作系统 | Linux，Python ≥ 3.10 | Ubuntu 22.04+ / Debian 12+ | 有 systemd 即可套用 §5 |

---

## 2. 安装

在 API 机上：

```bash
sudo apt-get update && sudo apt-get install -y python3-venv python3-pip git
git clone <你的仓库地址> && cd Timeline-Feed/pulsefeed

python3 -m venv /opt/pulsefeed-venv
source /opt/pulsefeed-venv/bin/activate
pip install -e '.[all,store]'        # api + llm + metrics + redis/postgres 客户端
```

`.[all,store]` 是完整服务端。extras 之所以拆开（`api`、`llm`、`metrics`、
`store`、`dev`、`gpu`），是让只用回放平台或只当库用的安装保持轻量；`gpu`
只服务于 transformer 编码器路径，装在 GPU 机上，不装在这里。

### 后端服务

方式 A——Docker（最快达到正确）：

```bash
docker compose up -d          # redis:7 + pgvector/pgvector:pg16，带健康检查
```

方式 B——系统包：

```bash
sudo apt-get install -y redis-server postgresql-16 postgresql-16-pgvector
sudo -u postgres psql -c "CREATE USER pulsefeed PASSWORD 'CHANGE_ME'"
sudo -u postgres psql -c "CREATE DATABASE pulsefeed OWNER pulsefeed"
```

Redis 有两个设置是关键（`docker-compose.yml` 已都设好）：

- `appendonly yes`——流必须活过 Redis 重启，否则"202 = 已持久接收"就是谎言。
- `maxmemory-policy noeviction`——Redis 满了必须大声拒绝写入（生产者看到失败
  会重试），而不是悄悄丢掉一段流。

pgvector 是推荐而非必需：库存 Postgres 上 sink 会退化为 `real[]` 列 +
Python 内余弦——正确，但更慢。preflight 会告诉你现在是哪种。

---

## 3. 配置

```bash
cp .env.example .env && $EDITOR .env
```

`.env.example` 对每个变量都有注释。完整参考表：

| 变量 | 默认 | 用途 |
|---|---|---|
| `PULSEFEED_LLM_API_KEY` | — | 托管供应商的 key |
| `PULSEFEED_LLM_BASE_URL` | OpenAI | 任何 OpenAI 兼容端点 |
| `PULSEFEED_LLM_MODEL` | `gpt-4o-mini` | 主模型 |
| `PULSEFEED_LOCAL_LLM_BASE_URL` | — | 回退端点（如 GPU 机上的 vLLM） |
| `PULSEFEED_LOCAL_LLM_API_KEY` | — | 回退端点的 key（如果需要） |
| `PULSEFEED_LOCAL_LLM_MODEL` | `local-model` | 回退端点服务的模型名 |
| `PULSEFEED_PG_DSN` | — | Postgres 记录本源；不设 = 纯内存 |
| `PULSEFEED_RESTORE` | `1` | 启动时从 sink 恢复状态（`0` 关闭） |
| `PULSEFEED_RESTORE_TENANTS` | `PULSEFEED_TENANT` | 要恢复的租户，逗号分隔 |
| `PULSEFEED_REDIS_URL` | — | Redis Streams 总线；不设 = 进程内直接摄入 |
| `PULSEFEED_STREAM` | `pulsefeed:events` | 流的 key |
| `PULSEFEED_CONSUMER` | 主机名-pid | 消费组内的消费者名 |
| `PULSEFEED_SCORER_MODEL` | — | 训练产出的打分器 JSON |
| `PULSEFEED_TENANT` | `default` | 启动时注册的租户 |
| `PULSEFEED_TOKEN_BUDGET` | `500000` | 每日 token 预算 |
| `PULSEFEED_USD_BUDGET` | `5.0` | 每日美元预算 |
| `PULSEFEED_WORKERS` | `4` | 并发增强数 |
| `PULSEFEED_AUDIT_RATE` | `0.01` | 被拒事件的复检抽样率 |
| `PULSEFEED_API_KEYS` | — | `key:租户` 对；`key:*` = 运维 key；**不设 = 认证关闭** |
| `PULSEFEED_RATE_WRITE_PER_SECOND` | `200` | 每租户写入速率（突发 2×） |
| `PULSEFEED_RATE_READ_PER_SECOND` | `50` | 每租户读取速率（突发 2×） |
| `PULSEFEED_SKIP_PREFLIGHT` | — | 仅容器镜像：跳过启动时 preflight |

容易搞错的语义：

- **设了 `PULSEFEED_PG_DSN` ⇒ 启动失败关闭。** DSN 配了但连不上，服务拒绝
  启动。操作者要的是持久化，就绝不能得到一个"看起来健康、第一次重启才发现
  失忆"的部署。
- **设了 `PULSEFEED_REDIS_URL` ⇒ `POST /v1/events` 先发布到流**，事件进了
  Redis 才回 202；进程内消费者以至少一次语义摄入。Redis 挂了，摄入返回
  **503**——上游请缓冲重试。不设则为进程内直接摄入（延迟更低，但在途事件
  没有崩溃恢复）。
- **生成真正的密钥：** 每个 key 用
  `python -c "import secrets; print(secrets.token_urlsafe(32))"`。
  key 决定租户——见 [../api.md](../api.md#authentication-and-rate-limiting)。

---

## 4. Preflight（起飞前检查）

```bash
set -a && source .env && set +a       # 把服务端将看到的变量导出
pulsefeed-preflight                   # 或 python -m pulsefeed.preflight
```

preflight 读服务端读的同一组变量，并验证每个**已配置**的依赖真的能用：
Redis 做一次流写读往返；Postgres 做一次真实写入并报告 pgvector 是否可用；
每个配置的 LLM 端点做一次单 token 补全（报延迟）；打分器模型文件按当前特征
契约加载；所有数值变量能解析。未配置的组件是 *skip* 不是失败——每行 skip
都写明你因此选择了什么回退。

退出码 0/1，脚本用 `--json`。把部署卡在它上面：

```bash
pulsefeed-preflight || { echo "环境是坏的，不部署"; exit 1; }
```

打分器检查之所以存在：服务端对坏模型文件是刻意**静默**回退的（坏文件只能
降级排序，不能挡启动）；preflight 就是同一份文件**大声**失败的地方——在流量
到来之前。

---

## 5. 用 systemd 跑

`/etc/systemd/system/pulsefeed.service`：

```ini
[Unit]
Description=PulseFeed API
After=network-online.target redis-server.service postgresql.service
Wants=network-online.target

[Service]
Type=exec
User=pulsefeed
Group=pulsefeed
WorkingDirectory=/opt/Timeline-Feed/pulsefeed
EnvironmentFile=/etc/pulsefeed/env
ExecStartPre=/opt/pulsefeed-venv/bin/pulsefeed-preflight
ExecStart=/opt/pulsefeed-venv/bin/uvicorn pulsefeed.api:create_app --factory \
    --host 127.0.0.1 --port 8000
Restart=on-failure
RestartSec=3

# 加固——这个进程除了对外的 socket 什么权限都不需要。
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd --system --no-create-home pulsefeed
sudo install -d -m 750 -o pulsefeed /etc/pulsefeed
sudo cp .env /etc/pulsefeed/env && sudo chown pulsefeed /etc/pulsefeed/env && sudo chmod 600 /etc/pulsefeed/env
sudo systemctl daemon-reload && sudo systemctl enable --now pulsefeed
journalctl -u pulsefeed -f
```

要点：

- `ExecStartPre` 每次启动都跑 preflight：一台 Postgres 半夜挂掉的机器会带着
  指名的原因启动失败并写进 journal，而不是失忆地继续服务。
- `--host 127.0.0.1` 是刻意的——TLS 和对外暴露是反向代理的职责（§6）。
- **只跑一个实例。** 不要把这个 unit 模板化成 `pulsefeed@1,2,3`——见 §10。
- 重启在设计上是廉价的：启动时从 Postgres 恢复状态（按配置的租户逐个
  `restore()`），总线里未确认的投递会被重投。重启丢失的只有：敞开的合并簇
  （其事件在总线里，会重投）和内存里的延迟采样。

### 或者走 docker

```bash
docker compose --profile app up -d    # 构建镜像，等健康检查通过
```

镜像在容器启动时先跑 preflight（在编排器反正会重启的场景下可设
`PULSEFEED_SKIP_PREFLIGHT=1` 跳过），以非 root 用户运行，不内嵌任何密钥——
配置全部经环境注入，训练好的打分器用卷挂载。

---

## 6. 对外暴露：反向代理、TLS、密钥

裸端口绝不上公网。前面放 nginx：

```nginx
server {
    listen 443 ssl;
    server_name pulsefeed.example.com;
    ssl_certificate     /etc/letsencrypt/live/pulsefeed.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pulsefeed.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        client_max_body_size 2m;         # 1000 事件的批量请求绰绰有余
    }

    # 运维端点是给你的基础设施用的，不是给公网用的。
    location ~ ^/(metrics|healthz|readyz)$ {
        allow 10.0.0.0/8;                # 你的监控网段
        deny all;
        proxy_pass http://127.0.0.1:8000;
    }
}
```

然后确认部署真的关严了：

```bash
curl -s https://pulsefeed.example.com/readyz | python3 -m json.tool
#   "auth": "enabled"           ← 如果是 "disabled (dev mode)"，到此为止先修它
#   "durability": {"sink": "postgres", "bus": "redis", ...}
```

`/readyz` 之所以把认证和持久化状态如实报出来，正是因为配置错误的部署
（认证关着，或本该持久化的机器在纯内存跑）否则和正常部署看起来一模一样。

---

## 7. GPU 机：本地模型 + 训练

可选，两个互相独立的用途。两者在设计上都只是配置变更而非改代码——provider
层对任何讲 OpenAI chat-completions 协议的端点一视同仁。

### 7a. 用 vLLM 服务本地模型

在 GPU 机上：

```bash
python3 -m venv ~/vllm-venv && source ~/vllm-venv/bin/activate
pip install vllm
python -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct \
    --host 0.0.0.0 --port 8001 \
    --max-model-len 8192
```

在 API 机上，接成**回退**（托管供应商熔断打开时启用）：

```bash
PULSEFEED_LOCAL_LLM_BASE_URL=http://<gpu机>:8001/v1
PULSEFEED_LOCAL_LLM_MODEL=Qwen/Qwen2.5-7B-Instruct
```

或接成**主力**（数据不出内网；确定性 mock 仍是最后的兜底）：

```bash
PULSEFEED_LLM_BASE_URL=http://<gpu机>:8001/v1
PULSEFEED_LLM_API_KEY=            # vLLM 不设 --api-key 就不需要
PULSEFEED_LLM_MODEL=Qwen/Qwen2.5-7B-Instruct
```

然后从 API 机验证——preflight 的 LLM 探测做的就是这件事，直接跑它：

```bash
pulsefeed-preflight        # "llm fallback ... answered in NNNms"
```

算力提示：增强流量在设计上就是小的——默认 θ 下触发器只放行约 1–3% 的簇，
参考轨迹 8 个模拟小时共 51 次调用、平均约 550 token。一张中端 GPU 不会是
瓶颈；延迟（进入 deadline 策略的输入）比吞吐重要。买硬件之前先用
`pulsefeed_provider_latency_seconds` 量你自己的 p95。上面的模型名只是示例
不是推荐——换成你的卡装得下、你的评测更喜欢的任何模型。

### 7b. 用真实流量训练打分器

完整方法论（标签偏差、逆倾向加权、按时间切分、校准发布门）在
[../training.md](../training.md)；部署侧的闭环是：

```bash
# 1. 在 API 机上边跑边收集（training.md §7）：
#    TrainingCollector(pipeline).attach() → data/train.jsonl

# 2. 把数据集挪到 GPU 机（其实哪台都行；今天的第 3 步 CPU 就够）
scp data/train.jsonl gpu-box:pulsefeed-data/

# 3. 训练并导出
pulsefeed-train --source pipeline --out scorer.json --json

# 4. 运回来，重启之前先验证——服务端对坏文件是静默回退的；
#    preflight 和 /readyz 是你仅有的两个大声的检查
scp gpu-box:scorer.json /etc/pulsefeed/scorer.json
PULSEFEED_SCORER_MODEL=/etc/pulsefeed/scorer.json pulsefeed-preflight
sudo systemctl restart pulsefeed
curl -s localhost:8000/readyz | grep -o '"scorer": "[^"]*"'
#   "scorer": "LogisticScoreModel"   ← 而不是 "LinearScoreModel"
```

回滚 = 取消 `PULSEFEED_SCORER_MODEL` 再重启。GPU 嵌入路径
（training.md §8 的第 2–4 步）以后也在这台机器上做；今天的逻辑回归打分器
在 CPU 上几秒就训完。

---

## 8. 监控

Prometheus 抓取配置：

```yaml
scrape_configs:
  - job_name: pulsefeed
    scrape_interval: 15s
    static_configs:
      - targets: ["<api机>:8000"]
```

告警按 [../operations.md §2](../operations.md#2-what-to-alert-on) 接——P0
丢弃、摄入停滞、总线积压、熔断长开、预算过早耗尽、审计漏检率。仪表盘照
[../operations.md §3](../operations.md#3-the-dashboard-that-matters) 建那
一块就够：**负载上升而 LLM 调用率保持平坦，本身就是系统在工作的证据。**

配了总线后，`/readyz` 还会报告 `durability.bus_lag`（未确认投递数——要
告警的就是它）和消费者的摄入计数。

---

## 9. 备份、恢复、升级

**备什么：Postgres。只有 Postgres。** sink 是记录本源——事件、注解、摘要
（含已被取代的）、实体记忆。Redis 只保存在途的摄入缓冲（AOF 让它扛得住
重启；就算 Redis 全丢，丢的也只是未处理的投递，而生产者本来就该在重试）；
进程内存里的一切都能从 sink 重建。

```bash
pg_dump -Fc -U pulsefeed pulsefeed > pulsefeed-$(date +%F).dump   # 放进 cron
```

**灾难恢复** = 恢复 dump、启动服务。启动时的 `restore()` 按配置的租户重载
事件、实体记忆和活跃摘要；被取代的结论留在信念历史里可查，但不会复活到
feed 里。这条链路穿过工厂函数端到端验证于
`tests/test_deploy_wiring.py::test_restart_restores_from_the_sink`。

**升级：**

```bash
cd /opt/Timeline-Feed/pulsefeed && git pull
source /opt/pulsefeed-venv/bin/activate && pip install -e '.[all,store]'
pulsefeed-preflight && sudo systemctl restart pulsefeed
```

schema 迁移是连接时幂等执行的 `CREATE ... IF NOT EXISTS`；项目现阶段没有
独立的迁移步骤。重启窗口 = `restore()` 的 p99 + 进程启动，量级是秒；窗口内
反向代理回 502，生产者按总线语义重试（不用总线的话，自己缓冲）。

**部署后冒烟测试**（写进你的部署脚本）：

```bash
BASE=https://pulsefeed.example.com; KEY=<某个API密钥>
curl -sf $BASE/healthz > /dev/null
curl -sf $BASE/readyz | python3 -c "
import json,sys; r=json.load(sys.stdin)
assert r['auth'] == 'enabled', '认证是关的'
assert r['durability']['sink'] == 'postgres', 'SINK 是纯内存'
print('readyz ok:', r['scorer'], r['durability'])"
curl -sf -X POST $BASE/v1/events -H "x-api-key: $KEY" -H 'content-type: application/json' \
     -d '{"source":"deploy-smoke","content":"deployment smoke test"}' > /dev/null
curl -sf "$BASE/v1/timeline?limit=1" -H "x-api-key: $KEY" > /dev/null && echo "冒烟通过"
```

---

## 10. 扩展的边界（加副本之前必读）

把话说白，因为天真的"多加几个副本"正是在这里翻车：

**服务路径是单进程的。** 时间线读取来自有界的进程内存（p99 读延迟是字典
查找量级正因为此）。一个负载均衡后面挂两个 API 副本 = 两条各自漂移的
feed：Redis 消费组会把流**分片**给各消费者，每个副本只持有部分状态，读请求
在两份殘缺状态之间弹跳。

今天真正能扩的是：

- **纵向**——按突发测量结果（45.6× 负载冲击、峰值队列 0 吸收掉），单机到
  每小时数万事件都从容。
- **按租户分片**——租户在设计上完全隔离，所以实例 A 服务租户 1–N、实例 B
  服务其余，各用自己的流（`PULSEFEED_STREAM`）或自己的 Redis DB，在代理层
  按 key 路由。
- **只有写路径**今天就能横向扩：N 台无状态只发布的机器喂一个消费者——但
  消费者、因而服务端，仍是单一的。

真正的副本扩展需要把时间线读取路径改为从 sink（Postgres）服务而非进程
内存。存储层是为此建的；读取路径还没有——在指南里假装它有，比坦白说出来
糟糕得多。

---

## 11. 启动排障

| 症状 | 大概率原因 | 处置 |
|---|---|---|
| 一启动就退出，journal 里 preflight 有 ✗ | 某个已配置的后端不可达 | 修好或取消那个变量；preflight 会点名 |
| `RuntimeError: asyncpg is required` | 配了 DSN 但没装 `store` extra | `pip install -e '.[all,store]'` |
| 能启动，但该持久化的机器上 `/readyz` durability 显示 `memory` | `PULSEFEED_PG_DSN` 没进服务的环境 | 查 `EnvironmentFile`、`systemctl show pulsefeed -p Environment` |
| POST 返回 503 | 配了总线而 Redis 挂了——设计如此的失败关闭 | 恢复 Redis；生产者重试 |
| POST 202 但时间线里看不到 | 总线模式下消费者死了？ | `/readyz` → `durability.ingest` 计数和 `bus_lag` |
| 部署了模型后 `/readyz` scorer 仍是 `LinearScoreModel` | 模型文件坏/特征不匹配，静默回退 | 用同样的环境跑 preflight，它会大声给出原因 |
| 所有请求都 401 | key 没放在 `X-API-Key` 头里，或不在 `PULSEFEED_API_KEYS` 里 | 检查头名；key 是精确比较的 |
| 429 且带 `Retry-After` | 该租户令牌桶耗尽 | 按响应头退避；限额不合理就调 `PULSEFEED_RATE_*` |
| 大库上首次启动慢 | `restore()` 在重载事件 | 正常；想冷启动可设 `PULSEFEED_RESTORE=0` |
