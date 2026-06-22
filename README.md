# MagicQueue (Python)

[![CI](https://github.com/JackyZhang8/magicqueue-py/actions/workflows/ci.yml/badge.svg)](https://github.com/JackyZhang8/magicqueue-py/actions/workflows/ci.yml)

轻量、可插拔的消息队列库 —— Go 版 [MagicQueue](https://github.com/JackyZhang8/MagicQueue) 的 Python 移植，API 与语义对齐。

## 特性

- **可插拔驱动**：内存（开发/测试）与 Redis（生产/分布式），可继承 `Driver` 自定义。
- **三档消息优先级**：高 / 普通 / 低，同档 FIFO。
- **批量入队** `enqueue_batch`：Redis 用 pipeline 单次往返；持久化用 sqlite 批量写；失败整体回滚。
- **Redis 多 key `BLPOP`**：按优先级阻塞消费，无 `LLEN` 轮询、无空转。
- **持久化与崩溃恢复**：基于标准库 `sqlite3`，重启自动重投未确认消息。
- **自动重试**：指数退避 + 抖动，异步调度（`threading.Timer`）不阻塞 worker。
- **死信队列**：重试耗尽的消息可转入死信队列而非丢弃。
- **优雅关闭**：取消信号 + 等待在途任务完成（可用作上下文管理器）。
- **处理器异常隔离**：单条消息抛异常不会拖垮 worker，可注册回调。
- **可注入 Logger**：实现 `log(msg)` 接入任意日志框架。

> 交付语义为 **at-least-once（至少一次）**：消息可能被重复投递，处理器应保证**幂等**。

## 安装

```bash
pip install -e .            # 仅内存驱动
pip install -e ".[redis]"   # 含 Redis 驱动
```

需要 Python 3.9+。持久化使用标准库 `sqlite3`，无额外依赖。

## 快速开始

```python
from magicqueue import MQueue, Payload, JobResult

q = MQueue("svc").use_memory()
q.set_handler("email", "notify", lambda p: JobResult.ok())  # 返回 fail(..) 会触发重试
q.start_workers(4)

q.enqueue(Payload("email", group="notify"))

q.stop()  # 优雅关闭；也可用 with MQueue(...) as q: ...
```

处理器可以是普通可调用对象，也可以是实现 `execute(payload) -> JobResult` 的对象。

## 性能基准（内存驱动横向对比）

同一台机器、统一方法下三语言版本的吞吐对比（内存驱动，排除 Redis/磁盘干扰）。

**测试环境**：Intel Xeon Platinum 8375C @ 2.90GHz（2 vCPU）/ 7.8 GiB RAM / Ubuntu 22.04 / Go 1.22、Rust 1.83（release）、CPython 3.12。
**方法**：每项 N = 200,000 消息，取 3 次运行的中位数；批量大小 1000；端到端为 4 workers + 空 handler（pre-enqueue 后计时至全部处理完）。

| 指标 | Go | Rust | Python |
|------|---:|-----:|-------:|
| 单条入队 `enqueue`（条/秒）        | 663,013   | 1,293,833 | 97,660 |
| 批量入队 `enqueue_batch`（条/秒，batch=1000） | 727,392 | 1,109,984 | 98,832 |
| 端到端处理（条/秒，4 workers）     | 404,835   | 1,075,262 | 45,624 |

> 说明：Go/Rust 为编译型、Python 为解释型且受 GIL 限制，量级差异属预期。数字为单台 2 vCPU 云主机上的近似值，仅用于版本间相对比较，绝对值会随硬件波动。
>
> 复现：Go `go run ./bench`、Rust `cargo run --release --example bench`、Python `python bench.py`。

## 消息优先级

`Payload.priority`（int）按符号映射三档，默认 `0`：

| priority | 档位 | 子队列后缀 |
|----------|------|-----------|
| `> 0`    | 高   | `:p2`     |
| `== 0`   | 普通 | `:p1`     |
| `< 0`    | 低   | `:p0`     |

同一档内保持 FIFO。消费顺序：**高 → 普通 → 低**。

```python
q.enqueue(Payload("task", priority=10))   # 高
q.enqueue(Payload("task"))                # 普通
q.enqueue(Payload("task", priority=-1))   # 低
```

**实现**：每个逻辑队列拆成三个子队列（key 加后缀 `:p2/:p1/:p0`）。
Redis 消费用多 key `BLPOP key:p2 key:p1 key:p0 timeout`——Redis 按参数顺序返回首个非空队列的元素，
一次调用既实现优先级又是阻塞消费，**取代了 `LLEN` 轮询 + `LPOP` 的空转与竞态**。

## 批量入队

```python
ids = q.enqueue_batch([
    Payload("task", priority=10),
    Payload("task"),
    Payload("task", priority=-1),
])
```

- Redis 用 **pipeline** 单次往返完成所有 `RPUSH`。
- 持久化消息用 **sqlite `executemany`** 在一个事务内落盘。
- 任一条校验失败则整体不入队；推送失败会回滚已持久化的条目。
- 自定义驱动可重写 `push_batch` 优化，否则自动退化为逐条 `push`。

## 持久化

```python
q = MQueue("svc").use_memory().use_persistence("./data/mq.db")
q.enqueue(Payload("job", is_persist=True))
```

启用后，`start_workers` 会先把尚未确认的消息重投回驱动再启动消费者。
成功或永久失败（进入死信队列）后才会从持久层删除。

### 持久层后端：sqlite（默认）或 LevelDB

持久层通过 `Store` 抽象可插拔，内置两种实现，语义一致：

| 方法 | 后端 | 依赖 | 与其他语言版本对齐 |
|------|------|------|------|
| `use_persistence(path)` | 标准库 `sqlite3` | 无额外依赖（默认） | — |
| `use_leveldb(path)`     | LevelDB（`plyvel`） | `pip install "magicqueue[leveldb]"` | Go 版 LevelDB / Rust 版 sled |

```python
# 与 Go/Rust 版一致的 LevelDB 持久化
q = MQueue("svc").use_memory().use_leveldb("./data/mq.leveldb")
q.enqueue(Payload("job", is_persist=True))
```

> `plyvel` 在 Linux/macOS 提供预编译 wheel，通常无需系统级 `libleveldb`。
> 也可实现自定义 `Store` 并用 `use_store(store)` 注入。

## Redis

```python
import redis
from magicqueue import MQueue

client = redis.Redis.from_url("redis://127.0.0.1:6379")
q = MQueue("svc").use_redis(client)        # 复用调用方的 client
# 或：q = MQueue("svc").use_redis_url("redis://127.0.0.1:6379")
```

## 重试与死信队列

```python
from magicqueue import Options

q.with_options(Options(
    retry_base_delay=0.5,
    retry_max_delay=30.0,
    enable_dead_letter=True,
))
q.enqueue(Payload("job", max_retry=5))
```

退避时长为 `base * 2^(retry-1)`，封顶 `retry_max_delay`，并叠加 `[0.5x, 1.0x]` 抖动。

## 示例

| 路径 | 说明 |
|------|------|
| `examples/basic_memory.py`      | 内存队列最简上手 |
| `examples/multiple_handlers.py` | 多 (topic, group) 处理器 |
| `examples/batch_priority.py`    | 批量入队 + 优先级排序 |
| `examples/retry_deadletter.py`  | 重试与死信队列 |
| `examples/graceful_shutdown.py` | 优雅关闭（SIGINT/SIGTERM） |
| `examples/persistence.py`       | sqlite 持久化与崩溃恢复 |
| `examples/redis_example.py`     | Redis 驱动（需本地 Redis） |

运行：`python examples/basic_memory.py`

## 架构

```
enqueue / enqueue_batch
        │  (JSON) + 可选 sqlite 持久化
        ▼
   Driver.push  ── 内存 / Redis（按优先级分 :p2/:p1/:p0 子队列）
        ▲                       │
        │ requeue(重试)          │ bpop(多 key BLPOP，高→普通→低)
        │                       ▼
   死信队列 ◄── 重试耗尽    consume 线程 ──► jobs 队列 ──► worker 池 ──► Handler
                                                              │
                                                          成功→ack(删除持久层)
                                                          失败→重试/死信
```

## 开发

```bash
pip install -e ".[dev]"
ruff check src tests examples
pytest -q
python examples/basic_memory.py
```

## License

MIT，见 [LICENSE](LICENSE)。Author: JackyZhang8。
