# quorum-log-go

单机多节点一致性集群的 Go 实现（Raft 风格）。节点以本进程内 goroutine 运行，
通过内存消息总线通信、以本地文件做持久化，可模拟网络分区、宕机与重启，
不依赖任何外部服务、真实集群或凭据。

## 架构

```
raft/
  types.go       核心类型：角色、日志条目、命令、RPC 消息、配置
  node.go        节点核心：单 goroutine 事件循环、状态恢复、对外 API
  election.go    领导选举与角色转换
  replicate.go   日志复制、提交位点推进、幂等写入
  snapshot.go    快照压缩与 InstallSnapshot 追赶
  storage.go     文件持久化（state.json / wal.log / snapshot.json，全部原子写）
  transport.go   内存消息总线（支持分区隔离与丢包统计）
  cluster.go     测试/演示用集群封装（宕机、重启、分区、收敛等待）
```

每个节点是一个**单 goroutine 事件循环**（actor 模型）：tick、网络消息、客户端
请求都经 channel 汇入同一循环处理，状态无锁、无数据竞争（测试以 `-race` 验证）。

## 协议状态机

### 角色与转换

```
                选举超时
  Follower  ─────────────►  Candidate ──获多数票──►  Leader
     ▲                       │ 发现更高任期            │ 发现更高任期
     └───────────────────────┴─────────────────────────┘
                      （退位为 Follower）
```

- **选举**：选举超时在 `[ElectionTickMin, ElectionTickMax)` 间随机，避免选票分裂。
  候选者先自投并**先落盘 `HardState{term, votedFor}` 再发投票请求**；
  投票方同样**先落盘再回复**，宕机重启后同一任期不会重复投票。
- **日志新旧比较**：先比最后条目任期，再比日志长度，保证新领导的日志
  包含所有已提交条目。
- **领导上任**：立即追加一条当前任期的 no-op 条目并广播心跳，
  使前任期条目能按提交规则尽快提交。

### 日志复制与提交

- 客户端写入携带幂等标识 `Command{ClientID, Seq}`：
  - 已应用的重复请求直接返回成功，不追加条目；
  - 在途/日志中已存在的请求复用原条目等待提交；
  - 状态机应用时按 `clientSeq` 再次去重（应对重启后的边界时序）。
  因此**超时重试不会产生重复条目、不会重复生效**。
- **提交规则**：只有被**多数节点**复制、且属于**当前领导任期**的条目才推进
  提交位点；被后续领导覆盖的未提交条目永远不会被应用、对外不可见。
- **一致性检查**：`AppendEntries` 携带 `prevLogIndex/prevLogTerm`，冲突时
  跟随者截断冲突后缀（原子重写 WAL）并以领导日志覆盖；响应携带冲突任期
  与首个冲突下标，领导可快速回退 `nextIndex`。

### 快照与日志压缩

- 已应用且未压缩的日志条数超过 `SnapshotThreshold` 时，对**已提交前缀**
  生成快照（含 KV 状态机与幂等表），随后截断 WAL。
- 落后太多（`nextIndex <= snapIndex`）的跟随者改走 `InstallSnapshot`：
  整体替换状态机后保留一致后缀，再以增量日志继续追赶。
  追赶完成后状态与"从未压缩、逐条重放"**完全一致**（测试逐键比对验证）。

## 异常与恢复规则

| 异常 | 规则 |
| --- | --- |
| 网络分区 | 少数派无法获得多数票/多数确认：分区期间不可能出现两个都能提交的领导；恢复后多数派日志覆盖少数派未提交条目 |
| 节点宕机 | 停止事件循环并注销收件箱；磁盘状态保留 |
| 宕机重启 | 从 `state.json`+`snapshot.json`+`wal.log` 恢复：任期/投票不丢，提交位点回到快照点，之后条目由领导重新复制提交 |
| 写入中途宕机 | 所有持久化均为"临时文件 + Sync + rename"原子写；WAL 末尾半行在恢复时丢弃，不会留下部分应用的状态 |
| 快照安装中断 | 快照与 WAL 截断分两步原子写，恢复时要么是旧快照+完整 WAL、要么是新快照+新 WAL，重收快照即可 |
| 领导退位/宕机 | 未决客户端请求立即以 `ErrNotLeader` 失败，客户端可携带同一幂等标识向新领导重试 |

## 可配置项（`raft.Config`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `ID` / `Peers` | — | 节点标识与集群成员 |
| `Dir` | — | 持久化目录（每节点独立） |
| `ElectionTickMin/Max` | 15 / 30 | 选举超时区间（tick 数） |
| `HeartbeatTick` | 3 | 心跳间隔（tick 数） |
| `TickIntervalMs` | 10 | 一个 tick 的真实毫秒数 |
| `SnapshotThreshold` | 50 | 触发快照压缩的未压缩日志条数 |

## 本地验证

```bash
go build ./...
go test ./... -v          # 全部场景，日志含任期/提交位点/落盘流水
go test ./... -race -count=3   # 竞态检测 + 重复运行验证稳定性
```

测试覆盖：

- `TestElectionAndReplication`：选举、写入复制、多数提交与读可见（正常路径回归）
- `TestIdempotentRetry`：超时重试/并发重复请求不产生重复条目
- `TestPartitionConvergesToSingleLeader`：分区后收敛唯一领导，隔离方无法提交
- `TestCrashRestart`：宕机重启后状态恢复、增量追赶、领导宕机重新选主
- `TestLogConflictOverwrite`：未提交冲突条目被覆盖且不对外呈现
- `TestSnapshotCatchUp` / `TestSnapshotCatchUpInterrupted`：快照压缩、
  快照+增量追赶、追赶中断后恢复的一致性
