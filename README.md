# quorum-log-go

单机多节点一致性集群（Raft 变体）的 Go 实现。节点以本地进程内对象模拟，
通过内存网络通信、本地文件持久化，无需任何外部服务、真实集群或凭据。
支持领导选举、日志复制、幂等客户端写入、快照压缩与崩溃恢复。

## 协议状态机

每个节点是 `Follower / Candidate / Leader` 三态状态机，由外部 `Tick()` 确定性驱动：

- **选举**：跟随者在 `[ElectionTimeout, 2*ElectionTimeout)` 个 tick 内未收到领导心跳，
  转为候选者，任期 +1、投自己一票（先落盘 `hardstate` 再发请求），向全员发 `RequestVote`。
  获得多数派选票即成为领导。同一任期每节点只投一票（`votedFor` 持久化），
  且只投给日志至少与自己一样新（`lastTerm, lastIndex` 字典序）的候选者。
- **复制**：领导当选后先追加一条本任期 no-op 条目，随后按 `HeartbeatTick` 周期向跟随者发送
  `AppendEntries`（携带 `prevLogIndex/prevLogTerm` 一致性校验与 `leaderCommit`）。
  跟随者校验失败时返回冲突任期/下标提示，领导据此快速回退 `nextIndex`。
- **提交**：领导仅对**本任期**的条目按多数派 `matchIndex` 推进 `commitIndex`
  （之前任期的条目随本任期条目间接提交），随后按序应用到状态机。
  被覆盖的未提交条目永远不会进入已提交前缀，也不会对外呈现。
- **快照**：未压缩日志条数超过 `SnapshotThreshold` 时，对已应用前缀生成快照
  （状态机 KV + 客户端幂等序号表），原子落盘后截断日志前缀。
  落后过多的跟随者通过 `InstallSnapshot` 整体替换状态，再接收增量日志追平。

## 持久化与恢复规则

所有状态变更遵循**先落盘、再生效**：

| 数据 | 位置 | 写入方式 |
|---|---|---|
| 任期 / 投票 | `hardstate.json` | 临时文件 + rename 原子替换 |
| 日志条目 | `log/<index>.entry` | CRC32 校验 + fsync + rename |
| 快照 | `snapshot.json` | 原子替换，随后删除已包含的日志文件 |

- 宕机 = `Stop()`（从网络摘除、停止时钟），数据保留在磁盘；`RestartNode` 从磁盘重建。
- 重启后仅快照前缀视为已提交；之后的条目等领导心跳重新确认提交位点后**幂等重放**
  （按 `ClientID+Seq` 去重），因此中断不会留下被部分应用的状态。
- 快照安装同样原子：先完整落盘并解码校验，再一次性替换内存状态。

## 客户端语义

- 写入必须携带幂等标识 `(ClientID, Seq)`，`Seq` 单调递增。
  超时重试同一 `(ClientID, Seq)` 不会产生重复条目，也不会重复应用。
- `Write` 同步复制到达多数派并提交后才返回 `Committed=true`；
  未达多数派时返回 `Committed=false`，客户端可用相同标识安全重试。
- `ReadCommitted` 仅由领导响应，读到的是多数派已提交的状态。

## 可配置项（`raft.Config`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `ElectionTimeout` | 10 | 选举超时基数（tick），实际超时为其 1~2 倍随机值 |
| `HeartbeatTick` | 2 | 领导心跳间隔（tick） |
| `SnapshotThreshold` | 20（集群默认） | 触发快照的未压缩日志条数 |
| `DataDir` | — | 节点持久化目录（每节点独立） |

## 本地验证

```bash
go build ./...        # 编译
go test ./raft/ -v    # 全部场景，日志含任期/提交位点/落盘流水
go test ./raft/ -race -count=3   # 竞态与稳定性
```

测试覆盖：

- `TestElectionAndBasicReplication` — 选举与基本复制（正常路径）
- `TestPartitionConvergesToSingleLeader` — 五节点分区，少数派不可提交，愈合后唯一领导
- `TestIdempotentClientRetry` — 客户端重试/乱序旧请求去重
- `TestCrashRestartRecovery` — 宕机重启后状态恢复并追平
- `TestLogConflictOverwrite` — 冲突日志被覆盖，未提交条目不对外呈现
- `TestSnapshotCatchUp` — 落后节点经快照+增量追平，状态与未压缩节点一致
- `TestInterruptDuringCatchUp` — 追赶中途宕机重启，不留部分应用状态

## 目录结构

```
raft/
  types.go    消息与公共类型
  node.go     协议状态机（选举/复制/提交/快照/恢复）
  storage.go  文件持久化（硬状态、日志、快照，原子写）
  network.go  模拟网络（投递、分区、断链）
  cluster.go  单机多节点骨架与确定性时钟
  *_test.go   场景测试
```
