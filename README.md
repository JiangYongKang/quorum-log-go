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

## 强杀（掉电）恢复与中断处理

进程可能在任意两个持久化步骤之间被强杀，磁盘上因此留下**中间态残留**。
节点重启时由 `Storage.Recover` 统一清理，规则确定且幂等——无论上一轮在何处中断、
连续中断多少次，恢复结果都相同：

| 中断位置 | 磁盘残留 | 恢复动作 |
|---|---|---|
| 临时文件已写、rename 之前 | `*.tmp` 半成品 | 删除临时文件，以旧的有效文件为准 |
| 快照已替换、日志压缩之前 | 新快照 + 已被覆盖的旧日志 | 删除 `index <= snapshotIndex` 的日志文件 |
| 后缀截断删到一半 | 日志下标出现空洞 | 从第一个空洞起整体截断（空洞后条目未获多数派确认，不可能已提交） |
| 批量追加写到一半 | 只有已写前缀 | 天然是合法前缀，无需处理 |
| 日志文件损坏（CRC 不过） | 不可读条目 | 从第一个损坏点起整体截断 |

恢复完成后的**健康门禁**：节点先处于未就绪状态，不参与对外服务——

- 跟随者：应用进度追平领导公布的 `leaderCommit` 后转为就绪；
- 领导：在本任期提交过条目（含当选时的 no-op）后转为就绪；
- 未就绪节点拒绝客户端提交（`Write` 返回 `recovering` 错误）与已提交读
  （`ReadCommitted` 返回 `ok=false`），可通过 `Node.Ready()` 查询。

每次恢复都会生成 `RecoveryReport`（`Node.RecoveryReport()`，可 JSON 序列化，
日志中也有人类可读摘要），记录：回退了哪些日志（`rolledBack`）、
清理了哪些残留（`staleLogsRemoved` / `tempFilesRemoved`）、
从哪个位点追平到哪个位点（`caughtUpFrom -> caughtUpTo`）、是否已就绪（`ready`）。

**兼容范围**：磁盘格式未变——`hardstate.json`、`log/*.entry`（CRC+JSON）、
`snapshot.json` 与上一版本完全一致，旧数据目录可直接启动；
恢复逻辑只是在此之上增加了残留清理，无迁移步骤。

测试通过 `Storage.ArmCrash(stage)` 注入崩溃点（`tmp` / `snapshot-compact` /
`truncate` / `append`），到达该阶段后存储进入"已崩溃"状态，
模拟强杀后磁盘保留中间态；随后 `StopNode` + `RestartNode` 验证恢复。

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
- `TestKillDuringClientWrite` — 领导落盘客户端写入中途被强杀，残留清理后重试收敛
- `TestKillDuringSnapshot` — 快照压缩中途被强杀，旧日志残留被清理、从快照恢复
- `TestKillDuringCatchUp` — 落后节点补数据中途被强杀，重启后重新追平
- `TestRepeatedKillsDeterministic` — 同一节点连续 3 次强杀+带残留重启，两轮全程结果一致且等于不中断参考
- `TestRecoveryReportAndReadinessGate` — 恢复报告内容、未就绪节点拒绝读写、就绪后对外读写一致
- `TestStorageRecoverCleansResidue` — 各类残留（tmp/旧日志/空洞/损坏文件）的清理与幂等性

## 目录结构

```
raft/
  types.go     消息与公共类型
  node.go      协议状态机（选举/复制/提交/快照/恢复/健康门禁）
  storage.go   文件持久化（硬状态、日志、快照，原子写 + 崩溃残留恢复 + 故障注入）
  recovery.go  恢复报告类型与崩溃注入阶段定义
  network.go   模拟网络（投递、分区、断链）
  cluster.go   单机多节点骨架与确定性时钟
  *_test.go    场景测试
```
