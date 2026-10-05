package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ---- 强杀测试辅助 ----

// nodeDir 返回节点的数据目录（测试内访问集群配置）。
func nodeDir(c *Cluster, id string) string { return c.cfgs[id].DataDir }

// armCrashOnce 装配一次性强杀注入：指定目录的指定落盘步骤第一次被触达时，
// 存储操作以 errSimulatedCrash 失败（磁盘保留中途状态），随后自动解除。
// 返回触发计数，配合 t.Cleanup 复位全局钩子。
func armCrashOnce(dir, step string) (fired *int) {
	count := new(int)
	prev := crashHook
	crashHook = func(cp CrashPoint) bool {
		if cp.Dir == dir && cp.Step == step && *count == 0 {
			*count++
			return true
		}
		if prev != nil {
			return prev(cp)
		}
		return false
	}
	return count
}

func resetCrashHook() { crashHook = nil }

// listResidue 列出节点数据目录中的残留：临时文件与 <= snapIndex 的过期日志。
func listResidue(t *testing.T, c *Cluster, id string) []string {
	t.Helper()
	dir := nodeDir(c, id)
	// 从磁盘读快照点，而不是内存
	store, err := OpenStorage(dir)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	snapIndex, _, _, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	residue := []string{}
	for _, sub := range []string{".", logDirName} {
		ents, err := os.ReadDir(filepath.Join(dir, sub))
		if err != nil {
			continue
		}
		for _, e := range ents {
			name := e.Name()
			if strings.HasSuffix(name, ".tmp") {
				residue = append(residue, filepath.Join(sub, name))
				continue
			}
			var idx int
			if _, err := fmt.Sscanf(name, "%d.entry", &idx); err == nil && idx <= snapIndex {
				residue = append(residue, filepath.Join(sub, name))
			}
		}
	}
	sort.Strings(residue)
	return residue
}

// writeBatch 向当前领导连续写入一批 set 命令，要求全部提交。
func writeBatch(t *testing.T, c *Cluster, client string, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		leader := c.WaitLeader(100)
		if leader == nil {
			t.Fatalf("no leader for write %d", i)
		}
		r := leader.Write(ClientWrite{ClientID: client, Seq: int64(i),
			Command: []byte(fmt.Sprintf("set:key%03d:val%03d", i, i))})
		if !r.Committed {
			t.Fatalf("write seq=%d not committed: %+v", i, r)
		}
	}
}

// wantKV 构造全部写入应用后的期望状态机内容。
func wantKV(from, to int) map[string]string {
	m := map[string]string{}
	for i := from; i <= to; i++ {
		m[fmt.Sprintf("key%03d", i)] = fmt.Sprintf("val%03d", i)
	}
	return m
}

// assertConverged 断言所有存活节点的已应用状态与期望一致。
func assertConverged(t *testing.T, c *Cluster, want map[string]string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		got := c.Node(id).AppliedKV()
		if len(got) != len(want) {
			t.Fatalf("node %s: got %d keys, want %d", id, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("node %s: key %s got %q want %q", id, k, got[k], v)
			}
		}
	}
}

// ---- 用例 1：处理客户端写入的中途被强杀 ----

// 跟随者在接收领导复制（日志落盘）的中途被强杀：磁盘留下 .tmp 残留，
// 重启后必须清理干净并追平，最终状态与全程不中断一致。
func TestKillDuringClientWrite(t *testing.T) {
	defer resetCrashHook()
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	t.Logf("scenario: follower killed mid-write-replication, leader=%s", leader.ID())

	// 先正常提交 5 条
	writeBatch(t, c, "cli1", 1, 5)

	// 装配：c 在写日志条目（tmp-written）时被强杀
	victimDir := nodeDir(c, "c")
	fired := armCrashOnce(victimDir, "tmp-written")

	// 再发一条写入：c 在复制落盘中途“崩溃”（时钟由测试驱动，
	// Write 返回后不会有后台活动，可立即强杀，残留保留在磁盘上）
	leader = c.WaitLeader(100)
	r := leader.Write(ClientWrite{ClientID: "cli1", Seq: 6, Command: []byte("set:key006:val006")})
	if !r.Committed {
		t.Fatalf("write seq=6 not committed: %+v", r)
	}
	if *fired == 0 {
		t.Fatal("crash hook never fired; scenario not covered")
	}
	t.Logf("crash injected during log write on node c")

	// 强杀（不做任何收尾）并重启
	c.KillNode("c")
	if err := c.RestartNode("c"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	rep := c.Node("c").LastRecovery()
	t.Logf("recovery report: %s", rep.String())
	if len(rep.ResidueCleaned) == 0 {
		t.Fatal("expected tmp residue to be cleaned during recovery")
	}

	// 客户端重试剩余写入，c 追平后：状态与全部已提交写入严格一致，且无残留
	writeBatch(t, c, "cli1", 7, 10)
	c.Ticks(60)
	assertConverged(t, c, wantKV(1, 10), "a", "b", "c")
	if r := listResidue(t, c, "c"); len(r) > 0 {
		t.Fatalf("residue left after recovery: %v", r)
	}
	if !c.Node("c").Healthy() {
		t.Fatal("node c should be healthy after catch-up")
	}
	t.Logf("PASS: node c recovered and converged, residue cleaned")
}

// ---- 用例 2：状态压缩（快照）中途被强杀 ----

// 领导在快照落盘完成、日志压缩删除的中途被强杀：磁盘上留下
// “新快照 + 部分未删除的过期日志”的中途状态。重启后必须清理过期日志、
// 基于新快照恢复，并与集群收敛到一致状态。
func TestKillDuringSnapshotCompaction(t *testing.T) {
	defer resetCrashHook()
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	victim := leader.ID()
	t.Logf("scenario: leader %s killed mid-snapshot-compaction", victim)

	// 装配：领导在压缩删除过期日志的中途被强杀
	fired := armCrashOnce(nodeDir(c, victim), "log-compacting")

	// 逐条写入直到快照压缩触发（阈值 20 条未压缩日志）
	seq := 1
	for ; seq <= 40 && *fired == 0; seq++ {
		leader = c.WaitLeader(100)
		r := leader.Write(ClientWrite{ClientID: "cli1", Seq: int64(seq),
			Command: []byte(fmt.Sprintf("set:key%03d:val%03d", seq, seq))})
		if !r.Committed {
			t.Fatalf("write seq=%d not committed: %+v", seq, r)
		}
	}
	if *fired == 0 {
		t.Fatal("snapshot compaction never happened; scenario not covered")
	}
	lastWritten := seq - 1
	t.Logf("crash injected at seq=%d during log compaction", lastWritten)

	// 立即强杀并重启：磁盘上是“新快照 + 部分过期日志”的中途状态
	c.KillNode(victim)
	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("restart: %v", err)
	}
	rep := c.Node(victim).LastRecovery()
	t.Logf("recovery report: %s", rep.String())
	if rep.SnapshotIndex == 0 {
		t.Fatal("expected recovery to be based on the new snapshot")
	}
	if len(rep.ResidueCleaned) == 0 {
		t.Fatal("expected stale log residue to be cleaned during recovery")
	}

	// 继续写入并追平：最终状态必须与全部已提交写入一致，且无残留
	writeBatch(t, c, "cli1", lastWritten+1, lastWritten+5)
	c.Ticks(80)
	assertConverged(t, c, wantKV(1, lastWritten+5), "a", "b", "c")
	if r := listResidue(t, c, victim); len(r) > 0 {
		t.Fatalf("residue left after recovery: %v", r)
	}
	if !c.Node(victim).Healthy() {
		t.Fatalf("node %s should be healthy after recovery", victim)
	}
	t.Logf("PASS: %s recovered from mid-compaction kill, snapshot=%d",
		victim, rep.SnapshotIndex)
}

// ---- 用例 3：给落后节点补数据（快照安装）的中途被强杀 ----

// 落后节点在接收领导快照、快照已落盘但日志压缩与内存替换未完成时被强杀。
// 重启后必须基于已落盘的快照恢复、清理过期日志残留，并重新追平。
func TestKillDuringSnapshotInstall(t *testing.T) {
	defer resetCrashHook()
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	t.Logf("scenario: lagging follower c killed mid-snapshot-install")

	// c 先跟上 5 条（磁盘上留下自己的日志文件），然后被隔离
	writeBatch(t, c, "cli1", 1, 5)
	c.Ticks(5)
	c.Net().Disconnect(leader.ID(), "c")
	c.Net().Disconnect("c", "a")
	c.Net().Disconnect("c", "b")

	// 隔离期间写够触发领导快照的量
	writeBatch(t, c, "cli1", 6, 30)
	c.Ticks(5)

	// 恢复网络，装配：c 在快照落盘完成、压缩未开始时被强杀
	c.Net().Heal()
	fired := armCrashOnce(nodeDir(c, "c"), "snapshot-written")
	for i := 0; i < 200 && *fired == 0; i++ {
		c.Tick()
	}
	if *fired == 0 {
		t.Fatal("snapshot install never happened; scenario not covered")
	}
	t.Logf("crash injected during snapshot install on node c")

	// 立即强杀并重启
	c.KillNode("c")
	if err := c.RestartNode("c"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	rep := c.Node("c").LastRecovery()
	t.Logf("recovery report: %s", rep.String())
	if rep.SnapshotIndex == 0 {
		t.Fatal("expected recovery to be based on the installed snapshot")
	}
	if len(rep.ResidueCleaned) == 0 {
		t.Fatal("expected stale log residue (pre-isolation entries) to be cleaned")
	}

	// 追平后：c 的已应用状态与领导严格一致，无残留
	c.Ticks(100)
	assertConverged(t, c, wantKV(1, 30), "a", "b", "c")
	if r := listResidue(t, c, "c"); len(r) > 0 {
		t.Fatalf("residue left after recovery: %v", r)
	}
	if !c.Node("c").Healthy() {
		t.Fatal("node c should be healthy after catch-up")
	}
	t.Logf("PASS: c recovered from mid-install kill, snapshot=%d", rep.SnapshotIndex)
}

// ---- 用例 4：同一节点连续多次在不同阶段被强杀 ----

// 同一跟随者连续三轮被强杀：分别在单条日志落盘中途、批量追赶写盘中途、
// 自身快照压缩中途。每轮磁盘上都留有上一轮未处理完的残留，
// 每次重启都必须恢复成功；最终状态与全程不中断的结果一致，残留不累积。
func TestRepeatedKillsConverge(t *testing.T) {
	defer resetCrashHook()
	c := newTestCluster(t, "a", "b", "c")
	if c.WaitLeader(100) == nil {
		t.Fatal("no leader")
	}
	t.Logf("scenario: node c killed 3 times at different stages")

	writeOne := func(seq int) {
		t.Helper()
		leader := c.WaitLeader(100)
		if leader == nil {
			t.Fatalf("no leader for write %d", seq)
		}
		r := leader.Write(ClientWrite{ClientID: "cli1", Seq: int64(seq),
			Command: []byte(fmt.Sprintf("set:key%03d:val%03d", seq, seq))})
		if !r.Committed {
			t.Fatalf("write seq=%d not committed: %+v", seq, r)
		}
	}
	killAndRestart := func(round int) {
		t.Helper()
		c.KillNode("c")
		if err := c.RestartNode("c"); err != nil {
			t.Fatalf("round %d restart: %v", round, err)
		}
		rep := c.Node("c").LastRecovery()
		t.Logf("round %d recovery: %s", round, rep.String())
	}

	// 第 1 轮：单条日志落盘中途（tmp-written）
	writeBatch(t, c, "cli1", 1, 5)
	fired := armCrashOnce(nodeDir(c, "c"), "tmp-written")
	writeOne(6)
	if *fired == 0 {
		t.Fatal("round 1: crash hook never fired")
	}
	killAndRestart(1)
	c.Ticks(20)

	// 第 2 轮：落后后批量追赶的写盘中途（log-appended）
	c.Net().Disconnect("c", "a")
	c.Net().Disconnect("c", "b")
	c.Net().Disconnect("a", "c")
	c.Net().Disconnect("b", "c")
	writeBatch(t, c, "cli1", 7, 11)
	c.Net().Heal()
	fired = armCrashOnce(nodeDir(c, "c"), "log-appended")
	for i := 0; i < 100 && *fired == 0; i++ {
		c.Tick()
	}
	if *fired == 0 {
		t.Fatal("round 2: crash hook never fired")
	}
	killAndRestart(2)
	c.Ticks(20)

	// 第 3 轮：自身快照压缩中途（log-compacting）
	fired = armCrashOnce(nodeDir(c, "c"), "log-compacting")
	seq := 12
	for ; seq <= 45 && *fired == 0; seq++ {
		writeOne(seq)
	}
	if *fired == 0 {
		t.Fatal("round 3: crash hook never fired")
	}
	killAndRestart(3)

	// 收尾：再写几条，全集群追平
	lastWritten := seq - 1
	writeBatch(t, c, "cli1", lastWritten+1, lastWritten+3)
	total := lastWritten + 3
	c.Ticks(100)

	// 最终结果必须与全程不中断的写入序列严格一致
	assertConverged(t, c, wantKV(1, total), "a", "b", "c")
	if r := listResidue(t, c, "c"); len(r) > 0 {
		t.Fatalf("residue accumulated after repeated kills: %v", r)
	}
	if !c.Node("c").Healthy() {
		t.Fatal("node c should be healthy after repeated recovery")
	}
	rep := c.Node("c").LastRecovery()
	t.Logf("final recovery report: %s", rep.String())
	t.Logf("PASS: 3 kills at different stages, converged to %d keys", total)
}

// ---- 用例 5：恢复报告内容与对外服务门控 ----

// 带残留（临时文件、损坏的日志尾部）重启时：
//  1. 恢复报告必须准确记录清理了哪些残留、从哪里回退；
//  2. 节点在与集群收敛前不得对外提供读取，收敛后报告回填追赶结果。
func TestRecoveryReportAndHealthGate(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	t.Logf("scenario: restart with planted residue, verify report and read gate")

	writeBatch(t, c, "cli1", 1, 8)
	c.Ticks(5)
	c.KillNode("c")

	// 手工布置上一轮中断的残留：
	//  - 根目录与日志目录各一个临时文件（原子替换未完成）
	//  - 一条 CRC 损坏的日志尾部（撕裂写入）
	dir := nodeDir(c, "c")
	mustWrite := func(rel string, data []byte) {
		if err := os.WriteFile(filepath.Join(dir, rel), data, 0o644); err != nil {
			t.Fatalf("plant %s: %v", rel, err)
		}
	}
	mustWrite("hardstate.json.tmp", []byte(`{"Term":99`))
	mustWrite(filepath.Join(logDirName, fmt.Sprintf("%020d.entry.tmp", 11)), []byte("partial"))
	mustWrite(filepath.Join(logDirName, fmt.Sprintf("%020d.entry", 10)), []byte("torn-write-garbage"))

	if err := c.RestartNode("c"); err != nil {
		t.Fatalf("restart with residue: %v", err)
	}
	node := c.Node("c")

	// 门控：刚恢复、尚未与集群收敛，不得对外服务
	if node.Healthy() {
		t.Fatal("node must not be healthy right after restart")
	}
	if _, ok := node.ReadCommitted(); ok {
		t.Fatal("unhealthy node must not serve reads")
	}

	// 报告：残留清理与损坏尾部回退必须被准确记录
	rep := node.LastRecovery()
	t.Logf("recovery report: %s", rep.String())
	if rep.DiskTruncatedFrom != 10 {
		t.Fatalf("DiskTruncatedFrom=%d, want 10 (corrupt tail)", rep.DiskTruncatedFrom)
	}
	for _, want := range []string{
		"hardstate.json.tmp",
		filepath.Join(logDirName, fmt.Sprintf("%020d.entry.tmp", 11)),
		filepath.Join(logDirName, fmt.Sprintf("%020d.entry", 10)),
	} {
		found := false
		for _, got := range rep.ResidueCleaned {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("residue %q not reported in %v", want, rep.ResidueCleaned)
		}
	}
	if r := listResidue(t, c, "c"); len(r) > 0 {
		t.Fatalf("residue left on disk after recovery: %v", r)
	}

	// 收敛后：恢复健康，报告回填追赶结果，状态与集群一致
	c.Ticks(60)
	if !node.Healthy() {
		t.Fatal("node must become healthy after catch-up")
	}
	rep = node.LastRecovery()
	t.Logf("post-convergence report: %s", rep.String())
	if !rep.Healthy || rep.FilledTo == 0 {
		t.Fatalf("report not backfilled after convergence: %+v", rep)
	}
	assertConverged(t, c, wantKV(1, 8), "a", "b", "c")

	// 领导侧门控：重启后的领导在恢复健康前不得提供读取与提交
	leaderID := leader.ID()
	if _, ok := leader.ReadCommitted(); !ok {
		t.Fatal("healthy leader must serve reads")
	}
	c.KillNode(leaderID)
	if err := c.RestartNode(leaderID); err != nil {
		t.Fatalf("restart leader: %v", err)
	}
	restarted := c.Node(leaderID)
	if restarted.Healthy() {
		t.Fatal("restarted node must not be healthy before convergence")
	}
	if _, ok := restarted.ReadCommitted(); ok {
		t.Fatal("restarted node must not serve reads before convergence")
	}
	c.Ticks(100)
	if !restarted.Healthy() {
		t.Fatal("restarted node must converge to healthy")
	}
	newLeader := c.WaitLeader(100)
	if newLeader == nil {
		t.Fatal("no leader after restart")
	}
	if _, ok := newLeader.ReadCommitted(); !ok {
		t.Fatal("healthy leader must serve reads after convergence")
	}
	t.Logf("PASS: report accurate, read/write gate held until healthy")
}

// ---- 用例 6：恢复结果可复现，且与全程不中断一致 ----

// 同一强杀场景跑两遍（相同节点 ID → 相同时钟种子 → 确定性行为），
// 再跑一遍全程不中断的参照集群：三份最终状态必须完全一致；
// 两次中断的恢复报告关键字段也必须一致（结果不随中断次数/位置漂移）。
func TestKillRecoveryReproducible(t *testing.T) {
	defer resetCrashHook()
	t.Logf("scenario: same kill scenario x2 + uninterrupted reference")

	// runKill 执行一次“追赶写盘中途强杀”场景，返回最终状态与恢复报告。
	runKill := func(t *testing.T) (map[string]string, RecoveryReport) {
		c := newTestCluster(t, "a", "b", "c")
		leader := c.WaitLeader(100)
		if leader == nil {
			t.Fatal("no leader")
		}
		writeBatch(t, c, "cli1", 1, 5)
		c.Net().Disconnect("c", "a")
		c.Net().Disconnect("c", "b")
		c.Net().Disconnect("a", "c")
		c.Net().Disconnect("b", "c")
		writeBatch(t, c, "cli1", 6, 15)
		c.Net().Heal()
		fired := armCrashOnce(nodeDir(c, "c"), "log-appended")
		for i := 0; i < 100 && *fired == 0; i++ {
			c.Tick()
		}
		if *fired == 0 {
			t.Fatal("crash hook never fired")
		}
		c.KillNode("c")
		if err := c.RestartNode("c"); err != nil {
			t.Fatalf("restart: %v", err)
		}
		writeBatch(t, c, "cli1", 16, 20)
		c.Ticks(100)
		assertConverged(t, c, wantKV(1, 20), "a", "b", "c")
		return c.Node("c").AppliedKV(), c.Node("c").LastRecovery()
	}

	kv1, rep1 := runKill(t)
	t.Logf("kill run 1: %s", rep1.String())
	kv2, rep2 := runKill(t)
	t.Logf("kill run 2: %s", rep2.String())

	// 两次中断运行：最终状态与恢复过程完全可复现
	if fmt.Sprint(kv1) != fmt.Sprint(kv2) {
		t.Fatal("two interrupted runs diverged")
	}
	if rep1.DiskTruncatedFrom != rep2.DiskTruncatedFrom ||
		rep1.SnapshotIndex != rep2.SnapshotIndex ||
		rep1.LogKept != rep2.LogKept ||
		fmt.Sprint(rep1.ResidueCleaned) != fmt.Sprint(rep2.ResidueCleaned) {
		t.Fatalf("recovery not reproducible:\n run1=%+v\n run2=%+v", rep1, rep2)
	}

	// 全程不中断的参照集群
	ref := newTestCluster(t, "a", "b", "c")
	if ref.WaitLeader(100) == nil {
		t.Fatal("no leader in reference cluster")
	}
	writeBatch(t, ref, "cli1", 1, 20)
	ref.Ticks(100)
	want := ref.Node("c").AppliedKV()

	if fmt.Sprint(kv1) != fmt.Sprint(want) {
		t.Fatalf("interrupted result differs from uninterrupted reference:\n got %v\nwant %v", kv1, want)
	}
	t.Logf("PASS: interrupted runs reproducible and identical to uninterrupted run (%d keys)", len(want))
}
