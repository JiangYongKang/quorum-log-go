package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// ---- 测试辅助 ----

// writeCommitted 向当前领导写入并等待提交成功（领导更替时自动重试同一幂等标识）。
func writeCommitted(t *testing.T, c *Cluster, seq int64, cmd string) {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		l := c.WaitLeader(100)
		if l == nil {
			c.Ticks(10)
			continue
		}
		r := l.Write(ClientWrite{ClientID: "cli1", Seq: seq, Command: []byte(cmd)})
		if r.Committed {
			return
		}
		t.Logf("write seq=%d not committed yet (%s), retrying", seq, r.Err)
		c.Ticks(5)
	}
	t.Fatalf("write seq=%d never committed", seq)
}

// runReference 跑一个全程不中断的参考集群，写入 n 条，返回最终状态机内容。
func runReference(t *testing.T, n int) map[string]string {
	t.Helper()
	c := newTestCluster(t, "a", "b", "c")
	if c.WaitLeader(100) == nil {
		t.Fatal("reference: no leader")
	}
	for i := 1; i <= n; i++ {
		writeCommitted(t, c, int64(i), fmt.Sprintf("set:k%02d:v%02d", i, i))
	}
	c.Ticks(10)
	return c.Leader().AppliedKV()
}

// pickFollower 任选一个非领导节点。
func pickFollower(c *Cluster) string {
	leader := c.Leader()
	for _, id := range []string{"a", "b", "c"} {
		if leader == nil || id != leader.ID() {
			return id
		}
	}
	return "a"
}

// tmpResidue 列出节点数据目录下残留的临时文件。
func tmpResidue(c *Cluster, id string) []string {
	var out []string
	root := c.cfgs[id].DataDir
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(p, ".tmp") {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// ---- 场景：处理客户端写入中途被强杀 ----

func TestKillDuringClientWrite(t *testing.T) {
	ref := runReference(t, 5)
	t.Logf("reference cluster finished 5 writes, %d keys", len(ref))

	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	for i := 1; i <= 3; i++ {
		writeCommitted(t, c, int64(i), fmt.Sprintf("set:k%02d:v%02d", i, i))
	}

	// 领导在落盘客户端写入的中途（tmp 已写、rename 之前）被强杀
	victim := leader.ID()
	leader.ArmCrash(CrashAfterTmpWrite)
	r := leader.Write(ClientWrite{ClientID: "cli1", Seq: 4, Command: []byte("set:k04:v04")})
	if r.Committed {
		t.Fatal("write interrupted by kill must not report committed")
	}
	t.Logf("killed leader %s mid-write (tmp residue left on disk)", victim)
	c.StopNode(victim)

	// 余下节点选出新领导；被强杀的节点带残留重启
	c.Ticks(30)
	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("restart after kill: %v", err)
	}
	rep := c.Node(victim).RecoveryReport()
	if len(rep.TempFilesRemoved) == 0 {
		t.Fatalf("expected temp-file residue cleaned, report: %s", rep.String())
	}
	t.Logf("restart cleaned residue: %s", rep.String())

	// 客户端用相同幂等标识重试被中断的写入，再补一条
	writeCommitted(t, c, 4, "set:k04:v04")
	writeCommitted(t, c, 5, "set:k05:v05")
	c.Ticks(30)

	got := c.Node(victim).AppliedKV()
	if !reflect.DeepEqual(got, ref) {
		t.Fatalf("killed-then-recovered node state %v != uninterrupted reference %v", got, ref)
	}
	if !c.Node(victim).Ready() {
		t.Fatal("recovered node should be ready after catching up")
	}
	t.Logf("node %s recovered and converged with reference (%d keys)", victim, len(got))
}

// ---- 场景：做状态压缩（快照）中途被强杀 ----

func TestKillDuringSnapshot(t *testing.T) {
	ref := runReference(t, 20)

	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	// 写 18 条（不触发快照），然后注入“快照已替换、日志压缩前”崩溃点
	for i := 1; i <= 18; i++ {
		writeCommitted(t, c, int64(i), fmt.Sprintf("set:k%02d:v%02d", i, i))
	}
	victim := leader.ID()
	leader.ArmCrash(CrashSnapshotCompact)
	writeCommitted(t, c, 19, "set:k19:v19") // 第 19 条触发快照，压缩中途崩溃
	t.Logf("killed leader %s mid-snapshot-compaction", victim)
	c.StopNode(victim)

	c.Ticks(30)
	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("restart after kill: %v", err)
	}
	rep := c.Node(victim).RecoveryReport()
	if len(rep.StaleLogsRemoved) == 0 {
		t.Fatalf("expected stale pre-snapshot logs cleaned, report: %s", rep.String())
	}
	if rep.SnapshotIndex == 0 {
		t.Fatalf("expected recovery from snapshot, report: %s", rep.String())
	}
	t.Logf("restart recovered from snapshot and cleaned stale logs: %s", rep.String())

	writeCommitted(t, c, 20, "set:k20:v20")
	c.Ticks(30)

	got := c.Node(victim).AppliedKV()
	if !reflect.DeepEqual(got, ref) {
		t.Fatalf("recovered node state %v != reference %v", got, ref)
	}
	if left := tmpResidue(c, victim); len(left) > 0 {
		t.Fatalf("residue left after recovery: %v", left)
	}
	t.Logf("node %s recovered from mid-snapshot kill, converged (%d keys)", victim, len(got))
}

// ---- 场景：给落后节点补数据的中途被强杀 ----

func TestKillDuringCatchUp(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	victim := pickFollower(c)

	// 隔离 victim，写 10 条（不触发快照，保证追赶走 AppendEntries 批量补数据）
	c.Net().Disconnect(leader.ID(), victim)
	for _, id := range []string{"a", "b", "c"} {
		if id != victim && id != leader.ID() {
			c.Net().Disconnect(id, victim)
		}
	}
	for i := 1; i <= 10; i++ {
		writeCommitted(t, c, int64(i), fmt.Sprintf("set:k%02d:v%02d", i, i))
	}
	want := leader.AppliedKV()

	// 恢复网络，victim 在批量补日志写到一半时被强杀
	c.Net().Heal()
	c.Node(victim).ArmCrash(CrashMidAppend)
	c.Ticks(3)
	t.Logf("killed node %s mid-catch-up (partial batch on disk)", victim)
	c.StopNode(victim)

	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("restart after kill: %v", err)
	}
	if c.Node(victim).Ready() {
		t.Fatal("node must not be ready right after restart")
	}
	c.Ticks(30)

	got := c.Node(victim).AppliedKV()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("re-caught-up node state %v != leader %v", got, want)
	}
	rep := c.Node(victim).RecoveryReport()
	if !rep.Ready || rep.CaughtUpTo != len(want)+1 { // 10 条写入 + 1 条 no-op
		t.Fatalf("unexpected recovery report: %s", rep.String())
	}
	t.Logf("node %s re-caught-up after mid-catch-up kill: %s", victim, rep.String())
}

// ---- 场景：连续多次强杀 + 残留不累积 + 结果可复现 ----

func TestRepeatedKillsDeterministic(t *testing.T) {
	const writes = 25
	ref := runReference(t, writes)

	// 同一流程跑两遍，验证多次中断后的最终状态可复现
	var finals []map[string]string
	for run := 1; run <= 2; run++ {
		func() {
			c := newTestCluster(t, "a", "b", "c")
			if c.WaitLeader(100) == nil {
				t.Fatal("no leader")
			}
			victim := pickFollower(c)
			seq := 0
			// 连续 3 轮：每轮写若干条后对同一节点强杀一次（磁盘带上一轮残留重启）
			for round := 1; round <= 3; round++ {
				for i := 0; i < 5; i++ {
					seq++
					writeCommitted(t, c, int64(seq), fmt.Sprintf("set:k%02d:v%02d", seq, seq))
				}
				c.Node(victim).ArmCrash(CrashAfterTmpWrite)
				seq++
				writeCommitted(t, c, int64(seq), fmt.Sprintf("set:k%02d:v%02d", seq, seq)) // 触发崩溃点
				c.StopNode(victim)
				if err := c.RestartNode(victim); err != nil {
					t.Fatalf("run=%d round=%d restart: %v", run, round, err)
				}
				c.Ticks(30)
				if !c.Node(victim).Ready() {
					t.Fatalf("run=%d round=%d: node not ready after recovery", run, round)
				}
				t.Logf("run=%d round=%d: node %s killed and recovered: %s",
					run, round, victim, c.Node(victim).RecoveryReport().String())
			}
			// 补足剩余写入
			for seq < writes {
				seq++
				writeCommitted(t, c, int64(seq), fmt.Sprintf("set:k%02d:v%02d", seq, seq))
			}
			c.Ticks(30)

			got := c.Node(victim).AppliedKV()
			if !reflect.DeepEqual(got, ref) {
				t.Fatalf("run=%d: final state %v != uninterrupted reference %v", run, got, ref)
			}
			if left := tmpResidue(c, victim); len(left) > 0 {
				t.Fatalf("run=%d: residue accumulated: %v", run, left)
			}
			// 再重启一次：不应再清出任何残留（残留不累积、恢复幂等）
			c.StopNode(victim)
			if err := c.RestartNode(victim); err != nil {
				t.Fatalf("run=%d extra restart: %v", run, err)
			}
			rep := c.Node(victim).RecoveryReport()
			if len(rep.TempFilesRemoved) != 0 || len(rep.StaleLogsRemoved) != 0 || len(rep.RolledBack) != 0 {
				t.Fatalf("run=%d: residue reappeared after clean recovery: %s", run, rep.String())
			}
			finals = append(finals, got)
		}()
	}
	if !reflect.DeepEqual(finals[0], finals[1]) {
		t.Fatalf("two interrupted runs diverged: %v vs %v", finals[0], finals[1])
	}
	t.Logf("two interrupted runs both converged to reference (%d keys)", len(ref))
}

// ---- 场景：恢复报告内容与健康门禁 ----

func TestRecoveryReportAndReadinessGate(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	for i := 1; i <= 5; i++ {
		writeCommitted(t, c, int64(i), fmt.Sprintf("set:k%02d:v%02d", i, i))
	}
	c.Ticks(5)

	// 健康领导的对外读取必须与实际应用状态严格一致
	kv, ok := c.Leader().ReadCommitted()
	if !ok {
		t.Fatal("ready leader should serve committed reads")
	}
	if !reflect.DeepEqual(kv, c.Leader().AppliedKV()) {
		t.Fatalf("committed view %v != applied state %v", kv, c.Leader().AppliedKV())
	}

	// 制造残留后重启 victim
	victim := pickFollower(c)
	c.Node(victim).ArmCrash(CrashAfterTmpWrite)
	writeCommitted(t, c, 6, "set:k06:v06")
	c.StopNode(victim)
	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("restart: %v", err)
	}

	// 重启后尚未追平：不 ready，不对外服务
	n := c.Node(victim)
	if n.Ready() {
		t.Fatal("node must not be ready before catching up")
	}
	rep := n.RecoveryReport()
	if rep.Ready || len(rep.TempFilesRemoved) == 0 {
		t.Fatalf("unexpected report right after restart: %s", rep.String())
	}
	// 即使（假设的）领导身份，未恢复健康也拒绝提交与读取
	n.mu.Lock()
	n.role = Leader
	n.mu.Unlock()
	if r := n.Write(ClientWrite{ClientID: "cli2", Seq: 1, Command: []byte("set:x:y")}); r.OK {
		t.Fatal("unready node must reject client writes")
	}
	if _, ok := n.ReadCommitted(); ok {
		t.Fatal("unready node must reject committed reads")
	}
	n.mu.Lock()
	n.role = Follower
	n.mu.Unlock()

	// 追平后：ready，报告定稿且可 JSON 序列化
	c.Ticks(30)
	if !n.Ready() {
		t.Fatal("node should be ready after catching up with cluster")
	}
	rep = n.RecoveryReport()
	if !rep.Ready || rep.CaughtUpTo < rep.CaughtUpFrom {
		t.Fatalf("bad final report: %s", rep.String())
	}
	buf, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("report not JSON-serializable: %v", err)
	}
	t.Logf("final recovery report: %s", rep.String())
	t.Logf("final recovery report (json): %s", buf)
}

// ---- 单元：Recover 对各类残留的清理与幂等性 ----

func encodeEntryFile(e LogEntry) []byte {
	payload, _ := json.Marshal(e)
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf, crc32.ChecksumIEEE(payload))
	copy(buf[4:], payload)
	return buf
}

func TestStorageRecoverCleansResidue(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 正常写入 1..5，快照覆盖 1..3（正常压缩后磁盘剩 4,5）
	ents := []LogEntry{}
	for i := 1; i <= 5; i++ {
		ents = append(ents, LogEntry{Term: 1, Index: i, ClientID: "c", Seq: int64(i)})
	}
	if err := s.AppendLog(ents); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(3, 1, []byte(`{"kv":{},"seq":{}}`)); err != nil {
		t.Fatal(err)
	}

	// 手工制造上一轮强杀留下的残留：
	//  - 快照已覆盖但未删掉的旧日志 1,2
	os.WriteFile(logEntryPath(dir, 1), encodeEntryFile(LogEntry{Term: 1, Index: 1}), 0o644)
	os.WriteFile(logEntryPath(dir, 2), encodeEntryFile(LogEntry{Term: 1, Index: 2}), 0o644)
	//  - 未完成的临时文件（硬状态与日志各一）
	os.WriteFile(filepath.Join(dir, hardStateFile+".tmp"), []byte(`{"Term":9}`), 0o644)
	os.WriteFile(logEntryPath(dir, 6)+".tmp", encodeEntryFile(LogEntry{Term: 2, Index: 6}), 0o644)
	//  - 空洞后缀：6,7 缺失但 8 存在
	os.WriteFile(logEntryPath(dir, 8), encodeEntryFile(LogEntry{Term: 2, Index: 8}), 0o644)
	//  - 损坏文件 9（CRC 校验不过）
	os.WriteFile(logEntryPath(dir, 9), []byte("garbage"), 0o644)

	valid, info, err := s.Recover(3)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(valid) != 2 || valid[0].Index != 4 || valid[1].Index != 5 {
		t.Fatalf("valid prefix = %v, want entries 4,5", valid)
	}
	if !reflect.DeepEqual(info.StaleLogs, []int{1, 2}) {
		t.Fatalf("stale logs = %v, want [1 2]", info.StaleLogs)
	}
	if !reflect.DeepEqual(info.Truncated, []int{8, 9}) {
		t.Fatalf("truncated = %v, want [8 9]", info.Truncated)
	}
	if len(info.TempFiles) != 2 {
		t.Fatalf("temp files = %v, want 2 entries", info.TempFiles)
	}
	t.Logf("recover cleaned: stale=%v truncated=%v tmp=%v", info.StaleLogs, info.Truncated, info.TempFiles)

	// 幂等：再次恢复不应清出任何残留，有效前缀不变
	valid2, info2, err := s.Recover(3)
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if len(info2.StaleLogs)+len(info2.Truncated)+len(info2.TempFiles) != 0 {
		t.Fatalf("second recover found residue again: %+v", info2)
	}
	if !reflect.DeepEqual(valid2, valid) {
		t.Fatalf("second recover changed prefix: %v vs %v", valid2, valid)
	}
	t.Log("recover is idempotent: second run cleaned nothing, prefix unchanged")
}
