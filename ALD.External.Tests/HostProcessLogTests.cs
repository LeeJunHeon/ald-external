using Xunit;

namespace ALD.External.Tests
{
    public class HostProcessLogTests
    {
        private static HostProcessLog NewLog(TempDirs d, string program = "ald")
        {
            var log = new HostProcessLog(d.Nas, d.Local, program);
            log.RetryIntervalMs = 200;
            return log;
        }

        // 1) 기본 흐름: Request → MarkStarted → Finalize(성공) → pending → 워커 → NAS 파일
        [Fact]
        public void Success_Line_Written_To_Nas_With_Header_And_Date_Crossing()
        {
            using var d = new TempDirs();
            using var log = NewLog(d);

            var recv = new DateTime(2026, 9, 17, 23, 59, 0, 123);
            var start = new DateTime(2026, 9, 17, 23, 59, 30, 0);
            var end = new DateTime(2026, 9, 18, 0, 10, 0, 0);

            string key = log.Request("ALD", "req-1", "192.168.0.10:51234", recv, "recipe_a.csv");
            log.Update(key, rowCount: "2", processNames: "Al2O3 / TiO2");
            log.MarkStarted(key, start);
            Assert.True(log.Finalize(key, "성공", "", end));

            Assert.Equal(1, TestUtil.PendingCount(log));
            log.EnsureWorker();
            log.Wake();

            string nas = log.NasFilePath("20260917");   // 요청 날짜 파일
            Assert.True(TestUtil.WaitUntil(() => TestUtil.PendingCount(log) == 0 && File.Exists(nas)));

            Assert.True(TestUtil.HasBom(nas));
            var lines = TestUtil.ReadLines(nas);
            Assert.Equal(2, lines.Length);
            Assert.Equal(HostProcessLog.Header, lines[0]);

            var f = TestUtil.ParseCsv(lines[1]);
            Assert.Equal(15, f.Count);
            Assert.Equal("ALD", f[0]);
            Assert.Equal("23:59:00", f[1]);
            Assert.Equal("23:59:30", f[2]);
            Assert.Equal("00:10:00", f[3]);
            Assert.Equal("10.5", f[4]);
            Assert.Equal("성공", f[5]);
            Assert.Equal("", f[6]);
            Assert.Equal("Al2O3 / TiO2", f[7]);
            Assert.Equal("recipe_a.csv", f[8]);
            Assert.Equal("2", f[9]);
            Assert.Equal("req-1", f[10]);
            Assert.Equal("192.168.0.10:51234", f[11]);
            Assert.Equal("", f[12]);
            Assert.Equal("ald", f[13]);
            Assert.Equal(key, f[14]);
            Assert.Equal("ald-20260917235900123-req-1", key);

            // 로컬 사본도 15칸
            var local = TestUtil.ReadLines(log.LocalCopyPath(recv));
            Assert.Equal(2, local.Length);
            Assert.Equal(lines[1], local[1]);
            Assert.False(File.Exists(log.NasFilePath("20260918")));
        }

        // 2) 거절 / 멱등 / IsOwned 아비트레이션
        [Fact]
        public void Reject_Is_Blank_Times_And_Finalize_Is_Idempotent()
        {
            using var d = new TempDirs();
            using var log = NewLog(d);

            var recv = new DateTime(2026, 9, 18, 10, 0, 0);
            string key = log.Request("ALD", "7", "1.2.3.4:5", recv);
            log.Reject(key, "csv_path not provided\r\nsecond line");
            Assert.False(log.Finalize(key, "성공"));           // 두 번째는 무시

            // 거절 vs 실패 아비트레이션: MarkOwned 된 키는 Reject 대상이 아니다(호출부 판단)
            string key2 = log.Request("ALD", "8", "1.2.3.4:5", recv);
            log.MarkOwned(key2);
            Assert.True(log.IsOwned(key2));
            if (!log.IsOwned(key2)) log.Reject(key2, "should not happen");
            Assert.True(log.Finalize(key2, "실패", "알람 관측(State=-1)", recv.AddMinutes(1)));
            Assert.False(log.Finalize(key2, "거절", "late reject"));

            log.EnsureWorker(); log.Wake();
            string nas = log.NasFilePath("20260918");
            Assert.True(TestUtil.WaitUntil(() => TestUtil.PendingCount(log) == 0 && File.Exists(nas)));

            var lines = TestUtil.ReadLines(nas);
            Assert.Equal(3, lines.Length);

            var f = TestUtil.ParseCsv(lines[1]);
            Assert.Equal("거절", f[5]);
            Assert.Equal("csv_path not provided | second line", f[6]);
            Assert.Equal("", f[2]); Assert.Equal("", f[3]); Assert.Equal("", f[4]);
            Assert.Equal("7", f[10]);

            var g = TestUtil.ParseCsv(lines[2]);
            Assert.Equal("실패", g[5]);
            Assert.Equal(key2, g[14]);
        }

        // 3) 잠금: 신선한 잠금 → pending 유지, 오래된 잠금 → 삭제 후 기록
        [Fact]
        public void Fresh_Lock_Blocks_And_Stale_Lock_Is_Removed()
        {
            using var d = new TempDirs();
            using var log = NewLog(d);

            string lockDir = Path.Combine(d.Nas, "_lock");
            Directory.CreateDirectory(lockDir);
            string lockPath = Path.Combine(lockDir, "Robot.lock");

            var recv = new DateTime(2026, 9, 18, 11, 0, 0);
            string key = log.Request("ALD", "L1", "p", recv);
            log.Finalize(key, "성공", "", recv.AddMinutes(1));

            // 신선한 잠금(다른 프로세스가 잡고 있는 상태)
            File.WriteAllText(lockPath, "sputter 1 2026-09-18 11:00:00");
            File.SetLastWriteTime(lockPath, DateTime.Now);
            log.ProcessRoundForTest();                    // 25 x 200ms 후 포기
            Assert.Equal(1, TestUtil.PendingCount(log));
            Assert.False(File.Exists(log.NasFilePath("20260918")));
            Assert.True(File.Exists(lockPath));           // 남의 잠금은 건드리지 않음

            // 오래된 잠금
            File.SetLastWriteTime(lockPath, DateTime.Now.AddSeconds(-60));
            log.ProcessRoundForTest();
            Assert.Equal(0, TestUtil.PendingCount(log));
            Assert.Equal(2, TestUtil.ReadLines(log.NasFilePath("20260918")).Length);
            Assert.False(File.Exists(lockPath));          // 작업 후 잠금 삭제
        }

        // 4) NAS 없음 → pending 누적, 호출부 즉시 반환 → 폴더 생기면 순서대로 전송
        [Fact]
        public void Nas_Missing_Keeps_Pending_Then_Sends_In_Order()
        {
            using var d = new TempDirs(createNas: false);
            string nasDir = Path.Combine(d.Root, "nas_missing", "deeper");
            using var log = new HostProcessLog(nasDir, d.Local, "ald") { RetryIntervalMs = 200 };
            // 폴더가 생성되면 자동으로 만들어지는 것을 막기 위해 부모를 파일로 막는다
            File.WriteAllText(Path.Combine(d.Root, "nas_missing"), "block");

            var recv = new DateTime(2026, 9, 18, 12, 0, 0);
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var keys = new List<string>();
            for (int i = 0; i < 3; i++)
            {
                string k = log.Request("ALD", "n" + i, "p", recv.AddSeconds(i));
                log.Finalize(k, "성공", "", recv.AddMinutes(i + 1));
                keys.Add(k);
            }
            Assert.True(sw.ElapsedMilliseconds < 2000, "Finalize must return immediately");
            Assert.Equal(3, TestUtil.PendingCount(log));

            log.EnsureWorker(); log.Wake();
            Thread.Sleep(600);
            Assert.Equal(3, TestUtil.PendingCount(log));

            // NAS 복구
            File.Delete(Path.Combine(d.Root, "nas_missing"));
            Directory.CreateDirectory(nasDir);
            log.Wake();

            string nas = log.NasFilePath("20260918");
            Assert.True(TestUtil.WaitUntil(() => TestUtil.PendingCount(log) == 0));
            var lines = TestUtil.ReadLines(nas);
            Assert.Equal(4, lines.Length);
            for (int i = 0; i < 3; i++)
                Assert.Equal(keys[i], TestUtil.ParseCsv(lines[i + 1])[14]);
        }

        // 5) NAS 에 이미 같은 기록키가 있으면 중복 없이 pending 만 제거
        [Fact]
        public void Duplicate_Key_On_Nas_Is_Not_Written_Twice()
        {
            using var d = new TempDirs();
            using var log = NewLog(d);

            var recv = new DateTime(2026, 9, 18, 13, 0, 0);
            string key = log.Request("ALD", "dup", "p", recv);
            log.Finalize(key, "성공", "", recv.AddMinutes(1));

            // 이미 NAS 에 기록된 상태를 흉내낸다 (pending 은 그대로)
            string nas = log.NasFilePath("20260918");
            string pendingLine = TestUtil.ReadLines(log.PendingPath)[0];
            string line15 = pendingLine.Substring(pendingLine.IndexOf(',') + 1);
            File.WriteAllText(nas, "﻿" + HostProcessLog.Header + "\r\n" + line15 + "\r\n");

            log.ProcessRoundForTest();
            Assert.Equal(0, TestUtil.PendingCount(log));
            Assert.Equal(2, TestUtil.ReadLines(nas).Length);
        }

        // 6) StartupRecover
        [Fact]
        public void StartupRecover_Handles_Pending_And_Orphan_Open_Entries()
        {
            using var d = new TempDirs();
            var recv = new DateTime(2026, 9, 18, 14, 0, 0);
            string kBoth, kNoStart, kStarted;

            using (var log1 = NewLog(d))
            {
                // pending 과 open 양쪽에 있는 키: Finalize 를 pending 까지만 하고 open 을 다시 복원한다
                kBoth = log1.Request("ALD", "both", "p", recv);
                kNoStart = log1.Request("ALD", "nostart", "p", recv.AddSeconds(1));
                kStarted = log1.Request("ALD", "started", "p", recv.AddSeconds(2));
                log1.MarkStarted(kStarted, recv.AddSeconds(30));
                // 워커 없음 → pending 에 남는다
                log1.Finalize(kBoth, "성공", "", recv.AddMinutes(1));
            }

            // open.json 에 kBoth 를 다시 넣어 "pending 에 쓰고 open 삭제 전에 죽은" 상황을 만든다
            string openJson = File.ReadAllText(d.Local + "\\open_ald.json");
            Assert.DoesNotContain(kBoth, openJson);
            openJson = openJson.TrimEnd().TrimEnd('}') +
                $",\n  \"{kBoth}\": {{ \"Target\": \"ALD\", \"RequestId\": \"both\", \"Peer\": \"p\", \"ReceivedAt\": \"2026-09-18T14:00:00\", \"StartedAt\": null, \"RecipeName\": \"\", \"RowCount\": \"\", \"ProcessNames\": \"\" }}\n}}";
            File.WriteAllText(d.Local + "\\open_ald.json", openJson);

            using var log2 = NewLog(d);
            Assert.True(log2.IsOpen(kBoth));
            log2.StartupRecover();

            Assert.False(log2.IsOpen(kBoth));
            Assert.False(log2.IsOpen(kNoStart));
            Assert.False(log2.IsOpen(kStarted));

            string nas = log2.NasFilePath("20260918");
            Assert.True(TestUtil.WaitUntil(() => TestUtil.PendingCount(log2) == 0 && File.Exists(nas)));
            var lines = TestUtil.ReadLines(nas);
            Assert.Equal(4, lines.Length);   // 헤더 + kBoth(성공) + 중단 2줄

            var rows = lines.Skip(1).Select(TestUtil.ParseCsv).ToList();
            Assert.Single(rows, r => r[14] == kBoth && r[5] == "성공");

            var a = rows.Single(r => r[14] == kNoStart);
            Assert.Equal("중단(재시작)", a[5]);
            Assert.Equal("프로그램 재시작 (시작 전)", a[6]);
            Assert.Equal("", a[2]); Assert.Equal("", a[3]); Assert.Equal("", a[4]);

            var b = rows.Single(r => r[14] == kStarted);
            Assert.Equal("중단(재시작)", b[5]);
            Assert.Equal("프로그램 재시작 (공정 중)", b[6]);
            Assert.Equal("14:00:30", b[2]); Assert.Equal("", b[3]); Assert.Equal("", b[4]);
        }

        // 7) 기록키 충돌
        [Fact]
        public void Key_Collision_Gets_Suffix()
        {
            using var d = new TempDirs();
            using var log = NewLog(d);
            var recv = new DateTime(2026, 9, 18, 15, 0, 0, 500);

            string k1 = log.Request("ALD", "same id!", "p", recv);
            string k2 = log.Request("ALD", "same id!", "p", recv);
            string k3 = log.Request("ALD", "same id!", "p", recv);

            Assert.Equal("ald-20260918150000500-same_id_", k1);
            Assert.Equal(k1 + "-2", k2);
            Assert.Equal(k1 + "-3", k3);

            // pending 에 있는 키와도 충돌 처리
            log.Finalize(k1, "성공", "", recv);
            log.Finalize(k2, "성공", "", recv);
            log.Finalize(k3, "성공", "", recv);
            string k4 = log.Request("ALD", "same id!", "p", recv);
            Assert.Equal(k1 + "-4", k4);

            // 64자 절단
            string longId = new string('a', 100);
            string k5 = log.Request("ALD", longId, "p", recv.AddSeconds(1));
            Assert.EndsWith("-" + new string('a', 64), k5);
        }

        // 8) 두 프로그램이 같은 NAS 폴더에 동시에 append
        [Fact]
        public void Two_Programs_Append_Concurrently_Without_Corruption()
        {
            using var d = new TempDirs();
            using var ald = new HostProcessLog(d.Nas, Path.Combine(d.Local, "ald"), "ald") { RetryIntervalMs = 100 };
            using var sp = new HostProcessLog(d.Nas, Path.Combine(d.Local, "sp"), "sputter") { RetryIntervalMs = 100 };
            ald.EnsureWorker();
            sp.EnsureWorker();

            var recv = new DateTime(2026, 9, 18, 16, 0, 0);

            Thread Run(HostProcessLog log, string target, int seed) => new Thread(() =>
            {
                var rnd = new Random(seed);
                for (int i = 0; i < 100; i++)
                {
                    string k = log.Request(target, "r" + i, "peer:" + i, recv.AddMilliseconds(i));
                    log.MarkStarted(k, recv.AddSeconds(i));
                    log.Finalize(k, i % 3 == 0 ? "성공" : "실패", "reason, with \"quotes\"\nline2", recv.AddSeconds(i + 60));
                    Thread.Sleep(rnd.Next(0, 5));
                }
            });

            var t1 = Run(ald, "ALD", 1);
            var t2 = Run(sp, "Sputter", 2);
            t1.Start(); t2.Start();
            t1.Join(); t2.Join();

            Assert.True(TestUtil.WaitUntil(
                () => TestUtil.PendingCount(ald) == 0 && TestUtil.PendingCount(sp) == 0, timeoutMs: 120000));

            string nas = ald.NasFilePath("20260918");
            var lines = TestUtil.ReadLines(nas);
            Assert.Equal(201, lines.Length);
            Assert.Equal(HostProcessLog.Header, lines[0]);
            Assert.Equal(1, lines.Count(l => l == HostProcessLog.Header));

            var keys = new HashSet<string>();
            foreach (var l in lines.Skip(1))
            {
                var f = TestUtil.ParseCsv(l);
                Assert.Equal(15, f.Count);
                Assert.True(f[13] == "ald" || f[13] == "sputter");
                if (f[5] == "성공") Assert.Equal("", f[6]);
                else Assert.Equal("reason, with \"quotes\" | line2", f[6]);
                Assert.True(keys.Add(f[14]), "duplicate key " + f[14]);
            }
            Assert.Equal(100, lines.Skip(1).Count(l => TestUtil.ParseCsv(l)[13] == "ald"));
            Assert.False(File.Exists(Path.Combine(d.Nas, "_lock", "Robot.lock")));
        }

        // 형식 유틸
        [Fact]
        public void Csv_Quoting_And_Formatting()
        {
            Assert.Equal("abc", HostProcessLog.CsvField("abc"));
            Assert.Equal("\"a,b\"", HostProcessLog.CsvField("a,b"));
            Assert.Equal("\"a\"\"b\"", HostProcessLog.CsvField("a\"b"));
            Assert.Equal("x | y | z", HostProcessLog.OneLine("x\r\ny\nz"));
            Assert.Equal("", HostProcessLog.FormatDurationMinutes(null, DateTime.Now));
            Assert.Equal("0.5", HostProcessLog.FormatDurationMinutes(new DateTime(2026, 1, 1, 0, 0, 0), new DateTime(2026, 1, 1, 0, 0, 30)));
            Assert.Equal("ab_c-d_", HostProcessLog.SanitizeRequestId("ab c-d/"));
        }
    }
}
