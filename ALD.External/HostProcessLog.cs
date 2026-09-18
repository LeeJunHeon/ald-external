// HostProcessLog.cs
//
// 외부 클라이언트(호스트/로봇)가 요청한 공정을 하루 1개 공유 CSV 에 기록한다.
// (Sputter 프로그램과 공통 사양 v3.1 — 파일·컬럼·잠금 규약은 바꾸지 말 것)
//
// 동작 개요
//   Request  → open_<prog>.json 에 미종료 요청 등록 (로컬)
//   Finalize → 15칸 줄 생성 → pending_<prog>.csv append (로컬) → 로컬 사본 append → open 에서 제거 → 워커 깨움
//   워커     → pending 을 파일 순서대로 NAS Robot_YYYYMMDD.csv 에 append (잠금 파일 사용, 기록키 중복 검사)
//
// 호출 스레드(통신 스레드)는 로컬 파일 1줄 append 까지만 하고, NAS 는 워커 스레드만 접근한다.
// 어떤 메서드도 예외를 밖으로 내지 않는다(실패는 hostlog_debug.txt 에만 기록).
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;

namespace ALD.External
{
    internal sealed class HostProcessLog : IDisposable
    {
        // ==== 상수 ====

        /// <summary>NAS 공용 폴더. 공용 파일 Robot_YYYYMMDD.csv (append 전용), 잠금 _lock\Robot.lock</summary>
        public const string NasDir = @"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Robot";

        /// <summary>로컬 폴더 기본값. 내 줄 사본 / pending / open 파일이 여기에 생긴다.</summary>
        public static string DefaultLocalDir => Path.Combine(AppContext.BaseDirectory, "Logs_Robot");

        /// <summary>워커가 깨움 신호 없이도 pending 을 재시도하는 주기(초)</summary>
        public const int RetrySeconds = 30;

        /// <summary>
        /// 잠금 파일의 LastWriteTime 이 이보다 오래되면 죽은 잠금으로 보고 삭제한다(초).
        /// 정상 잠금 보유는 수십 ms 이고, 살아 있는 잠금은 FileShare.None 이라 삭제 자체가 실패(공유 위반)하므로
        /// 이 값은 잠금을 쥔 채 크래시한 프로세스를 복구하기 위한 것이다. 넉넉히 둔다.
        /// </summary>
        public const int LockStaleSeconds = 120;

        /// <summary>프로그램명(파일 접미사·프로그램 컬럼)</summary>
        public const string DefaultProgramName = "ald";

        private const string LockFileName = "Robot.lock";
        private const string LockDirName = "_lock";

        /// <summary>컬럼 15개, 순서 고정</summary>
        public const string Header =
            "대상,요청시각,시작시각,종료시각,소요(분),결과,사유,공정명,레시피,행개수,request_id,peer,로그파일,프로그램,기록키";

        public const int ColumnCount = 15;

        // ==== 상태 ====

        private readonly string _nasDir;
        private readonly string _localDir;
        private readonly string _program;

        private readonly string _pendingPath;
        private readonly string _openPath;
        private readonly string _debugPath;

        // open/pending/키 생성/메모리 큐를 모두 보호한다. 잡고 있는 동안 NAS 는 절대 접근하지 않는다.
        private readonly object _sync = new();
        private readonly Dictionary<string, OpenEntry> _open = new(StringComparer.Ordinal);
        private readonly HashSet<string> _owned = new(StringComparer.Ordinal);
        private string? _lastKey;

        // pending append 에 실패한 줄. 워커가 다시 pending 에 넣어 보낸다.
        private readonly List<PendingItem> _memoryQueue = new();

        // 워커
        private Thread? _worker;
        private readonly AutoResetEvent _wake = new(false);
        private volatile bool _stop;

        /// <summary>재시도 주기(ms). 테스트에서만 줄인다.</summary>
        internal int RetryIntervalMs { get; set; } = RetrySeconds * 1000;

        public string ProgramName => _program;
        public string LocalDir => _localDir;
        public string NasDirectory => _nasDir;

        public HostProcessLog(string? nasDir = null, string? localDir = null, string? programName = null)
        {
            _nasDir = string.IsNullOrWhiteSpace(nasDir) ? NasDir : nasDir!;
            _localDir = string.IsNullOrWhiteSpace(localDir) ? DefaultLocalDir : localDir!;
            _program = string.IsNullOrWhiteSpace(programName) ? DefaultProgramName : programName!;

            _pendingPath = Path.Combine(_localDir, $"pending_{_program}.csv");
            _openPath = Path.Combine(_localDir, $"open_{_program}.json");
            _debugPath = Path.Combine(_localDir, "hostlog_debug.txt");

            try
            {
                Directory.CreateDirectory(_localDir);
                LoadOpen();
            }
            catch (Exception ex)
            {
                Debug("ctor: " + ex.Message);
            }
        }

        // ==== 파일 경로 ====

        public string PendingPath => _pendingPath;
        public string OpenPath => _openPath;
        public string LocalCopyPath(DateTime requestDate)
            => Path.Combine(_localDir, $"Robot_{requestDate:yyyyMMdd}_{_program}.csv");
        public string NasFilePath(string yyyymmdd)
            => Path.Combine(_nasDir, $"Robot_{yyyymmdd}.csv");
        private string LockPath => Path.Combine(_nasDir, LockDirName, LockFileName);

        // ==== 공개 API (전부 동기, 예외를 밖으로 내지 않음) ====

        /// <summary>
        /// 요청 수신 즉시 호출. 기록키를 만들고 open 에 등록한다. 반환값은 기록키(실패해도 키는 반환).
        /// </summary>
        public string Request(
            string target,
            string requestId,
            string peer,
            DateTime receivedAt,
            string recipeName = "",
            string rowCount = "",
            string processNames = "")
        {
            string key = "";
            try
            {
                lock (_sync)
                {
                    key = MakeUniqueKeyLocked(receivedAt, requestId);
                    _open[key] = new OpenEntry
                    {
                        Target = target ?? "",
                        RequestId = requestId ?? "",
                        Peer = peer ?? "",
                        ReceivedAt = receivedAt,
                        RecipeName = recipeName ?? "",
                        RowCount = rowCount ?? "",
                        ProcessNames = processNames ?? ""
                    };
                    _lastKey = key;
                    SaveOpenLocked();
                }
            }
            catch (Exception ex)
            {
                Debug("Request: " + ex.Message);
            }
            return key;
        }

        public void Update(string key, string? recipeName = null, string? rowCount = null, string? processNames = null)
        {
            try
            {
                lock (_sync)
                {
                    if (!_open.TryGetValue(key, out var e)) return;
                    if (recipeName != null) e.RecipeName = recipeName;
                    if (rowCount != null) e.RowCount = rowCount;
                    if (processNames != null) e.ProcessNames = processNames;
                    SaveOpenLocked();
                }
            }
            catch (Exception ex)
            {
                Debug("Update: " + ex.Message);
            }
        }

        /// <summary>최초 1회만 시작일시를 기록한다.</summary>
        public void MarkStarted(string key, DateTime startedAt)
        {
            try
            {
                lock (_sync)
                {
                    if (!_open.TryGetValue(key, out var e)) return;
                    if (e.StartedAt.HasValue) return;
                    e.StartedAt = startedAt;
                    SaveOpenLocked();
                }
            }
            catch (Exception ex)
            {
                Debug("MarkStarted: " + ex.Message);
            }
        }

        public void MarkOwned(string key)
        {
            lock (_sync) _owned.Add(key ?? "");
        }

        public bool IsOwned(string key)
        {
            lock (_sync) return _owned.Contains(key ?? "");
        }

        public bool IsOpen(string key)
        {
            lock (_sync) return _open.ContainsKey(key ?? "");
        }

        public DateTime? GetStartedAt(string key)
        {
            lock (_sync) return _open.TryGetValue(key ?? "", out var e) ? e.StartedAt : null;
        }

        public void Reject(string key, string reason) => Finalize(key, "거절", reason);

        /// <summary>
        /// 요청 종료 기록. 멱등(open 에 key 가 없으면 false).
        /// pending append(로컬) 까지만 이 스레드에서 하고, NAS 전송은 워커가 한다.
        /// </summary>
        public bool Finalize(string key, string result, string reason = "", DateTime? finishedAt = null)
        {
            try
            {
                lock (_sync)
                {
                    if (!_open.TryGetValue(key ?? "", out var e)) return false;
                    if (_memoryQueue.Any(q => q.Key == key)) return false;   // 이미 종료 줄이 큐에 있음

                    string line = BuildLine(e, result, reason, finishedAt);
                    var item = new PendingItem(e.ReceivedAt.ToString("yyyyMMdd", CultureInfo.InvariantCulture), line, key!);

                    // ① pending 에 append (FileStream Flush(true))
                    if (!TryAppendPendingLocked(item))
                    {
                        // 로컬 디스크 문제: open 유지 + 메모리 큐. 워커가 다시 시도한다.
                        _memoryQueue.Add(item);
                        _owned.Remove(key!);
                        _wake.Set();
                        return true;
                    }

                    // ② 내 줄 사본 (실패 무시)
                    TryAppendLocalCopy(e.ReceivedAt, line);

                    // ③ open 에서 삭제
                    _open.Remove(key!);
                    _owned.Remove(key!);
                    SaveOpenLocked();
                }

                // ④ 워커 깨움
                _wake.Set();
                return true;
            }
            catch (Exception ex)
            {
                Debug("Finalize: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// 프로그램 시작 시 1회.
        /// pending 에 이미 있는 키의 open 항목은 지우기만 하고, open 에만 있는 항목은 "중단(재시작)" 으로 닫는다.
        /// 로컬 파일만 사용하며 NAS 를 기다리지 않는다.
        /// </summary>
        public void StartupRecover()
        {
            try
            {
                List<string> orphanKeys;
                lock (_sync)
                {
                    var pendingKeys = ReadPendingKeysLocked();
                    orphanKeys = new List<string>();

                    foreach (var key in _open.Keys.ToList())
                    {
                        if (pendingKeys.Contains(key))
                            _open.Remove(key);          // 이미 pending 에 있으니 중복 줄을 만들지 않는다
                        else
                            orphanKeys.Add(key);
                    }
                    SaveOpenLocked();
                }

                foreach (var key in orphanKeys)
                {
                    DateTime? started = GetStartedAt(key);
                    string reason = started.HasValue
                        ? "프로그램 재시작 (공정 중)"
                        : "프로그램 재시작 (시작 전)";
                    Finalize(key, "중단(재시작)", reason, finishedAt: null);
                }
            }
            catch (Exception ex)
            {
                Debug("StartupRecover: " + ex.Message);
            }

            EnsureWorker();
        }

        /// <summary>워커에 종료 신호를 보내고 timeout 만 기다린다.</summary>
        public void Close(TimeSpan timeout)
        {
            try
            {
                _stop = true;
                _wake.Set();
                var w = _worker;
                if (w != null && w.IsAlive && w != Thread.CurrentThread)
                    w.Join(timeout);
                _worker = null;
            }
            catch (Exception ex)
            {
                Debug("Close: " + ex.Message);
            }
        }

        public void Dispose() => Close(TimeSpan.FromSeconds(2));

        /// <summary>워커가 없으면 시작한다(StartupRecover 가 호출). 테스트에서도 직접 호출 가능.</summary>
        public void EnsureWorker()
        {
            try
            {
                lock (_sync)
                {
                    if (_worker != null && _worker.IsAlive) return;
                    _stop = false;
                    _worker = new Thread(WorkerLoop)
                    {
                        IsBackground = true,
                        Name = $"HostProcessLog-{_program}"
                    };
                    _worker.Start();
                }
            }
            catch (Exception ex)
            {
                Debug("EnsureWorker: " + ex.Message);
            }
        }

        /// <summary>테스트/진단용: 지금 바로 한 라운드 보내도록 깨운다.</summary>
        public void Wake() => _wake.Set();

        /// <summary>테스트/진단용: 워커 스레드 없이 이 스레드에서 한 라운드 처리한다(NAS 접근).</summary>
        internal void ProcessRoundForTest() => ProcessRound();

        // ==== 줄 생성 ====

        internal static string FormatTime(DateTime? t)
            => t.HasValue ? t.Value.ToString("HH:mm:ss", CultureInfo.InvariantCulture) : "";

        internal static string FormatDurationMinutes(DateTime? start, DateTime? end)
        {
            if (!start.HasValue || !end.HasValue) return "";
            double m = (end.Value - start.Value).TotalMinutes;
            return m.ToString("F1", CultureInfo.InvariantCulture);
        }

        internal static string OneLine(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return s!.Replace("\r\n", " | ").Replace("\r", " | ").Replace("\n", " | ");
        }

        /// <summary>RFC4180: 콤마/따옴표/개행이 있으면 "..." 로 감싸고 내부 따옴표는 "" 로.</summary>
        internal static string CsvField(string? s)
        {
            s ??= "";
            if (s.IndexOfAny(new[] { ',', '"', '\r', '\n' }) < 0) return s;
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }

        internal static string JoinCsv(IEnumerable<string> fields)
            => string.Join(",", fields.Select(CsvField));

        private static string BuildLine(OpenEntry e, string result, string reason, DateTime? finishedAt)
        {
            // 시작하지 않은 요청은 종료시각·소요(분) 빈칸 (Sputter 구현 _build_row 와 동일 규칙)
            if (!e.StartedAt.HasValue)
                finishedAt = null;

            string res = result ?? "";
            string rsn = res == "성공" ? "" : OneLine(reason);

            var fields = new[]
            {
                e.Target,                                       // 대상
                FormatTime(e.ReceivedAt),                       // 요청시각
                FormatTime(e.StartedAt),                        // 시작시각
                FormatTime(finishedAt),                         // 종료시각
                FormatDurationMinutes(e.StartedAt, finishedAt), // 소요(분)
                res,                                            // 결과
                rsn,                                            // 사유
                e.ProcessNames,                                 // 공정명
                e.RecipeName,                                   // 레시피
                e.RowCount,                                     // 행개수
                e.RequestId,                                    // request_id
                e.Peer,                                         // peer
                "",                                             // 로그파일 (ALD 는 per-run 로그 없음)
                e.Program,                                      // 프로그램
                e.Key                                           // 기록키
            };
            return JoinCsv(fields);
        }

        // ==== 기록키 ====

        internal static string SanitizeRequestId(string? requestId)
        {
            var sb = new StringBuilder();
            foreach (char c in requestId ?? "")
            {
                bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
                sb.Append(ok ? c : '_');
            }
            string s = sb.ToString();
            return s.Length > 64 ? s.Substring(0, 64) : s;
        }

        private string MakeUniqueKeyLocked(DateTime receivedAt, string requestId)
        {
            string baseKey = $"{_program}-{receivedAt:yyyyMMddHHmmssfff}-{SanitizeRequestId(requestId)}";
            HashSet<string>? pendingKeys = null;

            string key = baseKey;
            for (int n = 2; ; n++)
            {
                bool clash = _open.ContainsKey(key) || key == _lastKey;
                if (!clash)
                {
                    pendingKeys ??= ReadPendingKeysLocked();
                    clash = pendingKeys.Contains(key);
                }
                if (!clash) return key;
                key = baseKey + "-" + n.ToString(CultureInfo.InvariantCulture);
            }
        }

        // ==== open_<prog>.json ====

        private sealed class OpenEntry
        {
            [JsonIgnore] public string Key { get; set; } = "";
            [JsonIgnore] public string Program { get; set; } = "";
            public string Target { get; set; } = "";
            public string RequestId { get; set; } = "";
            public string Peer { get; set; } = "";
            public DateTime ReceivedAt { get; set; }
            public DateTime? StartedAt { get; set; }
            public string RecipeName { get; set; } = "";
            public string RowCount { get; set; } = "";
            public string ProcessNames { get; set; } = "";
        }

        private static readonly JsonSerializerOptions JsonOpts = new()
        {
            WriteIndented = true,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private void LoadOpen()
        {
            lock (_sync)
            {
                _open.Clear();
                if (!File.Exists(_openPath)) return;
                string json = File.ReadAllText(_openPath, Encoding.UTF8);
                if (string.IsNullOrWhiteSpace(json)) return;
                var dict = JsonSerializer.Deserialize<Dictionary<string, OpenEntry>>(json, JsonOpts);
                if (dict == null) return;
                foreach (var kv in dict)
                {
                    kv.Value.Key = kv.Key;
                    kv.Value.Program = _program;
                    _open[kv.Key] = kv.Value;
                }
            }
        }

        private void SaveOpenLocked()
        {
            foreach (var kv in _open)
            {
                kv.Value.Key = kv.Key;
                kv.Value.Program = _program;
            }
            string json = JsonSerializer.Serialize(_open, JsonOpts);
            string tmp = _openPath + ".tmp";
            Directory.CreateDirectory(_localDir);
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                byte[] bytes = Encoding.UTF8.GetBytes(json);
                fs.Write(bytes, 0, bytes.Length);
                fs.Flush(true);
            }
            File.Move(tmp, _openPath, overwrite: true);
        }

        // ==== pending_<prog>.csv (16칸 = 요청날짜 + 15칸) ====

        private sealed record PendingItem(string Date, string Line, string Key)
        {
            public string ToPendingLine() => Date + "," + Line;
        }

        private bool TryAppendPendingLocked(PendingItem item)
        {
            try
            {
                Directory.CreateDirectory(_localDir);
                using var fs = new FileStream(_pendingPath, FileMode.Append, FileAccess.Write, FileShare.Read);
                byte[] bytes = Encoding.UTF8.GetBytes(item.ToPendingLine() + "\r\n");
                fs.Write(bytes, 0, bytes.Length);
                fs.Flush(true);
                return true;
            }
            catch (Exception ex)
            {
                Debug("pending append: " + ex.Message);
                return false;
            }
        }

        private List<string> ReadPendingLinesLocked()
        {
            var result = new List<string>();
            if (!File.Exists(_pendingPath)) return result;
            using var fs = new FileStream(_pendingPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(fs, Encoding.UTF8);
            string? line;
            while ((line = sr.ReadLine()) != null)
            {
                if (line.Length == 0) continue;
                result.Add(line);
            }
            return result;
        }

        private HashSet<string> ReadPendingKeysLocked()
        {
            var set = new HashSet<string>(StringComparer.Ordinal);
            try
            {
                foreach (var l in ReadPendingLinesLocked())
                    set.Add(ExtractKey(l));
            }
            catch (Exception ex)
            {
                Debug("pending keys: " + ex.Message);
            }
            return set;
        }

        /// <summary>줄의 마지막 열(기록키). 기록키는 [A-Za-z0-9_-] 만 있으므로 마지막 콤마 뒤가 곧 키다.</summary>
        internal static string ExtractKey(string line)
        {
            string t = line.TrimEnd('\r', '\n');
            int i = t.LastIndexOf(',');
            return i < 0 ? t : t.Substring(i + 1);
        }

        /// <summary>pending 에서 첫 번째로 일치하는 줄 하나를 제거한다(임시파일 → 교체).</summary>
        private void RemovePendingLineLocked(string pendingLine)
        {
            var lines = ReadPendingLinesLocked();
            int idx = lines.IndexOf(pendingLine);
            if (idx < 0) return;
            lines.RemoveAt(idx);

            string tmp = _pendingPath + ".tmp";
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                foreach (var l in lines)
                {
                    byte[] b = Encoding.UTF8.GetBytes(l + "\r\n");
                    fs.Write(b, 0, b.Length);
                }
                fs.Flush(true);
            }
            File.Move(tmp, _pendingPath, overwrite: true);
        }

        // ==== 로컬 사본 Robot_YYYYMMDD_<prog>.csv ====

        private void TryAppendLocalCopy(DateTime requestDate, string line)
        {
            try
            {
                Directory.CreateDirectory(_localDir);
                AppendCsvLine(LocalCopyPath(requestDate), line);
            }
            catch (Exception ex)
            {
                Debug("local copy: " + ex.Message);
            }
        }

        /// <summary>
        /// 파일이 없으면 BOM+헤더를 먼저 쓰고, 이후에는 BOM 없이 append.
        /// 파일 끝이 개행으로 끝나지 않으면(반쪽 줄) 먼저 CRLF 를 써서 기존 줄과 섞이지 않게 한다.
        /// </summary>
        internal static void AppendCsvLine(string path, string line)
        {
            bool exists = File.Exists(path) && new FileInfo(path).Length > 0;
            bool needsNewline = exists && !EndsWithNewline(path);
            using var fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
            if (needsNewline)
            {
                fs.Write(new byte[] { 0x0D, 0x0A }, 0, 2);
            }
            if (!exists)
            {
                byte[] bom = Encoding.UTF8.GetPreamble();
                fs.Write(bom, 0, bom.Length);
                byte[] h = Encoding.UTF8.GetBytes(Header + "\r\n");
                fs.Write(h, 0, h.Length);
            }
            byte[] b = Encoding.UTF8.GetBytes(line + "\r\n");
            fs.Write(b, 0, b.Length);
            fs.Flush(true);
        }

        private static bool EndsWithNewline(string path)
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (fs.Length == 0) return true;
            fs.Seek(-1, SeekOrigin.End);
            return fs.ReadByte() == 0x0A;
        }

        // ==== 워커 ====

        private void WorkerLoop()
        {
            while (!_stop)
            {
                try
                {
                    ProcessRound();
                }
                catch (Exception ex)
                {
                    Debug("worker: " + ex.Message);
                }

                if (_stop) break;
                try { _wake.WaitOne(RetryIntervalMs); } catch { }
            }
        }

        /// <summary>
        /// 한 라운드: 메모리 큐 → pending 으로 옮기고, pending 을 순서대로 NAS 로 보낸다.
        /// 앞 줄이 실패하면 뒤 줄은 이번 라운드에서 보내지 않는다(순서 유지).
        /// </summary>
        private void ProcessRound()
        {
            // 1) pending append 에 실패했던 줄을 다시 pending 으로
            lock (_sync)
            {
                while (_memoryQueue.Count > 0)
                {
                    var item = _memoryQueue[0];
                    if (!TryAppendPendingLocked(item)) break;
                    _memoryQueue.RemoveAt(0);

                    if (_open.TryGetValue(item.Key, out var e))
                    {
                        TryAppendLocalCopy(e.ReceivedAt, item.Line);
                        _open.Remove(item.Key);
                        try { SaveOpenLocked(); } catch (Exception ex) { Debug("save open: " + ex.Message); }
                    }
                }
            }

            // 2) pending → NAS
            List<string> lines;
            lock (_sync) lines = ReadPendingLinesLocked();

            foreach (var pendingLine in lines)
            {
                if (_stop) return;

                int comma = pendingLine.IndexOf(',');
                if (comma < 0)
                {
                    // 형식이 깨진 줄: 보낼 수 없으니 버린다(디버그 기록)
                    Debug("drop malformed pending line: " + pendingLine);
                    lock (_sync) RemovePendingLineLocked(pendingLine);
                    continue;
                }

                string date = pendingLine.Substring(0, comma);
                string line = pendingLine.Substring(comma + 1);
                string key = ExtractKey(line);

                if (!TrySendToNas(date, line, key))
                    return;  // 이 줄은 pending 에 남기고 이번 라운드 종료

                lock (_sync) RemovePendingLineLocked(pendingLine);
            }
        }

        /// <summary>잠금 안에서 NAS 파일에 1줄 append. 이미 같은 기록키가 있으면 쓰지 않고 true.</summary>
        private bool TrySendToNas(string date, string line, string key)
        {
            FileStream? lockStream = null;
            string lockPath = LockPath;
            try
            {
                Directory.CreateDirectory(Path.Combine(_nasDir, LockDirName));

                lockStream = AcquireLock(lockPath);
                if (lockStream == null)
                {
                    Debug("lock busy: " + key);
                    return false;
                }

                string path = NasFilePath(date);
                if (File.Exists(path) && ContainsKey(path, key))
                    return true;   // 이미 기록됨

                AppendCsvLine(path, line);
                return true;
            }
            catch (Exception ex)
            {
                Debug("nas send: " + ex.Message);
                return false;
            }
            finally
            {
                if (lockStream != null)
                {
                    try { lockStream.Dispose(); } catch { }
                    try { File.Delete(lockPath); } catch (Exception ex) { Debug("lock delete: " + ex.Message); }
                }
            }
        }

        private static bool ContainsKey(string path, string key)
        {
            string suffix = "," + key;
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(fs, Encoding.UTF8);
            string? l;
            while ((l = sr.ReadLine()) != null)
            {
                if (l.TrimEnd('\r', '\n').EndsWith(suffix, StringComparison.Ordinal))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// _lock\Robot.lock 을 CreateNew 로 만든다.
        /// 이미 있으면 LastWriteTime 이 LockStaleSeconds 보다 오래됐을 때만 삭제 후 1회 재시도,
        /// 아니면 200ms 간격 최대 25회 시도. 실패 시 null.
        /// </summary>
        private FileStream? AcquireLock(string lockPath)
        {
            const int maxTries = 25;
            const int delayMs = 200;
            bool staleTried = false;

            for (int i = 0; i < maxTries; i++)
            {
                try
                {
                    var fs = new FileStream(lockPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                    try
                    {
                        string content = $"{_program} {Environment.ProcessId} {DateTime.Now:yyyy-MM-dd HH:mm:ss}";
                        byte[] b = Encoding.UTF8.GetBytes(content);
                        fs.Write(b, 0, b.Length);
                        fs.Flush(true);
                    }
                    catch
                    {
                        // 내용 쓰기 실패는 무시(잠금 자체는 성립)
                    }
                    return fs;
                }
                catch (IOException)
                {
                    if (!staleTried)
                    {
                        staleTried = true;
                        try
                        {
                            if (File.Exists(lockPath))
                            {
                                DateTime lw = File.GetLastWriteTime(lockPath);
                                if ((DateTime.Now - lw).TotalSeconds > LockStaleSeconds)
                                {
                                    File.Delete(lockPath);
                                    Debug("stale lock removed");
                                    continue;   // 1회 즉시 재시도
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Debug("stale check: " + ex.Message);
                        }
                    }
                    Thread.Sleep(delayMs);
                }
                catch (UnauthorizedAccessException)
                {
                    Thread.Sleep(delayMs);
                }
            }
            return null;
        }

        // ==== 디버그 ====

        private void Debug(string msg)
        {
            try
            {
                Directory.CreateDirectory(_localDir);
                File.AppendAllText(_debugPath,
                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{_program}] {msg}\r\n", Encoding.UTF8);
            }
            catch
            {
                // 디버그 기록 실패는 무시
            }
        }
    }
}
