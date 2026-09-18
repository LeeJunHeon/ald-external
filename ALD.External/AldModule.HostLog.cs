// AldModule.HostLog.cs
//
// AldModule 의 호스트 요청 공정 로그 훅(공통 사양 v3.1).
// - START_ALD / START_ALD_PREHEAT 요청만 기록한다 (GET_ALD_STATUS 등 폴링은 기록하지 않음).
// - 명령 처리 코드는 그대로 두고, 관측하고 기록만 한다.
// - 이 파일의 모든 진입점은 예외를 메인 프로그램/통신 루프로 내보내지 않는다.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace ALD.External
{
    public static partial class AldModule
    {
        // ==== HostProcessLog 인스턴스 ====

        private static readonly object _hostLogLock = new();
        private static HostProcessLog? _hostLog;
        private static string? _hostLogNasDir;
        private static string? _hostLogLocalDir;

        internal static HostProcessLog HostLog
        {
            get
            {
                lock (_hostLogLock)
                {
                    _hostLog ??= new HostProcessLog(_hostLogNasDir, _hostLogLocalDir, HostProcessLog.DefaultProgramName);
                    return _hostLog;
                }
            }
        }

        /// <summary>
        /// 호스트 로그 폴더를 바꾼다(선택, Init 전 호출; 테스트용). null/빈 문자열이면 기본값.
        /// </summary>
        public static void ConfigureHostLog(string? nasDir, string? localDir)
        {
            try
            {
                HostProcessLog? old;
                lock (_hostLogLock)
                {
                    _hostLogNasDir = nasDir;
                    _hostLogLocalDir = localDir;
                    old = _hostLog;
                    _hostLog = null;
                }
                old?.Close(TimeSpan.FromSeconds(2));
                CancelAllRunTracks();
            }
            catch
            {
                // 설정 실패는 무시(기본값 사용)
            }
        }

        // ==== 요청별 관측 상태 ====

        private sealed class RunTrack
        {
            public string Key = "";
            public AldRunObserver Observer = new();
            public CancellationTokenSource Cts = new();
            public bool StartRequested => Observer.StartRequested;
        }

        private static readonly object _trackLock = new();
        private static readonly Dictionary<string, RunTrack> _tracks = new(StringComparer.Ordinal);

        // 현재 처리 중인 요청의 기록키. HandleJson 호출 전에 설정하고 끝나면 지운다.
        // 예열 잡은 백그라운드 Task 에서 StartAldInternal 을 부르므로 job.HostLogKey 로 다시 세팅한다.
        private static readonly AsyncLocal<string?> _hostLogKey = new();

        private const int ObservePollMs = 500;

        // ==== 진입점: HandleClientAsync → HandleJsonWithHostLog → HandleJson ====

        /// <summary>
        /// 기존 HandleJson 을 그대로 호출하면서 START 계열 요청만 호스트 로그에 기록한다.
        /// HandleJson 이 던진 예외는 그대로 다시 던진다(기존 오류 응답 경로 유지).
        /// </summary>
        internal static string HandleJsonWithHostLog(string requestJson, DateTime recvTime, string remote)
        {
            string? key = null;
            List<string>? previousLive = null;
            try
            {
                if (TryExtractStartRequest(requestJson, out string requestId, out string recipeFile))
                {
                    previousLive = LiveOwnedKeys();
                    key = HostLog.Request("ALD", requestId, remote ?? "", recvTime, recipeName: recipeFile);
                    if (string.IsNullOrEmpty(key)) key = null;
                }
            }
            catch
            {
                key = null;
            }

            if (key == null)
                return HandleJson(requestJson);

            string respJson;
            _hostLogKey.Value = key;
            try
            {
                respJson = HandleJson(requestJson);
            }
            catch (Exception ex)
            {
                try
                {
                    if (!HostLog.IsOwned(key))
                        HostLog.Reject(key, ex.Message);
                }
                catch { }
                throw;
            }
            finally
            {
                _hostLogKey.Value = null;
            }

            try
            {
                AfterStartResponse(key, respJson, previousLive);
            }
            catch
            {
                // 기록 실패가 응답에 영향을 주면 안 된다
            }
            return respJson;
        }

        private static bool TryExtractStartRequest(string requestJson, out string requestId, out string recipeFile)
        {
            requestId = "";
            recipeFile = "";
            using var doc = JsonDocument.Parse(requestJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return false;
            if (!root.TryGetProperty("command", out var cmdEl)) return false;
            string command = cmdEl.ValueKind == JsonValueKind.String ? (cmdEl.GetString() ?? "") : "";
            if (command != "START_ALD" && command != "START_ALD_PREHEAT") return false;

            if (root.TryGetProperty("request_id", out var idEl))
            {
                requestId = idEl.ValueKind == JsonValueKind.String
                    ? (idEl.GetString() ?? "")
                    : idEl.GetRawText();
            }

            if (root.TryGetProperty("data", out var data) &&
                data.ValueKind == JsonValueKind.Object &&
                data.TryGetProperty("csv_path", out var csvEl) &&
                csvEl.ValueKind == JsonValueKind.String)
            {
                string p = csvEl.GetString() ?? "";
                try { recipeFile = Path.GetFileName(p.Trim()); }
                catch { recipeFile = p; }
            }
            return true;
        }

        private static void AfterStartResponse(string key, string respJson, List<string>? previousLive)
        {
            string result = "";
            string message = "";
            try
            {
                using var doc = JsonDocument.Parse(respJson);
                if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Object)
                {
                    if (data.TryGetProperty("result", out var r) && r.ValueKind == JsonValueKind.String)
                        result = r.GetString() ?? "";
                    if (data.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String)
                        message = m.GetString() ?? "";
                }
            }
            catch
            {
                result = "fail";
                message = "invalid response";
            }

            bool accepted = result == "success" || result == "accepted";

            if (!accepted)
            {
                if (!HostLog.IsOwned(key))
                    HostLog.Reject(key, message);
                return;
            }

            // 새 요청이 수락됐는데 이전 요청의 종료를 아직 관측하지 못했다면 미확인으로 닫는다.
            if (previousLive != null)
            {
                foreach (var old in previousLive)
                {
                    if (old == key) continue;
                    RemoveTrack(old, cancel: true);
                    HostLog.Finalize(old, "미확인", "다음 요청 수락 전 종료 미관측", DateTime.Now);
                }
            }
        }

        private static List<string> LiveOwnedKeys()
        {
            lock (_trackLock) return _tracks.Keys.ToList();
        }

        // ==== 명령 처리 코드 안의 최소 훅 ====

        /// <summary>LoadRecipeCsv / LoadPreheatCsv 성공 직후: 행개수·공정명 갱신</summary>
        private static void HostLogUpdateRecipe(List<AldRecipeRow>? rows)
        {
            try
            {
                string? key = _hostLogKey.Value;
                if (string.IsNullOrEmpty(key) || rows == null) return;

                string names = string.Join(" / ", rows.Select(r => (r.ProcessName ?? "").Trim()));
                HostLog.Update(key, rowCount: rows.Count.ToString(), processNames: names);
            }
            catch { }
        }

        /// <summary>StartAldInternal 에서 _startRecipeCallback 이 예외 없이 돌아온 직후</summary>
        private static void HostLogOnStartCommandSent()
        {
            try
            {
                string? key = _hostLogKey.Value;
                if (string.IsNullOrEmpty(key)) return;

                HostLog.MarkOwned(key);

                RunTrack track;
                lock (_trackLock)
                {
                    if (!_tracks.TryGetValue(key, out track!))
                    {
                        track = new RunTrack { Key = key };
                        _tracks[key] = track;
                    }
                }
                track.Observer.RequestStart(DateTime.Now);
                _ = Task.Run(() => ObserveRunAsync(track));
            }
            catch { }
        }

        /// <summary>HandleStartAldPreheat 에서 _preheatJob = job 직후</summary>
        private static void HostLogOnPreheatAccepted(PreheatJob job)
        {
            try
            {
                string? key = _hostLogKey.Value;
                if (string.IsNullOrEmpty(key) || job == null) return;

                job.HostLogKey = key;
                HostLog.MarkOwned(key);
                lock (_trackLock)
                {
                    if (!_tracks.ContainsKey(key))
                        _tracks[key] = new RunTrack { Key = key };
                }
            }
            catch { }
        }

        /// <summary>RunPreheatSequenceAsync 가 StartAldInternal 을 부르기 직전: 현재 키를 잡 것으로</summary>
        private static void HostLogSetCurrentKey(string? key)
        {
            try { _hostLogKey.Value = key; } catch { }
        }

        /// <summary>RunPreheatSequenceAsync 의 finally: START 전에 끝난 예열 잡을 닫는다.</summary>
        private static void HostLogOnPreheatFinished(PreheatJob job)
        {
            try
            {
                string? key = job?.HostLogKey;
                if (string.IsNullOrEmpty(key)) return;

                bool startRequested;
                lock (_trackLock)
                    startRequested = _tracks.TryGetValue(key!, out var t) && t.StartRequested;

                if (startRequested)
                    return;   // 관측 태스크가 종료를 기록한다

                string phase, message, error;
                lock (_preheatLock)
                {
                    phase = job!.Phase;
                    message = job.Message;
                    error = job.ErrorMessage;
                }

                RemoveTrack(key!, cancel: true);
                DateTime now = DateTime.Now;
                if (phase == "CANCELLED")
                    HostLog.Finalize(key!, "STOP", message, now);
                else if (phase == "FAIL")
                    HostLog.Finalize(key!, "실패", error, now);
                else
                    HostLog.Finalize(key!, "미확인", "예열 종료 상태=" + phase, now);
            }
            catch { }
        }

        /// <summary>
        /// 메인 프로그램이 공정 종료를 직접 알려줄 수 있는 공개 API.
        /// 호출되면 관측 판정보다 우선해 즉시 기록한다(예: "성공", "실패", "STOP").
        /// </summary>
        public static void NotifyProcessFinished(string result, string reason)
        {
            try
            {
                List<RunTrack> live;
                lock (_trackLock)
                {
                    live = _tracks.Values.Where(t => t.StartRequested).ToList();
                    foreach (var t in live) _tracks.Remove(t.Key);
                }
                DateTime now = DateTime.Now;
                foreach (var t in live)
                {
                    try { t.Cts.Cancel(); } catch { }
                    HostLog.Finalize(t.Key, result ?? "미확인", reason ?? "", now);
                }
            }
            catch { }
        }

        // ==== 관측 태스크 ====

        private static async Task ObserveRunAsync(RunTrack track)
        {
            try
            {
                var ct = track.Cts.Token;
                var obs = track.Observer;

                while (!ct.IsCancellationRequested)
                {
                    bool? processBool = TryReadStaticBool("ALD.Utility", "Process_Bool");
                    int? state = TryReadStaticInt("ALD.Utility", "State");
                    bool? processStop = TryReadStaticBool("ALD.Utility", "ProcessStop");
                    int? idx = TryReadProcessIdx();

                    DateTime now = DateTime.Now;
                    var ev = obs.Feed(now, processBool, state, processStop, idx);

                    if (ev == AldRunEvent.Started)
                    {
                        HostLog.MarkStarted(track.Key, obs.StartedAt ?? now);
                    }
                    else if (ev == AldRunEvent.Finished || ev == AldRunEvent.StartTimeout)
                    {
                        RemoveTrack(track.Key, cancel: false);
                        HostLog.Finalize(track.Key, obs.Result, obs.Reason,
                            ev == AldRunEvent.Finished ? obs.FinishedAt : null);
                        return;
                    }

                    try { await Task.Delay(ObservePollMs, ct); }
                    catch (OperationCanceledException) { return; }
                }
            }
            catch
            {
                // 관측 실패는 조용히 끝낸다(open 항목은 재시작 시 "중단(재시작)" 으로 정리됨)
            }
        }

        private static void RemoveTrack(string key, bool cancel)
        {
            RunTrack? t;
            lock (_trackLock)
            {
                if (_tracks.TryGetValue(key, out t)) _tracks.Remove(key);
            }
            if (cancel && t != null)
            {
                try { t.Cts.Cancel(); } catch { }
            }
        }

        private static void CancelAllRunTracks()
        {
            List<RunTrack> all;
            lock (_trackLock)
            {
                all = _tracks.Values.ToList();
                _tracks.Clear();
            }
            foreach (var t in all)
            {
                try { t.Cts.Cancel(); } catch { }
            }
        }

        /// <summary>Init() 마지막에 호출</summary>
        private static void HostLogStartup()
        {
            try { HostLog.StartupRecover(); } catch { }
        }

        /// <summary>StopServer() 마지막에 호출</summary>
        private static void HostLogShutdown()
        {
            try
            {
                CancelAllRunTracks();
                HostProcessLog? log;
                lock (_hostLogLock) log = _hostLog;
                log?.Close(TimeSpan.FromSeconds(2));
            }
            catch { }
        }

        // ==== 리플렉션 읽기(실패는 null) ====

        private static bool? TryReadStaticBool(string typeName, string fieldName)
        {
            try { return ReadRequiredAldStaticField<bool>(typeName, fieldName); }
            catch { return null; }
        }

        private static int? TryReadStaticInt(string typeName, string fieldName)
        {
            try { return ReadRequiredAldStaticField<int>(typeName, fieldName); }
            catch { return null; }
        }

        /// <summary>ALD.Thread_Process._instance.Process_Idx (_instance 가 null 이면 null)</summary>
        private static int? TryReadProcessIdx()
        {
            try
            {
                var processType = FindTypeInCurrentAppDomain("ALD.Thread_Process");
                if (processType == null) return null;

                const BindingFlags staticFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;
                const BindingFlags instanceFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

                object? instance = processType.GetField("_instance", staticFlags)?.GetValue(null);
                if (instance == null) return null;

                var f = processType.GetField("Process_Idx", instanceFlags);
                if (f == null) return null;

                object? v = f.GetValue(instance);
                if (v is int i) return i;
                if (v != null && int.TryParse(v.ToString(), out int j)) return j;
                return null;
            }
            catch
            {
                return null;
            }
        }

        // ==== 통신 CSV(comm_YYYYMMDD.csv) 백그라운드 큐 ====
        // 파일·형식·내용은 그대로. 실제 파일 쓰기만 단일 백그라운드 스레드로 옮겨
        // NAS 지연이 클라이언트 처리 루프를 막지 않게 한다. 줄 순서는 큐 순서로 유지된다.

        private sealed record CommLogItem(DateTime RecvTime, DateTime SendTime, string Remote, string ReqJson, string RespJson);

        private static readonly ConcurrentQueue<CommLogItem> _commLogQueue = new();
        private static readonly AutoResetEvent _commLogWake = new(false);
        private static Thread? _commLogThread;
        private static readonly object _commLogThreadLock = new();

        private static void LogComm(DateTime recvTime, DateTime sendTime, string remote, string reqJson, string respJson)
        {
            try
            {
                _commLogQueue.Enqueue(new CommLogItem(recvTime, sendTime, remote ?? "", reqJson ?? "", respJson ?? ""));
                EnsureCommLogThread();
                _commLogWake.Set();
            }
            catch
            {
                // 로그 큐 실패해도 통신은 계속 진행
            }
        }

        private static void EnsureCommLogThread()
        {
            lock (_commLogThreadLock)
            {
                if (_commLogThread != null && _commLogThread.IsAlive) return;
                _commLogThread = new Thread(CommLogLoop)
                {
                    IsBackground = true,
                    Name = "AldModule-CommLog"
                };
                _commLogThread.Start();
            }
        }

        private static void CommLogLoop()
        {
            while (true)
            {
                try
                {
                    while (_commLogQueue.TryDequeue(out var item))
                        LogCommWrite(item.RecvTime, item.SendTime, item.Remote, item.ReqJson, item.RespJson);
                    _commLogWake.WaitOne(1000);
                }
                catch
                {
                    // 큐 처리 실패는 무시
                }
            }
        }
    }
}
