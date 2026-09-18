// AldModule.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;

namespace ALD.External
{
    // === 최상위 데이터 구조들 ===

    /// <summary>
    /// ALD_Recipe.csv 한 줄
    /// </summary>
    public class AldRecipeRow
    {
        public string ProcessName { get; set; } = "";  // "공정이름"
        public string RecipeName { get; set; } = "";   // "레시피"
        public int Distance { get; set; }              // "Distance"
        //public bool Is4Inch { get; set; }            // "4inch" (사용 시 활성화)
    }

    /// <summary>
    /// START_ALD_PREHEAT CSV의 PREHEAT 한 줄
    /// - SV_NO: ALD 메인 프로그램의 Heater SV 번호, 1-based
    /// - SV: 설정할 목표 온도
    /// - CtcNo: SV_NO에서 자동 매핑된 C-TC 번호, 1-based
    /// - TOLERANCE: 목표 온도 허용 오차
    /// </summary>
    public class PreheatStep
    {
        public int SvNo { get; set; }
        public double Sv { get; set; }
        public int CtcNo { get; set; }
        public double Tolerance { get; set; } = 1.0;

        // GET_ALD_STATUS에서 client에게 현재 진행 상태를 보여주기 위한 값
        public double CurrentCtc { get; set; }
        public bool Reached { get; set; }
    }

    /// <summary>
    /// START_ALD_PREHEAT CSV 전체 파싱 결과
    /// - PreheatSteps: 먼저 설정하고 감시할 히터 목록
    /// - Recipes: 온도 도달 및 soak 이후 실제 START_ALD에 넘길 기존 recipe row들
    /// </summary>
    public class PreheatRecipeFile
    {
        public List<PreheatStep> PreheatSteps { get; set; } = new();
        public List<AldRecipeRow> Recipes { get; set; } = new();

        // 기본값: CSV에 Mode가 없으면 Full Auto
        public string RunMode { get; set; } = "full_auto";

        // 기본값: 도달 후 10분 대기
        public int SoakSec { get; set; } = 600;

        // 기본값: 5초마다 C-TC 확인
        public int PollSec { get; set; } = 5;

        // 기본값: 3번 연속 만족해야 도달로 인정
        public int StableCount { get; set; } = 3;

        // 안에 목표 온도 도달하지 못하면 실패
        public int HeatTimeoutSec { get; set; } = 3600;
    }

    /// <summary>
    /// DLL 내부에서 background로 돌아가는 preheat job 상태
    /// GET_ALD_STATUS overlay에서 이 객체를 보고 running/HEATING/SOAKING 등을 반환한다.
    /// </summary>
    public class PreheatJob
    {
        public string JobId { get; set; } = "";
        public string CsvPath { get; set; } = "";

        // 호스트 요청 공정 로그 기록키 (AldModule.HostLog.cs)
        internal string? HostLogKey { get; set; }

        public string RunMode { get; set; } = "full_auto";
        public string Phase { get; set; } = "IDLE";
        public string Message { get; set; } = "";
        public string ErrorMessage { get; set; } = "";

        public DateTime StartedAt { get; set; } = DateTime.Now;
        public DateTime PhaseStartedAt { get; set; } = DateTime.Now;

        public List<PreheatStep> Steps { get; set; } = new();
        public List<AldRecipeRow> Recipes { get; set; } = new();

        public int SoakSec { get; set; } = 600;
        public int PollSec { get; set; } = 5;
        public int StableCount { get; set; } = 3;
        public int HeatTimeoutSec { get; set; } = 3600;
        public int SoakRemainingSec { get; set; } = 0;
    }

    /// <summary>
    /// SET_HEATER_SV 로직을 기존 handler와 START_ALD_PREHEAT에서 같이 쓰기 위한 내부 결과 구조
    /// </summary>
    internal sealed class HeaterSvWriteResult
    {
        public bool ModbusReady { get; set; }
        public int SvNo { get; set; }
        public int Index { get; set; }
        public double Value { get; set; }
        public ushort Raw { get; set; }
    }

    /// <summary>
    /// GET_TEMPERATURE 로직을 기존 handler와 START_ALD_PREHEAT에서 같이 쓰기 위한 내부 결과 구조
    /// </summary>
    internal sealed class TemperatureReadResult
    {
        public bool ModbusReady { get; set; }
        public int TcType { get; set; }
        public string TcName { get; set; } = "";
        public int TcNo { get; set; }
        public int Index { get; set; }
        public int ArrayLength { get; set; }
        public ushort Raw { get; set; }
        public double Value { get; set; }
    }

    /// <summary>
    /// ALD 메인 프로그램에서 리플렉션으로 읽은 공정 시간 정보
    /// </summary>
    internal sealed class AldTimingSnapshot
    {
        public bool ProcessRunning { get; set; }
        public bool WorkerRunning { get; set; }
        public bool ProcessTimeStarted { get; set; }

        public int ProcessIndex { get; set; }

        public double TotalSeconds { get; set; }
        public DateTime StartTime { get; set; }
    }

    /// <summary>
    /// ALD 상태 구조
    /// </summary>
    public class AldStatus
    {
        public string State { get; set; } = "idle";
        public bool Vacuum { get; set; }
    }

    /// <summary>
    /// ALD 외부 통신 모듈
    /// - TCP 서버를 열어 START_ALD / GET_ALD_STATUS 명령을 처리한다.
    /// - START_ALD: CSV 경로를 받아 레시피를 파싱한 뒤 콜백으로 전달
    /// - GET_ALD_STATUS: ALD 상태 콜백을 조회해 JSON으로 반환
    /// </summary>
    public static partial class AldModule
    {
        // ==== 통신 설정 ====

        private const int DefaultPort = 7000;
        private static int _port = DefaultPort;
        public static int Port => _port;

        // ==== 통신 CSV 로그 (NAS) ====
        // 저장 위치(고정): \\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Rayvac_ALD
        private static readonly string COMM_LOG_DIR = @"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Rayvac_ALD";
        private static readonly object COMM_LOG_LOCK = new object();

        private static string CommLogPath(DateTime t)
            => Path.Combine(COMM_LOG_DIR, $"comm_{t:yyyyMMdd}.csv");

        private static string Csv(string s)
        {
            s ??= "";
            s = s.Replace("\r\n", "\\n").Replace("\n", "\\n").Replace("\r", "\\n");
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }

        // 실제 파일 쓰기. 호출은 AldModule.HostLog.cs 의 LogComm(백그라운드 큐)에서만 한다.
        private static void LogCommWrite(DateTime recvTime, DateTime sendTime, string remote, string reqJson, string respJson)
        {
            try
            {
                Directory.CreateDirectory(COMM_LOG_DIR);
                string path = CommLogPath(recvTime);

                lock (COMM_LOG_LOCK)
                {
                    bool needHeader = !File.Exists(path);

                    using (var sw = new StreamWriter(path, true, Encoding.UTF8))
                    {
                        if (needHeader)
                            sw.WriteLine("recv_time,send_time,remote,req_json,resp_json");

                        sw.WriteLine(string.Join(",",
                            Csv(recvTime.ToString("yyyy-MM-dd HH:mm:ss.fff")),
                            Csv(sendTime.ToString("yyyy-MM-dd HH:mm:ss.fff")),
                            Csv(remote ?? ""),
                            Csv(reqJson ?? ""),
                            Csv(respJson ?? "")
                        ));
                    }
                }
            }
            catch
            {
                // NAS 문제로 로그 실패해도 통신은 계속 진행
            }
        }

        /// <summary>
        /// ALD 메인 프로그램(같은 프로세스)에 존재하는 Utility.State 를 읽어
        /// "알람이 현재 떠있는지" 여부만 반환한다.
        /// - ALD 코드 기준: Utility.State == -1 이면 알람창이 떠있는 상태
        /// </summary>
        private static bool GetAlarmOnFlag()
        {
            try
            {
                // 같은 프로세스에 로드된 어셈블리에서 "ALD.Utility" 타입을 찾는다.
                Type? utilityType = null;
                foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
                {
                    utilityType = asm.GetType("ALD.Utility", throwOnError: false, ignoreCase: false);
                    if (utilityType != null) break;
                }
                if (utilityType == null) return false;

                // public static int State
                var stateField = utilityType.GetField(
                    "State",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (stateField == null) return false;

                object? val = stateField.GetValue(null);
                if (val is int i) return i == -1;

                if (val != null && int.TryParse(val.ToString(), out int j))
                    return j == -1;

                return false;
            }
            catch
            {
                // 읽기 실패해도 통신은 계속 진행
                return false;
            }
        }


        // ===== Gate 상태(Loadlock <-> Chamber) 읽기: Valve_Comm.plcData[19]/[20] =====

        private static Type? FindTypeInCurrentAppDomain(string fullTypeName)
        {
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                var t = asm.GetType(fullTypeName, throwOnError: false, ignoreCase: false);
                if (t != null) return t;
            }
            return null;
        }


        /// <summary>
        /// 외부 공정 시작 전에 메인 프로그램의 선택 레시피 이름을
        /// 실제 실행할 레시피 이름으로 맞춘다.
        /// UI 컨트롤을 직접 조작하지 않고 문자열 필드만 변경한다.
        /// </summary>
        private static void SetSelectedRecipeForExternalStart(string recipeName)
        {
            if (string.IsNullOrWhiteSpace(recipeName))
            {
                throw new InvalidOperationException(
                    "Cannot update log recipe name: RecipeName is empty.");
            }

            var utilityType = FindTypeInCurrentAppDomain("ALD.Utility");

            if (utilityType == null)
            {
                throw new InvalidOperationException(
                    "Cannot update log recipe name: ALD.Utility not found.");
            }

            var field = utilityType.GetField(
                "SelectedRecipe",
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Static);

            if (field == null ||
                field.FieldType != typeof(string) ||
                field.IsInitOnly ||
                field.IsLiteral)
            {
                throw new InvalidOperationException(
                    "Cannot update log recipe name: " +
                    "ALD.Utility.SelectedRecipe is missing or not writable.");
            }

            field.SetValue(null, recipeName.Trim());
        }

        private static bool TryGetPlcDataOpen(int index, out bool open)
        {
            open = false;
            try
            {
                var valveCommType = FindTypeInCurrentAppDomain("ALD.Valve_Comm");
                if (valveCommType == null) return false;

                var plcDataField = valveCommType.GetField(
                    "plcData",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (plcDataField?.GetValue(null) is not Array arr) return false;
                if (index < 0 || index >= arr.Length) return false;

                var item = arr.GetValue(index);
                if (item == null) return false;

                var openProp = item.GetType().GetProperty(
                    "Open",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

                if (openProp == null) return false;

                var val = openProp.GetValue(item);
                if (val is bool b) { open = b; return true; }
                if (val != null && bool.TryParse(val.ToString(), out var bb)) { open = bb; return true; }

                return false;
            }
            catch
            {
                return false;
            }
        }

        private static bool TryGetGateStatus(out bool gateOpenRaw, out bool gateCloseRaw)
        {
            gateOpenRaw = gateCloseRaw = false;
            bool ok1 = TryGetPlcDataOpen(19, out gateOpenRaw); // Gate Open LS
            bool ok2 = TryGetPlcDataOpen(20, out gateCloseRaw); // Gate Close LS
            return ok1 && ok2;
        }


        /// <summary>
        /// ALD.Utility.ModbusCheck (Modbus 통신 시작 여부)
        /// </summary>
        private static bool GetModbusCheckFlag()
        {
            try
            {
                Type? utilityType = null;
                foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
                {
                    utilityType = asm.GetType("ALD.Utility", throwOnError: false, ignoreCase: false);
                    if (utilityType != null) break;
                }
                if (utilityType == null) return false;

                var f = utilityType.GetField("ModbusCheck",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
                if (f == null) return false;

                object? val = f.GetValue(null);
                if (val is bool b) return b;

                if (val != null && bool.TryParse(val.ToString(), out bool bb))
                    return bb;

                return false;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// ALD.Analog_Comm.pressureGauge[2] (Loadlock) 값을 읽어 Torr로 변환한다.
        /// </summary>
        private static bool TryGetLoadlockVacuum(out int raw, out double torr, out string error)
        {
            raw = 0;
            torr = 0;
            error = "";

            try
            {
                // 같은 프로세스에 로드된 어셈블리에서 "ALD.Analog_Comm" 타입을 찾는다.
                Type? analogType = null;
                foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
                {
                    analogType = asm.GetType("ALD.Analog_Comm", throwOnError: false, ignoreCase: false);
                    if (analogType != null) break;
                }
                if (analogType == null)
                {
                    error = "ALD.Analog_Comm type not found (ALD main assembly not loaded)";
                    return false;
                }

                var gaugeField = analogType.GetField("pressureGauge",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (gaugeField == null)
                {
                    error = "ALD.Analog_Comm.pressureGauge field not found";
                    return false;
                }

                object? val = gaugeField.GetValue(null);

                if (val is int[] gauges)
                {
                    if (gauges.Length <= 2)
                    {
                        error = "pressureGauge length < 3";
                        return false;
                    }
                    raw = gauges[2]; // ✅ Loadlock
                }
                else if (val is Array arr)
                {
                    if (arr.Length <= 2)
                    {
                        error = "pressureGauge length < 3";
                        return false;
                    }
                    raw = Convert.ToInt32(arr.GetValue(2)); // ✅ Loadlock
                }
                else
                {
                    error = "pressureGauge has unexpected type";
                    return false;
                }

                // ALD UI에서 쓰는 변환식과 동일
                torr = Math.Pow(10.0, ((raw / 1000.0) - 6.304) / 1.286);
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
            }
        }

        private const byte ProtocolVersion = 1;
        private const int HeaderSize = 16;                // !BBHIQ = 1 + 1 + 2 + 4 + 8

        // ALD 메인 프로그램에서 넘겨주는 콜백
        // mode: "process_only" 또는 "full_auto"
        private static Action<List<AldRecipeRow>, string>? _startRecipeCallback;
        private static Func<AldStatus>? _getStatusCallback;

        // 내부 상태
        private static readonly object _lock = new();
        private static List<AldRecipeRow> _recipes = new();

        // START_ALD_PREHEAT background job 상태
        // - preheat 중인지 확인
        // - GET_ALD_STATUS에서 virtual running overlay를 만들 때 사용
        // - 중복 START_ALD_PREHEAT 방지에도 사용
        private static readonly object _preheatLock = new();
        private static PreheatJob? _preheatJob;
        private static CancellationTokenSource? _preheatCts;

        private static TcpListener? _listener;
        private static CancellationTokenSource? _cts;

        // ==== 초기화 / 서버 제어 ====

        /// <summary>
        /// ALD 메인 프로그램에서 레시피 시작 콜백, 상태 조회 콜백을 등록한다.
        /// </summary>
        public static void Init(
            Action<List<AldRecipeRow>, string> startRecipeCallback,
            Func<AldStatus> getStatusCallback)
        {
            lock (_lock)
            {
                _startRecipeCallback = startRecipeCallback
                    ?? throw new ArgumentNullException(nameof(startRecipeCallback));
                _getStatusCallback = getStatusCallback
                    ?? throw new ArgumentNullException(nameof(getStatusCallback));

                _recipes = new List<AldRecipeRow>();
            }

            // 호스트 요청 공정 로그: 미종료 항목 정리 + 워커 시작 (AldModule.HostLog.cs)
            HostLogStartup();
        }

        /*
        // 나중에 main program에 process mode 기능이 추가되면 이 형태로 복구
        public static void Init(
            Action<List<AldRecipeRow>, string> startRecipeCallback,
            Func<AldStatus> getStatusCallback)
        {
            lock (_lock)
            {
                _startRecipeCallback = startRecipeCallback
                    ?? throw new ArgumentNullException(nameof(startRecipeCallback));
                _getStatusCallback = getStatusCallback
                    ?? throw new ArgumentNullException(nameof(getStatusCallback));

                _recipes = new List<AldRecipeRow>();
            }
        }
        */

        /// <summary>
        /// TCP 서버를 시작한다. (포트를 지정하지 않으면 기본 포트 사용)
        /// </summary>
        public static void StartServer(int port = 0)
        {
            if (_listener != null) return;

            if (port <= 0)
                port = DefaultPort;

            _port = port;

            _cts = new CancellationTokenSource();

            // 0.0.0.0 (모든 NIC)에서 수신
            _listener = new TcpListener(IPAddress.Any, _port);
            _listener.Start();

            _ = Task.Run(() => AcceptLoopAsync(_cts.Token));
        }

        /// <summary>
        /// TCP 서버를 중지한다.
        /// </summary>
        public static void StopServer()
        {
            _cts?.Cancel();

            // START_ALD_PREHEAT background job도 같이 중지
            lock (_preheatLock)
            {
                _preheatCts?.Cancel();
            }

            _listener?.Stop();
            _listener = null;

            // 호스트 요청 공정 로그: 관측 태스크 취소 + 워커 종료(최대 2초)
            HostLogShutdown();
        }

        // ==== CSV 파싱 ====

        private static string NormalizeRunMode(string? modeText)
        {
            string mode = (modeText ?? "")
                .Trim()
                .ToLowerInvariant()
                .Replace("-", "_")
                .Replace(" ", "_");

            // CSV에 없거나 빈 값이면 기본 Full Auto
            if (string.IsNullOrWhiteSpace(mode))
                return "full_auto";

            if (mode == "auto" || mode == "full" || mode == "full_auto")
                return "full_auto";

            if (mode == "process" || mode == "process_only" || mode == "only")
                return "process_only";

            throw new InvalidOperationException(
                $"Invalid Mode in CSV: '{modeText}'. Expected blank, full_auto, auto, process_only, or Process Only.");
        }


        /// <summary>
        /// ALD 레시피 CSV를 읽어 List&lt;AldRecipeRow&gt;로 변환한다.
        /// 헤더: 공정이름,레시피,Distance,(4inch)
        /// </summary>
        private static List<AldRecipeRow> LoadRecipeCsv(string path, out string mode)
        {
            var list = new List<AldRecipeRow>();

            // 기본값: CSV에 Mode가 없으면 Full Auto
            mode = "full_auto";
            bool modeSpecified = false;

            if (!File.Exists(path))
                throw new FileNotFoundException("ALD_Recipe.csv not found.", path);

            using var sr = new StreamReader(path, Encoding.UTF8);

            string? header = sr.ReadLine();
            if (header == null)
                throw new InvalidOperationException("ALD_Recipe.csv is empty.");

            header = header.TrimStart('\ufeff'); // UTF-8 BOM 제거

            var headers = header.Split(',');

            for (int i = 0; i < headers.Length; i++)
                headers[i] = headers[i].Trim();

            int idxProcess = Array.IndexOf(headers, "공정이름");
            int idxRecipe = Array.IndexOf(headers, "레시피");
            int idxDistance = Array.IndexOf(headers, "Distance");

            // 선택 컬럼: 없으면 full_auto
            int idxMode = Array.FindIndex(headers, h =>
                string.Equals(h, "Mode", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(h, "RunMode", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(h, "ProcessMode", StringComparison.OrdinalIgnoreCase));

            if (idxProcess < 0 || idxRecipe < 0 || idxDistance < 0)
                throw new InvalidOperationException("ALD_Recipe.csv header mismatch.");

            while (!sr.EndOfStream)
            {
                var line = sr.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;

                var cols = line.Split(',');

                int requiredMaxIndex = Math.Max(idxProcess, Math.Max(idxRecipe, idxDistance));
                if (cols.Length <= requiredMaxIndex)
                {
                    throw new InvalidOperationException(
                        $"ALD_Recipe.csv column count mismatch. cols={cols.Length}, required_index={requiredMaxIndex}, line='{line}'");
                }

                // 타입 설명 행(예: "(string)/(int)")은 건너뜀
                if (cols[idxProcess].Contains("string", StringComparison.OrdinalIgnoreCase))
                    continue;

                string processName = cols[idxProcess].Trim();
                string recipeName = cols[idxRecipe].Trim();
                string distanceText = cols[idxDistance].Trim();

                if (string.IsNullOrWhiteSpace(processName))
                    throw new InvalidOperationException($"공정이름 is empty. line='{line}'");

                if (string.IsNullOrWhiteSpace(recipeName))
                    throw new InvalidOperationException($"레시피 is empty. line='{line}'");

                if (!int.TryParse(distanceText, out int distance))
                    throw new InvalidOperationException($"Distance must be integer. value='{distanceText}', line='{line}'");

                // Mode 컬럼이 있고, 해당 row에 값이 있으면 mode로 사용
                if (idxMode >= 0 && idxMode < cols.Length)
                {
                    string rawMode = cols[idxMode].Trim();

                    if (!string.IsNullOrWhiteSpace(rawMode))
                    {
                        string normalizedMode = NormalizeRunMode(rawMode);

                        if (!modeSpecified)
                        {
                            mode = normalizedMode;
                            modeSpecified = true;
                        }
                        else if (mode != normalizedMode)
                        {
                            throw new InvalidOperationException(
                                $"Mode value is inconsistent in CSV. previous='{mode}', current='{normalizedMode}', line='{line}'");
                        }
                    }
                }

                var row = new AldRecipeRow
                {
                    ProcessName = processName,
                    RecipeName = recipeName,
                    Distance = distance,
                };

                list.Add(row);
            }

            if (list.Count == 0)
                throw new InvalidOperationException("ALD_Recipe.csv has no recipe rows.");

            return list;
        }

        /// <summary>
        /// START_ALD_PREHEAT용 CSV를 읽는다.
        /// CSV 안에는 PREHEAT row와 RECIPE row가 같이 들어간다.
        /// </summary>
        private static PreheatRecipeFile LoadPreheatCsv(string path)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("START_ALD_PREHEAT csv not found.", path);

            using var sr = new StreamReader(path, Encoding.UTF8);

            string? header = sr.ReadLine();
            if (header == null)
                throw new InvalidOperationException("START_ALD_PREHEAT csv is empty.");

            header = header.TrimStart('\ufeff');
            var headers = SplitCsvLine(header);

            int idxSection = FindHeader(headers, "Section");
            int idxKey = FindHeader(headers, "Key");
            int idxValue = FindHeader(headers, "Value");

            int idxSvNo = FindHeader(headers, "SV_NO");
            int idxSv = FindHeader(headers, "SV");

            // C-TC 번호는 CSV에서 입력받지 않는다.
            // SV_NO를 기준으로 내부에서 자동 매핑한다.
            int idxTolerance = FindHeader(headers, "TOLERANCE");

            int idxProcess = FindHeader(headers, "공정이름");
            int idxRecipe = FindHeader(headers, "레시피");
            int idxDistance = FindHeader(headers, "Distance");

            int idxMode = FindHeader(headers, "Mode");
            if (idxMode < 0) idxMode = FindHeader(headers, "RunMode");
            if (idxMode < 0) idxMode = FindHeader(headers, "ProcessMode");

            if (idxSection < 0)
                throw new InvalidOperationException("START_ALD_PREHEAT csv header mismatch: Section column not found.");

            if (idxSvNo < 0 || idxSv < 0)
                throw new InvalidOperationException(
                    "START_ALD_PREHEAT csv header mismatch: SV_NO, SV columns are required. C-TC number is mapped automatically from SV_NO.");

            if (idxProcess < 0 || idxRecipe < 0 || idxDistance < 0)
                throw new InvalidOperationException("START_ALD_PREHEAT csv header mismatch: 공정이름, 레시피, Distance columns are required.");

            var result = new PreheatRecipeFile();
            bool modeSpecified = false;

            while (!sr.EndOfStream)
            {
                string? line = sr.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;

                var cols = SplitCsvLine(line);
                string section = GetCol(cols, idxSection).Trim().ToUpperInvariant();
                if (string.IsNullOrWhiteSpace(section)) continue;

                // 타입 설명 행 같은 것은 무시
                if (section.Contains("STRING", StringComparison.OrdinalIgnoreCase) || section.StartsWith("("))
                    continue;

                if (section == "CONFIG")
                {
                    if (idxKey < 0 || idxValue < 0)
                        throw new InvalidOperationException("START_ALD_PREHEAT csv header mismatch: CONFIG rows require Key, Value columns.");

                    string key = GetCol(cols, idxKey).Trim().ToUpperInvariant();
                    string valueText = GetCol(cols, idxValue).Trim();

                    if (string.IsNullOrWhiteSpace(key))
                        continue;

                    if (!int.TryParse(valueText, out int intValue) || intValue <= 0)
                        throw new InvalidOperationException($"CONFIG {key} must be positive integer. value='{valueText}'");

                    switch (key)
                    {
                        case "SOAK_SEC":
                            result.SoakSec = intValue;
                            break;

                        case "POLL_SEC":
                            result.PollSec = intValue;
                            break;

                        case "STABLE_COUNT":
                            result.StableCount = intValue;
                            break;

                        case "HEAT_TIMEOUT_SEC":
                            result.HeatTimeoutSec = intValue;
                            break;

                        default:
                            throw new InvalidOperationException($"Unknown CONFIG key: {key}");
                    }
                }
                else if (section == "PREHEAT")
                {
                    int svNo = ParseRequiredInt(cols, idxSvNo, "SV_NO");
                    double sv = ParseRequiredDouble(cols, idxSv, "SV");
                    int mappedCtcNo = MapSvNoToCtcNo(svNo);

                    var step = new PreheatStep
                    {
                        SvNo = svNo,
                        Sv = sv,
                        CtcNo = mappedCtcNo,
                        Tolerance = ParseOptionalDouble(cols, idxTolerance, 1.0)
                    };

                    if (step.SvNo < 1 || step.SvNo > 12)
                        throw new InvalidOperationException($"SV_NO out of range: {step.SvNo}. valid range: 1~12");

                    if (step.Sv < 0 || step.Sv > 400)
                        throw new InvalidOperationException($"SV out of range: {step.Sv}. valid range: 0~400 degC");

                    if (step.CtcNo < 1 || step.CtcNo > 12)
                        throw new InvalidOperationException($"Mapped C-TC number out of range: {step.CtcNo}. valid range: 1~12");

                    if (step.Tolerance < 0)
                        throw new InvalidOperationException("TOLERANCE must be >= 0");

                    result.PreheatSteps.Add(step);
                }
                else if (section == "RECIPE")
                {
                    string processName = GetCol(cols, idxProcess).Trim();
                    string recipeName = GetCol(cols, idxRecipe).Trim();
                    string distanceText = GetCol(cols, idxDistance).Trim();

                    if (processName.Contains("string", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (string.IsNullOrWhiteSpace(processName) && string.IsNullOrWhiteSpace(recipeName))
                        continue;

                    if (!int.TryParse(distanceText, out int distance))
                        throw new InvalidOperationException($"Distance must be integer. value='{distanceText}'");

                    if (idxMode >= 0)
                    {
                        string rawMode = GetCol(cols, idxMode).Trim();

                        if (!string.IsNullOrWhiteSpace(rawMode))
                        {
                            string normalizedMode = NormalizeRunMode(rawMode);

                            if (!modeSpecified)
                            {
                                result.RunMode = normalizedMode;
                                modeSpecified = true;
                            }
                            else if (result.RunMode != normalizedMode)
                            {
                                throw new InvalidOperationException(
                                    $"Mode value is inconsistent in PREHEAT CSV. previous='{result.RunMode}', current='{normalizedMode}', line='{line}'");
                            }
                        }
                    }

                    result.Recipes.Add(new AldRecipeRow
                    {
                        ProcessName = processName,
                        RecipeName = recipeName,
                        Distance = distance
                    });
                }
            }

            if (result.PreheatSteps.Count == 0)
                throw new InvalidOperationException("START_ALD_PREHEAT csv has no PREHEAT rows.");

            if (result.Recipes.Count == 0)
                throw new InvalidOperationException("START_ALD_PREHEAT csv has no RECIPE rows.");

            return result;
        }

        private static int MapSvNoToCtcNo(int svNo)
        {
            // 현재 client.py에서 확인한 C-TC/SV 매핑 기준:
            // SV_NO 1~12가 heater_Write[0~11]에 대응되고,
            // C-TC 1~12가 heater_Read[0~11]에 대응된다.
            // 따라서 START_ALD_PREHEAT에서는 SV_NO와 같은 번호의 C-TC를 읽는다.
            return svNo switch
            {
                1 => 1,   // Stage
                2 => 2,   // Chamber Under
                3 => 3,   // Chamber Upper
                4 => 4,   // Precursor Line
                5 => 5,   // Reactant Line
                6 => 6,   // Precursor1
                7 => 7,   // Precursor2
                8 => 8,   // Precursor3
                9 => 9,   // Precursor4
                10 => 10, // Reactant1
                11 => 11, // Hot Trap
                12 => 12, // Pumping Line
                _ => throw new InvalidOperationException(
                    $"SV_NO out of range: {svNo}. valid range: 1~12")
            };
        }

        private static string[] SplitCsvLine(string line)
        {
            // 현재 기존 코드와 동일한 단순 CSV 처리 방식.
            // 값 안에 comma가 들어가는 CSV는 지원하지 않는다.
            return line.Split(',');
        }

        private static int FindHeader(string[] headers, string name)
        {
            for (int i = 0; i < headers.Length; i++)
            {
                if (string.Equals(headers[i].Trim(), name, StringComparison.OrdinalIgnoreCase))
                    return i;
            }
            return -1;
        }

        private static string GetCol(string[] cols, int index)
        {
            if (index < 0 || index >= cols.Length) return "";
            return cols[index].Trim();
        }

        private static int ParseRequiredInt(string[] cols, int index, string name)
        {
            string text = GetCol(cols, index);
            if (!int.TryParse(text, out int value))
                throw new InvalidOperationException($"{name} must be integer. value='{text}'");
            return value;
        }

        private static double ParseRequiredDouble(string[] cols, int index, string name)
        {
            string text = GetCol(cols, index);
            if (!double.TryParse(text, out double value))
                throw new InvalidOperationException($"{name} must be number. value='{text}'");
            return value;
        }

        private static int ParseOptionalInt(string[] cols, int index, int defaultValue)
        {
            string text = GetCol(cols, index);
            if (string.IsNullOrWhiteSpace(text)) return defaultValue;
            return int.TryParse(text, out int value) ? value : defaultValue;
        }

        private static double ParseOptionalDouble(string[] cols, int index, double defaultValue)
        {
            string text = GetCol(cols, index);
            if (string.IsNullOrWhiteSpace(text)) return defaultValue;
            return double.TryParse(text, out double value) ? value : defaultValue;
        }

        // ==== TCP 수신 루프 ====

        private static async Task AcceptLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && _listener != null)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(ct);
                }
                catch
                {
                    break;
                }

                _ = Task.Run(() => HandleClientAsync(client, ct), ct);
            }
        }

        /// <summary>
        /// 스트림에서 정확히 length 바이트를 읽는다. (중간에 끊기면 false)
        /// </summary>
        private static async Task<bool> ReadExactAsync(
            NetworkStream stream, byte[] buffer, int length, CancellationToken ct)
        {
            int offset = 0;
            while (offset < length)
            {
                int read;
                try
                {
                    read = await stream.ReadAsync(buffer, offset, length - offset, ct);
                }
                catch
                {
                    return false;
                }

                if (read <= 0)
                    return false; // 연결 종료

                offset += read;
            }
            return true;
        }

        // Big Endian 변환 유틸

        private static ushort ReadUInt16BE(byte[] buf, int offset)
        {
            return (ushort)((buf[offset] << 8) | buf[offset + 1]);
        }

        private static uint ReadUInt32BE(byte[] buf, int offset)
        {
            return ((uint)buf[offset] << 24)
                 | ((uint)buf[offset + 1] << 16)
                 | ((uint)buf[offset + 2] << 8)
                 | buf[offset + 3];
        }

        private static ulong ReadUInt64BE(byte[] buf, int offset)
        {
            return ((ulong)buf[offset] << 56)
                 | ((ulong)buf[offset + 1] << 48)
                 | ((ulong)buf[offset + 2] << 40)
                 | ((ulong)buf[offset + 3] << 32)
                 | ((ulong)buf[offset + 4] << 24)
                 | ((ulong)buf[offset + 5] << 16)
                 | ((ulong)buf[offset + 6] << 8)
                 | buf[offset + 7];
        }

        private static void WriteUInt16BE(byte[] buf, ref int offset, ushort value)
        {
            buf[offset++] = (byte)(value >> 8);
            buf[offset++] = (byte)(value & 0xFF);
        }

        private static void WriteUInt32BE(byte[] buf, ref int offset, uint value)
        {
            buf[offset++] = (byte)(value >> 24);
            buf[offset++] = (byte)(value >> 16);
            buf[offset++] = (byte)(value >> 8);
            buf[offset++] = (byte)(value & 0xFF);
        }

        private static void WriteUInt64BE(byte[] buf, ref int offset, ulong value)
        {
            buf[offset++] = (byte)(value >> 56);
            buf[offset++] = (byte)(value >> 48);
            buf[offset++] = (byte)(value >> 40);
            buf[offset++] = (byte)(value >> 32);
            buf[offset++] = (byte)(value >> 24);
            buf[offset++] = (byte)(value >> 16);
            buf[offset++] = (byte)(value >> 8);
            buf[offset++] = (byte)(value & 0xFF);   // ✅ 추가 (마지막 1바이트)
        }

        /// <summary>
        /// 응답 JSON을 받아 헤더+바디 패킷으로 만든다.
        /// </summary>
        private static byte[] BuildPacket(string bodyJson)
        {
            byte[] bodyBytes = Encoding.UTF8.GetBytes(bodyJson);

            // command 필드의 UTF-8 길이를 cmd_len에 넣는다.
            ushort cmdLen = 0;
            try
            {
                using var doc = JsonDocument.Parse(bodyJson);
                var root = doc.RootElement;
                if (root.TryGetProperty("command", out var cmdEl))
                {
                    string? cmd = cmdEl.GetString();
                    if (!string.IsNullOrEmpty(cmd))
                    {
                        cmdLen = (ushort)Encoding.UTF8.GetByteCount(cmd);
                    }
                }
            }
            catch
            {
                // command 읽기 실패 시 0 유지
            }

            byte version = ProtocolVersion;
            byte flags = 0;
            uint bodyLen = (uint)bodyBytes.Length;
            ulong ts = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var header = new byte[HeaderSize];
            int offset = 0;
            header[offset++] = version;
            header[offset++] = flags;
            WriteUInt16BE(header, ref offset, cmdLen);
            WriteUInt32BE(header, ref offset, bodyLen);
            WriteUInt64BE(header, ref offset, ts);

            var packet = new byte[HeaderSize + bodyBytes.Length];
            Buffer.BlockCopy(header, 0, packet, 0, HeaderSize);
            Buffer.BlockCopy(bodyBytes, 0, packet, HeaderSize, bodyBytes.Length);
            return packet;
        }

        /// <summary>
        /// 한 클라이언트 소켓에서 패킷을 읽어 명령을 처리한다.
        /// </summary>
        private static async Task HandleClientAsync(TcpClient client, CancellationToken ct)
        {
            using (client)
            using (var stream = client.GetStream())
            {
                client.NoDelay = true;

                var headerBuf = new byte[HeaderSize];

                while (!ct.IsCancellationRequested)
                {
                    // 1) 헤더 읽기
                    bool ok = await ReadExactAsync(stream, headerBuf, HeaderSize, ct);
                    if (!ok)
                        break;

                    byte version = headerBuf[0];
                    byte flags = headerBuf[1];
                    ushort cmdLen = ReadUInt16BE(headerBuf, 2);
                    uint bodyLen = ReadUInt32BE(headerBuf, 4);
                    ulong ts = ReadUInt64BE(headerBuf, 8);

                    if (version != ProtocolVersion)
                    {
                        // 버전이 다르면 연결 종료
                        break;
                    }

                    if (bodyLen == 0 || bodyLen > 10_000_000)
                    {
                        break;
                    }

                    // 2) 바디(JSON) 읽기
                    var bodyBuf = new byte[(int)bodyLen];   // ✅ 수정
                    ok = await ReadExactAsync(stream, bodyBuf, (int)bodyLen, ct);
                    if (!ok)
                        break;

                    string reqJson = Encoding.UTF8.GetString(bodyBuf);
                    DateTime recvTime = DateTime.Now;
                    string remote = client.Client.RemoteEndPoint?.ToString() ?? "";

                    // 3) JSON 처리
                    string respJson;
                    try
                    {
                        respJson = HandleJsonWithHostLog(reqJson, recvTime, remote);
                    }
                    catch (Exception ex)
                    {
                        respJson = BuildErrorResponse("\"0\"", "ALD_ERROR", ex.Message);
                    }

                    // 4) 응답 전송
                    byte[] packet = BuildPacket(respJson);
                    try
                    {
                        await stream.WriteAsync(packet, 0, packet.Length, ct);
                        await stream.FlushAsync(ct);

                        LogComm(recvTime, DateTime.Now, remote, reqJson, respJson);
                    }
                    catch
                    {
                        break;
                    }
                }
            }
        }

        // ==== JSON 처리 ====

        /// <summary>
        /// 요청 JSON을 파싱해서 START_ALD / GET_ALD_STATUS를 분기 처리한다.
        /// </summary>
        private static string HandleJson(string requestJson)
        {
            using var doc = JsonDocument.Parse(requestJson);
            var root = doc.RootElement;

            // request_id는 RawText로 그대로 보존 (따옴표 포함)
            string requestIdRaw = root.TryGetProperty("request_id", out var idEl)
                ? idEl.GetRawText()
                : "\"0\"";

            if (!root.TryGetProperty("command", out var cmdEl))
            {
                return BuildErrorResponse(requestIdRaw, "ALD_ERROR", "command not provided");
            }
            string command = cmdEl.GetString() ?? "";
            if (string.IsNullOrWhiteSpace(command))
            {
                return BuildErrorResponse(requestIdRaw, "ALD_ERROR", "command is empty");
            }

            if (command == "START_ALD")
            {
                if (!root.TryGetProperty("data", out var data))
                {
                    return BuildErrorResponse(requestIdRaw, "START_ALD", "data not provided");
                }

                // data["csv_path"] : CSV 전체 경로
                if (!data.TryGetProperty("csv_path", out var csvPathEl))
                {
                    return BuildErrorResponse(requestIdRaw, "START_ALD", "csv_path not provided");
                }

                string csvPath = csvPathEl.GetString() ?? "";
                return HandleStartAld(requestIdRaw, csvPath);
            }
            else if (command == "START_ALD_PREHEAT")
            {
                // 새 명령:
                // client는 csv_path만 보낸다.
                // SV 번호/값, C-TC 번호, soak 시간은 CSV 내부에서 읽는다.
                return HandleStartAldPreheat(requestIdRaw, root);
            }
            else if (command == "CANCEL_ALD_PREHEAT")
            {
                // 예열/soak 중 취소용.
                // 실제 ALD 공정이 이미 시작된 뒤에는 공정 정지 명령이 아니라 preheat job 취소 요청이다.
                return HandleCancelAldPreheat(requestIdRaw);
            }
            else if (command == "GET_ALD_STATUS")
            {
                return HandleGetStatus(requestIdRaw);
            }
            else if (command == "GET_VACUUM")
            {
                return HandleGetVacuum(requestIdRaw);
            }
            else if (command == "GET_GATE_STATUS")
            {
                return HandleGetGateStatus(requestIdRaw);
            }
            else if (command == "SET_HEATER_SV")
            {
                return HandleSetHeaterSv(requestIdRaw, root);
            }
            else if (command == "GET_TEMPERATURE")
            {
                return HandleGetTemperature(requestIdRaw, root);
            }
            else
            {
                return BuildErrorResponse(requestIdRaw, command, "Unknown command");
            }
        }

        /// <summary>
        /// START_ALD 처리:
        /// - CSV를 읽어 레시피 리스트로 변환
        /// - 콜백으로 전달
        /// </summary>
        private static string HandleStartAld(string requestIdRaw, string csvPath)
        {
            if (_startRecipeCallback == null)
                return BuildErrorResponse(requestIdRaw, "START_ALD", "Callback not set");

            // START_ALD_PREHEAT가 진행 중일 때 외부에서 START_ALD가 또 들어오면
            // 예열 sequence와 실제 공정 시작이 충돌할 수 있으므로 거절한다.
            lock (_preheatLock)
            {
                if (_preheatJob != null &&
                    IsPreheatActivePhase(_preheatJob.Phase))
                {
                    return BuildErrorResponse(
                        requestIdRaw,
                        "START_ALD",
                        $"Preheat job is already running. phase={_preheatJob.Phase}");
                }
            }

            // 이미 ALD 공정이 실행 중이면 새 START_ALD를 받지 않는다.
            // 이전 공정의 TotalTime을 새 공정 값으로 오인하는 것도 방지한다.
            if (GetProcessBoolFlag())
            {
                return BuildErrorResponse(
                    requestIdRaw,
                    "START_ALD",
                    "ALD process is already running");
            }

            csvPath = csvPath?.Trim() ?? "";
            if (string.IsNullOrEmpty(csvPath))
            {
                return BuildErrorResponse(requestIdRaw, "START_ALD", "csv_path is empty");
            }

            List<AldRecipeRow> loaded;
            string mode;

            try
            {
                loaded = LoadRecipeCsv(csvPath, out mode);
            }
            catch (Exception ex)
            {
                return BuildErrorResponse(requestIdRaw, "START_ALD", ex.Message);
            }

            HostLogUpdateRecipe(loaded);

            try
            {
                // 로그 이름 보정과 실제 공정 시작을 공통 함수에서 처리한다.
                StartAldInternal(loaded, mode);

                // 메인 프로그램이 새 레시피의 TotalTime을 계산한 뒤 읽는다.
                int? minTotalS = WaitForMinTotalSeconds();

                return BuildStartResult(
                    requestIdRaw,
                    "success",
                    $"ALD process started. mode={mode}",
                    minTotalS);
            }
            catch (Exception ex)
            {
                return BuildErrorResponse(
                    requestIdRaw,
                    "START_ALD",
                    ex.Message);
            }
        }

        /// <summary>
        /// 실제 ALD 공정 시작 공통 함수.
        /// START_ALD와 START_ALD_PREHEAT 마지막 단계가 같이 사용한다.
        /// </summary>
        private static void StartAldInternal(
            List<AldRecipeRow> loaded,
            string mode)
        {
            if (_startRecipeCallback == null)
                throw new InvalidOperationException("Callback not set");

            if (loaded == null || loaded.Count == 0)
                throw new InvalidOperationException("No recipe rows to start.");

            // 단일 레시피 실행인 경우 실제 레시피 이름으로 보정한다.
            //
            // 여러 행은 기존 메인 프로그램의 처리 방식을 유지한다.
            // 행별 시작 시점과 로그 생성 방식이 확인되기 전에는
            // 전체 작업을 첫 번째 레시피 이름으로 임의 지정하지 않는다.
            if (loaded.Count == 1)
            {
                SetSelectedRecipeForExternalStart(
                    loaded[0].RecipeName);
            }

            lock (_lock)
            {
                _recipes = loaded;
            }

            // 반드시 이름 보정 이후에 공정을 시작한다.
            _startRecipeCallback(loaded, mode);

            // 호스트 로그: START 명령 전달 완료 → 관측 시작
            HostLogOnStartCommandSent();
        }


        /// <summary>
        /// START_ALD_PREHEAT 처리.
        /// 오래 걸리는 예열/대기 시퀀스이므로 요청 thread에서 기다리지 않고 background Task로 실행한다.
        /// </summary>
        private static string HandleStartAldPreheat(string requestIdRaw, JsonElement root)
        {
            if (_startRecipeCallback == null)
                return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT", "Callback not set");

            if (_getStatusCallback == null)
                return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT", "Status callback not set");

            if (!root.TryGetProperty("data", out var data))
                return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT", "data not provided");

            if (!data.TryGetProperty("csv_path", out var csvPathEl))
                return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT", "csv_path not provided");

            string csvPath = csvPathEl.GetString()?.Trim() ?? "";
            if (string.IsNullOrEmpty(csvPath))
                return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT", "csv_path is empty");

            PreheatRecipeFile parsed;
            try
            {
                parsed = LoadPreheatCsv(csvPath);
            }
            catch (Exception ex)
            {
                return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT", ex.Message);
            }

            HostLogUpdateRecipe(parsed.Recipes);

            string mode = parsed.RunMode;

            // 시작 전 안전 조건 확인
            try
            {
                ValidatePreheatSafeState();
            }
            catch (Exception ex)
            {
                return BuildErrorResponse(
                    requestIdRaw, "START_ALD_PREHEAT", ex.Message);
            }

            PreheatJob job;
            CancellationTokenSource cts;

            lock (_preheatLock)
            {
                if (_preheatJob != null && IsPreheatActivePhase(_preheatJob.Phase))
                    return BuildErrorResponse(requestIdRaw, "START_ALD_PREHEAT",
                        $"Preheat job is already running. phase={_preheatJob.Phase}");

                _preheatCts?.Cancel();
                _preheatCts?.Dispose();
                _preheatCts = new CancellationTokenSource();
                cts = _preheatCts;

                job = new PreheatJob
                {
                    JobId = $"preheat-{DateTime.Now:yyyyMMdd-HHmmss}",
                    CsvPath = csvPath,
                    RunMode = mode,
                    Phase = "QUEUED",
                    Message = "Preheat job accepted",
                    StartedAt = DateTime.Now,
                    PhaseStartedAt = DateTime.Now,
                    Steps = parsed.PreheatSteps,
                    Recipes = parsed.Recipes,
                    SoakSec = parsed.SoakSec,
                    PollSec = parsed.PollSec,
                    StableCount = parsed.StableCount,
                    HeatTimeoutSec = parsed.HeatTimeoutSec,
                    SoakRemainingSec = parsed.SoakSec
                };

                _preheatJob = job;
                HostLogOnPreheatAccepted(job);
            }

            // 핵심:
            // 요청 TCP 응답은 즉시 accepted로 보내고,
            // 실제 SV 설정/온도 대기/soak/START_ALD는 background에서 진행한다.
            _ = Task.Run(() => RunPreheatSequenceAsync(job, cts.Token));

            return BuildPreheatAcceptedResult(requestIdRaw, job);
        }

        /// <summary>
        /// START_ALD_PREHEAT 취소.
        /// 아직 실제 ALD START가 들어가기 전 예열/soak 단계에서 취소할 때 사용한다.
        /// </summary>
        private static string HandleCancelAldPreheat(string requestIdRaw)
        {
            lock (_preheatLock)
            {
                if (_preheatJob == null || !IsPreheatActivePhase(_preheatJob.Phase))
                    return BuildErrorResponse(requestIdRaw, "CANCEL_ALD_PREHEAT", "No active preheat job");

                // START_ALD가 이미 들어간 뒤에는 이 명령으로 실제 ALD 공정을 멈출 수 없다.
                // 따라서 취소 성공처럼 보이면 client가 오해할 수 있으므로 거절한다.
                if (_preheatJob.Phase == "STARTING" || _preheatJob.Phase == "PROCESS")
                {
                    return BuildErrorResponse(
                        requestIdRaw,
                        "CANCEL_ALD_PREHEAT",
                        $"Cannot cancel preheat because ALD start was already requested. phase={_preheatJob.Phase}"
                    );
                }

                _preheatCts?.Cancel();

                return new JsonObject
                {
                    ["request_id"] = JsonNode.Parse(requestIdRaw),
                    ["command"] = "CANCEL_ALD_PREHEAT_RESULT",
                    ["data"] = new JsonObject
                    {
                        ["result"] = "success",
                        ["message"] = "Cancel requested",
                        ["job_id"] = _preheatJob.JobId,
                        ["phase"] = _preheatJob.Phase
                    }
                }.ToJsonString();
            }
        }

        private static bool TryReadAnalogBoolField(string fieldName, out bool value)
        {
            value = false;

            try
            {
                var analogType = FindTypeInCurrentAppDomain("ALD.Analog_Comm");
                if (analogType == null) return false;

                var field = analogType.GetField(
                    fieldName,
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (field == null) return false;

                object? raw = field.GetValue(null);
                if (raw == null) return false;

                value = Convert.ToBoolean(raw);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static async Task WaitForPlcWriteDoneAsync(int timeoutMs, CancellationToken ct)
        {
            DateTime start = DateTime.Now;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                if (TryReadAnalogBoolField("write_PLC", out bool writePlc))
                {
                    if (!writePlc)
                        return;
                }

                if ((DateTime.Now - start).TotalMilliseconds > timeoutMs)
                    throw new TimeoutException("Timeout waiting for ALD.Analog_Comm.write_PLC to become false");

                await Task.Delay(50, ct);
            }
        }

        private static async Task RunPreheatSequenceAsync(PreheatJob job, CancellationToken ct)
        {
            try
            {
                SetPreheatPhase(job, "SETTING_SV", "Writing heater SV values");

                // 1. CSV의 PREHEAT row에 있는 모든 SV를 먼저 설정
                foreach (var step in job.Steps)
                {
                    ct.ThrowIfCancellationRequested();
                    ValidatePreheatSafeState();

                    SetHeaterSvInternal(
                        step.SvNo, step.Sv, blockWhenProcessRunning: true);

                    // PLC write loop가 write_PLC를 처리하고 false로 내릴 때까지 대기
                    await WaitForPlcWriteDoneAsync(3000, ct);

                    // 다음 SV write 전에 약간의 여유
                    await Task.Delay(200, ct);
                }

                // 2. 모든 C-TC가 목표 온도에 도달할 때까지 대기
                SetPreheatPhase(job, "HEATING", "Waiting until all C-TC values reach target");

                DateTime heatStart = DateTime.Now;
                int stable = 0;

                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    if ((DateTime.Now - heatStart).TotalSeconds > job.HeatTimeoutSec)
                        throw new TimeoutException($"Preheat timeout. elapsed>{job.HeatTimeoutSec}s");

                    ValidatePreheatSafeState();

                    bool allReached = true;

                    foreach (var step in job.Steps)
                    {
                        // tcType 2 = C-TC
                        var temp = ReadTemperatureInternal(2, step.CtcNo);

                        step.CurrentCtc = temp.Value;
                        step.Reached = IsCtcWithinTargetRange(step, temp.Value);

                        if (!step.Reached)
                            allReached = false;
                    }

                    stable = allReached ? stable + 1 : 0;
                    UpdatePreheatMessage(job, $"Heating... stable={stable}/{job.StableCount}");

                    if (stable >= job.StableCount)
                        break;

                    await Task.Delay(job.PollSec * 1000, ct);
                }

                // 3. 목표 온도 도달 후 soak 대기
                SetPreheatPhase(job, "SOAKING", "Temperature reached. Soaking before START_ALD");

                DateTime soakStart = DateTime.Now;
                bool wasOutOfRangeDuringSoak = false;

                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    ValidatePreheatSafeState();

                    bool allReached = true;

                    foreach (var step in job.Steps)
                    {
                        var temp = ReadTemperatureInternal(2, step.CtcNo);

                        step.CurrentCtc = temp.Value;
                        step.Reached = IsCtcWithinTargetRange(step, temp.Value);

                        if (!step.Reached)
                            allReached = false;
                    }

                    if (!allReached)
                    {
                        // 온도가 허용 범위를 벗어나면 soak 카운트를 처음부터 다시 계산
                        soakStart = DateTime.Now;
                        job.SoakRemainingSec = job.SoakSec;
                        wasOutOfRangeDuringSoak = true;

                        UpdatePreheatMessage(job, "Temperature out of range during soak. Soak timer reset");

                        await Task.Delay(job.PollSec * 1000, ct);
                        continue;
                    }

                    int elapsed = (int)(DateTime.Now - soakStart).TotalSeconds;
                    job.SoakRemainingSec = Math.Max(0, job.SoakSec - elapsed);

                    if (wasOutOfRangeDuringSoak)
                    {
                        UpdatePreheatMessage(job, "Temperature recovered. Soaking restarted");
                        wasOutOfRangeDuringSoak = false;
                    }
                    else
                    {
                        UpdatePreheatMessage(job, $"Soaking... remaining={job.SoakRemainingSec}s");
                    }

                    if (elapsed >= job.SoakSec)
                        break;

                    await Task.Delay(job.PollSec * 1000, ct);
                }

                // 4. 최종 확인 후 기존 START_ALD 로직 실행
                ct.ThrowIfCancellationRequested();

                SetPreheatPhase(job, "STARTING", "Starting ALD process");
                ValidatePreheatSafeState();

                ct.ThrowIfCancellationRequested();
                HostLogSetCurrentKey(job.HostLogKey);
                StartAldInternal(job.Recipes, job.RunMode);

                SetPreheatPhase(job, "PROCESS", "ALD process start command sent");

                // 5. 이후 실제 ALD process 상태를 잠깐 모니터링
                await MonitorProcessAfterStartAsync(job, ct);
            }
            catch (PreheatInterruptedException ex)
            {
                SetPreheatPhase(job, "CANCELLED", ex.Message);
            }
            catch (OperationCanceledException)
            {
                SetPreheatPhase(job, "CANCELLED", "Preheat job cancelled");
            }
            catch (Exception ex)
            {
                lock (_preheatLock)
                {
                    job.Phase = "FAIL";
                    job.ErrorMessage = ex.Message;
                    job.Message = ex.Message;
                    job.PhaseStartedAt = DateTime.Now;
                }
            }
            finally
            {
                // 호스트 로그: START 전에 끝난 예열 잡 기록 (START 후에는 관측 태스크가 기록)
                HostLogOnPreheatFinished(job);
            }
        }

        private static bool IsCtcWithinTargetRange(PreheatStep step, double ctcValue)
        {
            double lower = step.Sv - step.Tolerance;
            double upper = step.Sv + step.Tolerance;

            return ctcValue >= lower && ctcValue <= upper;
        }

        private static async Task MonitorProcessAfterStartAsync(
            PreheatJob job,
            CancellationToken ct)
        {
            DateTime waitStart = DateTime.UtcNow;
            bool processSeenRunning = false;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                ReadCurrentAldState(
                    out bool alarmOn,
                    out bool processRunning,
                    out _,
                    out string state);

                if (state == "error")
                {
                    throw new InvalidOperationException(
                        alarmOn
                            ? "ALD alarm is active"
                            : "ALD status is error");
                }

                if (processRunning)
                    processSeenRunning = true;

                if (processSeenRunning && !processRunning)
                {
                    SetPreheatPhase(
                        job,
                        "DONE",
                        "ALD process is no longer running; outcome not verified");

                    return;
                }

                if (!processSeenRunning &&
                    (DateTime.UtcNow - waitStart).TotalSeconds > 60)
                {
                    throw new TimeoutException(
                        "ALD process running state was not observed "
                        + "within 60 seconds after START_ALD");
                }

                await Task.Delay(1000, ct);
            }
        }

        private sealed class PreheatInterruptedException : Exception
        {
            public PreheatInterruptedException(string message)
                : base(message)
            {
            }
        }

        private static void ValidatePreheatSafeState()
        {
            ReadCurrentAldState(
                out bool alarmOn,
                out bool processRunning,
                out bool manualRunning,
                out string state);

            if (state == "error")
            {
                throw new InvalidOperationException(
                    alarmOn ? "ALD alarm is active" : "ALD status is error");
            }

            if (processRunning || manualRunning)
            {
                throw new PreheatInterruptedException(
                    processRunning
                        ? "Preheat interrupted: ALD process is running"
                        : "Preheat interrupted: manual command is running");
            }

            if (!GetModbusCheckFlag())
            {
                throw new InvalidOperationException(
                    "Modbus is not initialized for preheat");
            }
        }

        private static bool IsPreheatActivePhase(string phase)
        {
            return phase == "QUEUED"
                || phase == "SETTING_SV"
                || phase == "HEATING"
                || phase == "SOAKING"
                || phase == "STARTING"
                || phase == "PROCESS";
        }

        private static void SetPreheatPhase(PreheatJob job, string phase, string message)
        {
            lock (_preheatLock)
            {
                SetPreheatPhaseLocked(job, phase, message);
            }
        }

        private static void SetPreheatPhaseLocked(PreheatJob job, string phase, string message)
        {
            job.Phase = phase;
            job.Message = message;
            job.PhaseStartedAt = DateTime.Now;
        }

        private static void UpdatePreheatMessage(PreheatJob job, string message)
        {
            lock (_preheatLock)
            {
                job.Message = message;
            }
        }


        /// <summary>
        /// GET_ALD_STATUS 처리:
        /// - 상태 콜백을 호출해 현재 상태를 반환
        /// </summary>
        private static string HandleGetStatus(string requestIdRaw)
        {
            AldStatus status;
            bool alarmOn;
            bool processRunning;
            bool manualRunning;
            string state;

            try
            {
                status = ReadCurrentAldState(
                    out alarmOn,
                    out processRunning,
                    out manualRunning,
                    out state);
            }
            catch (Exception ex)
            {
                return BuildErrorResponse(
                    requestIdRaw, "GET_ALD_STATUS", ex.Message);
            }

            string message = BuildDefaultStatusMessage(state, alarmOn);

            if (state == "running" && !processRunning && manualRunning)
                message = "Manual command running";

            lock (_preheatLock)
            {
                var job = _preheatJob;

                // 현재 오류 또는 실행 상태를 예열 상태로 덮어쓰지 않습니다.
                if (state == "idle" &&
                    job != null &&
                    IsPreheatActivePhase(job.Phase))
                {
                    // PROCESS는 실제 실행 여부를 위에서 판정합니다.
                    // 시작 확인을 기다리는 동안에도 임의로 running을 만들지 않습니다.
                    state = "preheating";
                    message = job.Phase == "PROCESS"
                        ? "Waiting for ALD process state confirmation"
                        : BuildSimplePreheatStatusMessageLocked(job);
                }

                // FAIL / CANCELLED / DONE은 과거 작업 결과입니다.
                // 현재 장비 state와 message에는 덮어쓰지 않습니다.
            }

            int? remainingS = null;

            // 수동 명령만 실행 중인 경우 ALD 공정 ETA를 표시하지 않습니다.
            if (state == "running" && processRunning)
                remainingS = GetAldRemainingSeconds();

            var data = new JsonObject
            {
                ["state"] = state,
                ["vacuum"] = status.Vacuum,
                ["alarm"] = alarmOn,
                ["message"] = message,
                ["eta"] = new JsonObject
                {
                    ["ALD"] = new JsonObject
                    {
                        ["remaining_s"] = remainingS.HasValue
                            ? JsonValue.Create(remainingS.Value)
                            : null
                    }
                }
            };

            var root = new JsonObject
            {
                ["request_id"] = JsonNode.Parse(requestIdRaw),
                ["command"] = "GET_ALD_STATUS_RESULT",
                ["data"] = data
            };

            return root.ToJsonString();
        }

        private static void AppendPreheatSummaryToStatusDataLocked(JsonObject data, PreheatJob job)
        {
            string message = BuildSimplePreheatStatusMessageLocked(job);

            if (!string.IsNullOrWhiteSpace(message))
                data["message"] = message;
        }


        private static string NormalizeClientState(string? rawState, bool alarmOn)
        {
            if (alarmOn)
                return "error";

            if (string.Equals(rawState, "running", StringComparison.OrdinalIgnoreCase))
                return "running";

            if (string.Equals(rawState, "error", StringComparison.OrdinalIgnoreCase))
                return "error";

            return "idle";
        }

        private static string BuildDefaultStatusMessage(string state, bool alarmOn)
        {
            if (alarmOn)
                return "Alarm active";

            switch (state)
            {
                case "running":
                    return "Process running";

                case "error":
                    return "Error";

                case "idle":
                default:
                    return "Ready";
            }
        }

        private static string GetClientStateForPreheatJobLocked(PreheatJob job)
        {
            if (job.Phase == "PROCESS")
                return "running";

            return "preheating";
        }

        private static string BuildSimplePreheatStatusMessageLocked(PreheatJob job)
        {
            string phase = job.Phase ?? "";

            if (phase == "FAIL")
            {
                string error = string.IsNullOrWhiteSpace(job.ErrorMessage)
                    ? job.Message
                    : job.ErrorMessage;

                if (string.IsNullOrWhiteSpace(error))
                    return "Preheat failed";

                if (error.IndexOf("timeout", StringComparison.OrdinalIgnoreCase) >= 0)
                    return "Preheat timeout";

                if (error.IndexOf("alarm", StringComparison.OrdinalIgnoreCase) >= 0)
                    return "Alarm active";

                if (error.IndexOf("Modbus", StringComparison.OrdinalIgnoreCase) >= 0)
                    return "Communication error";

                return error;
            }

            if (phase == "CANCELLED")
                return "Preheat cancelled";

            if (phase == "QUEUED" || phase == "SETTING_SV")
                return "Preparing preheat";

            if (phase == "HEATING")
                return "Preheating";

            if (phase == "SOAKING")
            {
                if (!string.IsNullOrWhiteSpace(job.Message) &&
                    job.Message.IndexOf("out of range", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return "Temperature out of range";
                }

                return "Stabilizing temperature";
            }

            if (phase == "STARTING")
                return "Starting process";

            if (phase == "PROCESS")
                return "Process running";

            return "Preheating";
        }

        private static string BuildPreheatAcceptedResult(string requestIdRaw, PreheatJob job)
        {
            var root = new JsonObject
            {
                ["request_id"] = JsonNode.Parse(requestIdRaw),
                ["command"] = "START_ALD_PREHEAT_RESULT",
                ["data"] = new JsonObject
                {
                    ["result"] = "accepted",
                    ["message"] = "Preheat sequence started",
                    ["job_id"] = job.JobId,
                    ["phase"] = job.Phase,
                    ["preheat_count"] = job.Steps.Count,
                    ["recipe_count"] = job.Recipes.Count,
                    ["soak_sec"] = job.SoakSec
                }
            };

            return root.ToJsonString();
        }


        private static string HandleGetGateStatus(string requestIdRaw)
        {
            if (!TryGetGateStatus(out bool gateOpenRaw, out bool gateCloseRaw))
                return BuildErrorResponse(requestIdRaw, "GET_GATE_STATUS", "Gate status read failed (reflection)");

            // ALD 메인 로직과 동일한 판정
            if (gateOpenRaw && !gateCloseRaw)
            {
                var ok = new JsonObject
                {
                    ["request_id"] = JsonNode.Parse(requestIdRaw),
                    ["command"] = "GET_GATE_STATUS_RESULT",
                    ["data"] = new JsonObject
                    {
                        ["gate_state"] = "open"
                    }
                };
                return ok.ToJsonString();
            }

            if (!gateOpenRaw && gateCloseRaw)
            {
                var ok = new JsonObject
                {
                    ["request_id"] = JsonNode.Parse(requestIdRaw),
                    ["command"] = "GET_GATE_STATUS_RESULT",
                    ["data"] = new JsonObject
                    {
                        ["gate_state"] = "close"
                    }
                };
                return ok.ToJsonString();
            }

            // 둘 다 false(이동 중/미정) 또는 둘 다 true(센서 이상) 같은 애매한 상태는 fail로 처리
            return BuildErrorResponse(
                requestIdRaw,
                "GET_GATE_STATUS",
                $"Ambiguous gate state (open_raw={gateOpenRaw}, close_raw={gateCloseRaw})"
            );
        }


        private static string HandleGetVacuum(string requestIdRaw)
        {
            bool modbusReady = GetModbusCheckFlag();

            if (!modbusReady)
                return BuildErrorResponse(requestIdRaw, "GET_VACUUM", "Modbus not started");

            if (!TryGetLoadlockVacuum(out int raw, out double torr, out string err))
                return BuildErrorResponse(requestIdRaw, "GET_VACUUM", err);

            var root = new JsonObject
            {
                ["request_id"] = JsonNode.Parse(requestIdRaw),
                ["command"] = "GET_VACUUM_RESULT",
                ["data"] = new JsonObject
                {
                    ["result"] = "success",
                    ["message"] = "OK",
                    ["target"] = "loadlock",
                    ["modbus_ready"] = modbusReady,
                    ["raw"] = raw,
                    ["torr"] = torr
                }
            };
            return root.ToJsonString();
        }


        private static bool GetProcessBoolFlag()
        {
            try
            {
                var utilityType = FindTypeInCurrentAppDomain("ALD.Utility");
                if (utilityType == null) return false;

                var f = utilityType.GetField(
                    "Process_Bool",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (f == null) return false;

                object? val = f.GetValue(null);
                if (val is bool b) return b;

                if (val != null && bool.TryParse(val.ToString(), out bool bb))
                    return bb;

                return false;
            }
            catch
            {
                return false;
            }
        }


        // 업체 프로그램의 필수 static 필드를 읽습니다.
        // 읽기 실패를 정상 상태(false/0)로 바꾸지 않습니다.
        private static T ReadRequiredAldStaticField<T>(
            string typeName,
            string fieldName)
        {
            var type = FindTypeInCurrentAppDomain(typeName);
            if (type == null)
                throw new InvalidOperationException(
                    $"ALD type not found: {typeName}");

            var field = type.GetField(
                fieldName,
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Static);

            if (field == null)
                throw new InvalidOperationException(
                    $"ALD field not found: {typeName}.{fieldName}");

            object? value = field.GetValue(null);

            if (value is T typedValue)
                return typedValue;

            throw new InvalidOperationException(
                $"Invalid ALD field value: {typeName}.{fieldName}");
        }

        private static bool ReadManualCommandRunning()
        {
            var type = FindTypeInCurrentAppDomain("ALD.Thread_Command");
            if (type == null)
                throw new InvalidOperationException(
                    "ALD type not found: ALD.Thread_Command");

            var instanceField = type.GetField(
                "_instance",
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Static);

            var runningField = type.GetField(
                "isRunning",
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance);

            if (instanceField == null || runningField == null)
                throw new InvalidOperationException(
                    "ALD.Thread_Command fields not found");

            // Instance 프로퍼티를 호출해서 새 객체를 만들지 않습니다.
            object? instance = instanceField.GetValue(null);

            // 아직 생성되지 않은 명령 작업자는 실행 중일 수 없습니다.
            if (instance == null)
                return false;

            object? value = runningField.GetValue(instance);

            if (value is bool running)
                return running;

            throw new InvalidOperationException(
                "Invalid ALD.Thread_Command.isRunning value");
        }

        private static AldStatus ReadCurrentAldState(
            out bool alarmOn,
            out bool processRunning,
            out bool manualRunning,
            out string state)
        {
            var callback = _getStatusCallback;
            if (callback == null)
                throw new InvalidOperationException("Status callback not set");

            var status = callback();
            if (status == null)
                throw new InvalidOperationException("Status callback returned null");

            alarmOn =
                ReadRequiredAldStaticField<int>("ALD.Utility", "State") == -1;

            processRunning =
                ReadRequiredAldStaticField<bool>(
                    "ALD.Utility", "Process_Bool")
                || string.Equals(
                    status.State, "running", StringComparison.OrdinalIgnoreCase);

            manualRunning = ReadManualCommandRunning();

            // 현재 오류가 실행 상태보다 우선합니다.
            if (alarmOn ||
                string.Equals(
                    status.State, "error", StringComparison.OrdinalIgnoreCase))
            {
                state = "error";
            }
            else if (processRunning || manualRunning)
            {
                state = "running";
            }
            else
            {
                state = "idle";
            }

            return status;
        }


        /// <summary>
        /// ALD 메인 프로그램의 현재 공정 시간 정보를 한 번에 읽는다.
        /// 읽기에 실패하면 false를 반환하고, 통신 자체는 중단하지 않는다.
        /// </summary>
        private static bool TryGetAldTimingSnapshot(
            out AldTimingSnapshot snapshot)
        {
            snapshot = new AldTimingSnapshot();

            try
            {
                Type? utilityType =
                    FindTypeInCurrentAppDomain("ALD.Utility");

                Type? processType =
                    FindTypeInCurrentAppDomain("ALD.Thread_Process");

                if (utilityType == null || processType == null)
                    return false;

                const BindingFlags staticFlags =
                    BindingFlags.Public |
                    BindingFlags.NonPublic |
                    BindingFlags.Static;

                const BindingFlags instanceFlags =
                    BindingFlags.Public |
                    BindingFlags.NonPublic |
                    BindingFlags.Instance;

                // ALD.Utility의 static 필드
                FieldInfo? processBoolField =
                    utilityType.GetField("Process_Bool", staticFlags);

                FieldInfo? processTimeStartField =
                    utilityType.GetField("ProcessTimeStart", staticFlags);

                // Thread_Process 싱글턴의 실제 인스턴스
                // Instance 프로퍼티를 호출하지 않고 _instance를 직접 읽어서
                // 상태 조회가 새 인스턴스를 만들지 않도록 한다.
                FieldInfo? singletonField =
                    processType.GetField("_instance", staticFlags);

                if (processBoolField == null ||
                    processTimeStartField == null ||
                    singletonField == null)
                {
                    return false;
                }

                object? processInstance = singletonField.GetValue(null);

                if (processInstance == null)
                    return false;

                FieldInfo? monitorField =
                    processType.GetField("recipeMonitor", instanceFlags);

                FieldInfo? processIndexField =
                    processType.GetField("Process_Idx", instanceFlags);

                FieldInfo? workerRunningField =
                    processType.GetField("isRunning", instanceFlags);

                if (monitorField == null ||
                    processIndexField == null ||
                    workerRunningField == null)
                {
                    return false;
                }

                object? monitor = monitorField.GetValue(processInstance);

                if (monitor == null)
                    return false;

                Type monitorType = monitor.GetType();

                PropertyInfo? totalTimeProperty =
                    monitorType.GetProperty("TotalTime", instanceFlags);

                PropertyInfo? startTimeProperty =
                    monitorType.GetProperty("StartTime", instanceFlags);

                if (totalTimeProperty == null ||
                    startTimeProperty == null)
                {
                    return false;
                }

                object? startTimeRaw =
                    startTimeProperty.GetValue(monitor);

                if (startTimeRaw is not DateTime startTime)
                    return false;

                snapshot = new AldTimingSnapshot
                {
                    ProcessRunning = Convert.ToBoolean(
                        processBoolField.GetValue(null)),

                    WorkerRunning = Convert.ToBoolean(
                        workerRunningField.GetValue(processInstance)),

                    ProcessTimeStarted = Convert.ToBoolean(
                        processTimeStartField.GetValue(null)),

                    ProcessIndex = Convert.ToInt32(
                        processIndexField.GetValue(processInstance)),

                    TotalSeconds = Convert.ToDouble(
                        totalTimeProperty.GetValue(monitor)),

                    StartTime = startTime
                };

                return true;
            }
            catch
            {
                // 메인 프로그램 버전 변경이나 리플렉션 실패가 발생해도
                // TCP 상태 조회 자체를 실패시키지는 않는다.
                return false;
            }
        }


        /// <summary>
        /// START_ALD callback 이후 메인 프로그램이 계산한 TotalTime을 읽는다.
        ///
        /// Thread_Process.isRunning은 TotalTime 계산이 끝난 다음 true가 되므로,
        /// 이전 공정의 TotalTime을 읽지 않도록 WorkerRunning까지 확인한다.
        /// </summary>
        private static int? WaitForMinTotalSeconds()
        {
            const int retryCount = 20;
            const int retryDelayMs = 50;

            for (int i = 0; i < retryCount; i++)
            {
                if (TryGetAldTimingSnapshot(out AldTimingSnapshot timing) &&
                    timing.ProcessRunning &&
                    timing.WorkerRunning &&
                    timing.TotalSeconds > 0)
                {
                    // 소수점 이하는 올림한다.
                    // 로봇에게 실제보다 짧은 시간을 보내는 것을 방지한다.
                    return (int)Math.Ceiling(timing.TotalSeconds);
                }

                if (i < retryCount - 1)
                    Thread.Sleep(retryDelayMs);
            }

            // START 자체는 성공했지만 1초 안에 시간이 준비되지 않은 경우
            return null;
        }


        /// <summary>
        /// GET_ALD_STATUS에서 보낼 ALD 레시피 남은 시간을 계산한다.
        /// </summary>
        private static int? GetAldRemainingSeconds()
        {
            if (!TryGetAldTimingSnapshot(out AldTimingSnapshot timing))
                return null;

            // 전체 공정이 실행 중이 아니면 ETA가 없다.
            if (!timing.ProcessRunning || !timing.WorkerRunning)
                return null;

            // 실제 ALD 레시피가 시작된 상태
            if (timing.ProcessTimeStarted)
            {
                if (timing.StartTime == DateTime.MinValue ||
                    timing.TotalSeconds <= 0)
                {
                    return null;
                }

                double elapsedSeconds =
                    (DateTime.Now - timing.StartTime).TotalSeconds;

                // PC 시간이 뒤로 변경된 경우를 방어한다.
                if (elapsedSeconds < 0)
                    elapsedSeconds = 0;

                double remaining =
                    timing.TotalSeconds - elapsedSeconds;

                if (remaining <= 0)
                    return 0;

                // 로봇에 실제보다 짧은 값을 보내지 않도록 올림
                return (int)Math.Ceiling(remaining);
            }

            // ProcessTimeStart가 false인데 Process_Idx가 70 이상이면
            // 실제 레시피는 끝났고 샘플 배출/후처리 중이다.
            if (timing.ProcessIndex >= 70 &&
                timing.ProcessIndex < 900)
            {
                return 0;
            }

            // Process_Idx 0~11:
            // 로드락 준비 및 샘플 이동 중
            //
            // Process_Idx 900:
            // 정지 또는 오류 처리 중
            return null;
        }

        private static bool TryReadUShortArrayValue(
            string fieldName,
            int number1Based,
            out ushort raw,
            out double value,
            out int arrayLength,
            out string error)
        {
            raw = 0;
            value = 0;
            arrayLength = 0;
            error = "";

            try
            {
                var analogType = FindTypeInCurrentAppDomain("ALD.Analog_Comm");
                if (analogType == null)
                {
                    error = "ALD.Analog_Comm type not found";
                    return false;
                }

                var field = analogType.GetField(
                    fieldName,
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (field == null)
                {
                    error = $"ALD.Analog_Comm.{fieldName} field not found";
                    return false;
                }

                if (field.GetValue(null) is not Array arr)
                {
                    error = $"ALD.Analog_Comm.{fieldName} is not array";
                    return false;
                }

                arrayLength = arr.Length;

                if (number1Based < 1 || number1Based > arr.Length)
                {
                    error = $"{fieldName} number out of range. valid range: 1~{arr.Length}";
                    return false;
                }

                object? item = arr.GetValue(number1Based - 1);
                if (item == null)
                {
                    error = $"{fieldName}[{number1Based - 1}] is null";
                    return false;
                }

                raw = Convert.ToUInt16(item);
                value = raw / 10.0;
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// GET_TEMPERATURE와 START_ALD_PREHEAT가 공통으로 사용하는 온도 읽기 함수.
        /// tcType 1 = M-TC, tcType 2 = C-TC.
        /// </summary>
        private static TemperatureReadResult ReadTemperatureInternal(int tcType, int tcNo)
        {
            string fieldName;
            string tcName;

            if (tcType == 1)
            {
                fieldName = "TC_Read";
                tcName = "M-TC";
            }
            else if (tcType == 2)
            {
                fieldName = "heater_Read";
                tcName = "C-TC";
            }
            else
            {
                throw new InvalidOperationException("invalid tc_type. 1=M-TC, 2=C-TC");
            }

            bool modbusReady = GetModbusCheckFlag();

            if (!TryReadUShortArrayValue(fieldName, tcNo, out ushort raw, out double value, out int arrayLength, out string err))
                throw new InvalidOperationException(err);

            return new TemperatureReadResult
            {
                ModbusReady = modbusReady,
                TcType = tcType,
                TcName = tcName,
                TcNo = tcNo,
                Index = tcNo - 1,
                ArrayLength = arrayLength,
                Raw = raw,
                Value = value
            };
        }

        private static string HandleGetTemperature(string requestIdRaw, JsonElement root)
        {
            if (!root.TryGetProperty("data", out var data))
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", "data not provided");
            }

            if (!data.TryGetProperty("tc_type", out var tcTypeEl))
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", "tc_type not provided. 1=M-TC, 2=C-TC");
            }

            if (!data.TryGetProperty("tc_no", out var tcNoEl))
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", "tc_no not provided");
            }

            if (!tcTypeEl.TryGetInt32(out int tcType))
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", "tc_type must be number. 1=M-TC, 2=C-TC");
            }

            if (!tcNoEl.TryGetInt32(out int tcNo))
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", "tc_no must be number");
            }

            string fieldName;
            string tcName;

            if (tcType == 1)
            {
                // M-TC = Analog_Comm.TC_Read[]
                fieldName = "TC_Read";
                tcName = "M-TC";
            }
            else if (tcType == 2)
            {
                // C-TC = Analog_Comm.heater_Read[]
                fieldName = "heater_Read";
                tcName = "C-TC";
            }
            else
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", "invalid tc_type. 1=M-TC, 2=C-TC");
            }

            bool modbusReady = GetModbusCheckFlag();

            if (!TryReadUShortArrayValue(fieldName, tcNo, out ushort raw, out double value, out int arrayLength, out string err))
            {
                return BuildErrorResponse(requestIdRaw, "GET_TEMPERATURE", err);
            }

            var resp = new JsonObject
            {
                ["request_id"] = JsonNode.Parse(requestIdRaw),
                ["command"] = "GET_TEMPERATURE_RESULT",
                ["data"] = new JsonObject
                {
                    ["result"] = "success",
                    ["message"] = "OK",
                    ["modbus_ready"] = modbusReady,
                    ["tc_type"] = tcType,
                    ["tc_name"] = tcName,
                    ["tc_no"] = tcNo,
                    ["index"] = tcNo - 1,
                    ["array_length"] = arrayLength,
                    ["raw"] = raw,
                    ["value"] = value,
                    ["unit"] = "degC"
                }
            };

            return resp.ToJsonString();
        }

        /// <summary>
        /// SET_HEATER_SV와 START_ALD_PREHEAT가 공통으로 사용하는 Heater SV 쓰기 함수.
        /// 내부적으로 ALD.Analog_Comm.heater_Write[index], index_Heater, write_PLC를 조작한다.
        /// </summary>
        private static HeaterSvWriteResult SetHeaterSvInternal(int svNo, double svValue, bool blockWhenProcessRunning)
        {
            if (svNo < 1 || svNo > 12)
                throw new InvalidOperationException("sv_no out of range. valid range: 1~12");

            // Form_DataInsert.cs 기준 Heater SV 허용 범위는 0~400도
            if (svValue < 0 || svValue > 400)
                throw new InvalidOperationException("value out of range. valid range: 0~400 degC");

            bool modbusReady = GetModbusCheckFlag();
            if (!modbusReady)
                throw new InvalidOperationException("Modbus not started");

            // 메인 UI도 공정 중에는 SV 입력을 막는 구조이므로 DLL에서도 기본적으로 막는다.
            if (blockWhenProcessRunning && GetProcessBoolFlag())
                throw new InvalidOperationException("ALD process is running. SV change is blocked");

            var analogType = FindTypeInCurrentAppDomain("ALD.Analog_Comm");
            if (analogType == null)
                throw new InvalidOperationException("ALD.Analog_Comm type not found");

            var heaterWriteField = analogType.GetField(
                "heater_Write",
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

            var indexHeaterField = analogType.GetField(
                "index_Heater",
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

            var writePlcField = analogType.GetField(
                "write_PLC",
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

            if (heaterWriteField == null)
                throw new InvalidOperationException("ALD.Analog_Comm.heater_Write field not found");

            if (indexHeaterField == null)
                throw new InvalidOperationException("ALD.Analog_Comm.index_Heater field not found");

            if (writePlcField == null)
                throw new InvalidOperationException("ALD.Analog_Comm.write_PLC field not found");

            if (heaterWriteField.GetValue(null) is not Array heaterWrite)
                throw new InvalidOperationException("ALD.Analog_Comm.heater_Write is not array");

            int index = svNo - 1;

            if (index < 0 || index >= heaterWrite.Length)
                throw new InvalidOperationException($"sv_no out of range. valid range: 1~{heaterWrite.Length}");

            ushort raw = Convert.ToUInt16(Math.Round(svValue * 10.0));

            lock (_lock)
            {
                // Form_DataInsert.cs와 같은 방식:
                // Analog_Comm.heater_Write[Tag - 10] = value * 10;
                // Analog_Comm.index_Heater = Tag - 10;
                // Analog_Comm.write_PLC = true;
                heaterWrite.SetValue(raw, index);
                indexHeaterField.SetValue(null, index);
                writePlcField.SetValue(null, true);
            }

            return new HeaterSvWriteResult
            {
                ModbusReady = modbusReady,
                SvNo = svNo,
                Index = index,
                Value = svValue,
                Raw = raw
            };
        }

        private static string HandleSetHeaterSv(string requestIdRaw, JsonElement root)
        {
            if (!root.TryGetProperty("data", out var data))
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "data not provided");
            }

            if (!data.TryGetProperty("sv_no", out var svNoEl))
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "sv_no not provided");
            }

            if (!data.TryGetProperty("value", out var valueEl))
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "value not provided");
            }

            if (!svNoEl.TryGetInt32(out int svNo))
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "sv_no must be number");
            }

            if (!valueEl.TryGetDouble(out double svValue))
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "value must be number");
            }

            if (svNo < 1 || svNo > 12)
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "sv_no out of range. valid range: 1~12");
            }

            // Form_DataInsert.cs 기준 Heater SV 허용 범위는 0~400도
            if (svValue < 0 || svValue > 400)
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "value out of range. valid range: 0~400 degC");
            }

            bool modbusReady = GetModbusCheckFlag();
            if (!modbusReady)
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "Modbus not started");
            }

            // 메인 UI도 공정 중에는 SV 입력을 막는 구조이므로 DLL에서도 기본적으로 막는다.
            // 테스트는 반드시 공정 정지 상태에서 진행하는 것이 안전하다.
            if (GetProcessBoolFlag())
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "ALD process is running. SV change is blocked");
            }

            try
            {
                var analogType = FindTypeInCurrentAppDomain("ALD.Analog_Comm");
                if (analogType == null)
                {
                    return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "ALD.Analog_Comm type not found");
                }

                var heaterWriteField = analogType.GetField(
                    "heater_Write",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                var indexHeaterField = analogType.GetField(
                    "index_Heater",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                var writePlcField = analogType.GetField(
                    "write_PLC",
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                if (heaterWriteField == null)
                {
                    return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "ALD.Analog_Comm.heater_Write field not found");
                }

                if (indexHeaterField == null)
                {
                    return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "ALD.Analog_Comm.index_Heater field not found");
                }

                if (writePlcField == null)
                {
                    return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "ALD.Analog_Comm.write_PLC field not found");
                }

                if (heaterWriteField.GetValue(null) is not Array heaterWrite)
                {
                    return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", "ALD.Analog_Comm.heater_Write is not array");
                }

                int index = svNo - 1;

                if (index < 0 || index >= heaterWrite.Length)
                {
                    return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV",
                        $"sv_no out of range. valid range: 1~{heaterWrite.Length}");
                }

                ushort raw = Convert.ToUInt16(Math.Round(svValue * 10.0));

                lock (_lock)
                {
                    // Form_DataInsert.cs와 같은 방식:
                    // Analog_Comm.heater_Write[Tag - 10] = value * 10;
                    // Analog_Comm.index_Heater = Tag - 10;
                    // Analog_Comm.write_PLC = true;
                    heaterWrite.SetValue(raw, index);
                    indexHeaterField.SetValue(null, index);
                    writePlcField.SetValue(null, true);
                }

                var resp = new JsonObject
                {
                    ["request_id"] = JsonNode.Parse(requestIdRaw),
                    ["command"] = "SET_HEATER_SV_RESULT",
                    ["data"] = new JsonObject
                    {
                        ["result"] = "success",
                        ["message"] = "SV write requested. Check readback after 0.5~2 seconds",
                        ["modbus_ready"] = modbusReady,
                        ["sv_no"] = svNo,
                        ["index"] = index,
                        ["value"] = svValue,
                        ["raw"] = raw,
                        ["unit"] = "degC"
                    }
                };

                return resp.ToJsonString();
            }
            catch (Exception ex)
            {
                return BuildErrorResponse(requestIdRaw, "SET_HEATER_SV", ex.Message);
            }
        }


        // ==== 응답 JSON 빌더 ====

        private static string BuildStartResult(
            string requestIdRaw,
            string result,
            string message,
            int? minTotalS)
        {
            var data = new JsonObject
            {
                ["result"] = result,
                ["message"] = message,
                ["min_total_s"] = minTotalS.HasValue
                    ? JsonValue.Create(minTotalS.Value)
                    : null
            };

            var root = new JsonObject
            {
                ["request_id"] = JsonNode.Parse(requestIdRaw),
                ["command"] = "START_ALD_RESULT",
                ["data"] = data
            };

            return root.ToJsonString();
        }

        private static string BuildErrorResponse(string requestIdRaw, string command, string message)
        {
            var root = new JsonObject
            {
                ["request_id"] = JsonNode.Parse(requestIdRaw),
                ["command"] = command + "_RESULT",
                ["data"] = new JsonObject
                {
                    ["result"] = "fail",
                    ["message"] = message
                }
            };
            return root.ToJsonString();
        }
    }
}
