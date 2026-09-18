// AldRunObserver.cs
//
// 외부 요청 공정의 시작/종료를 메인 프로그램 static 필드 관측값으로 판정하는 순수 상태 기계.
// 리플렉션·파일·스레드에 의존하지 않으므로 단위 테스트가 가능하다.
// 실제 폴링(500ms)과 필드 읽기는 AldModule.HostLog.cs 가 맡는다.
using System;

namespace ALD.External
{
    internal enum AldRunEvent
    {
        None,
        Started,        // 시작 관측 (StartedAt 채워짐)
        Finished,       // 종료 관측 (Result/Reason/FinishedAt 채워짐)
        StartTimeout    // StartRequested 후 StartTimeoutSec 동안 시작 미관측
    }

    internal sealed class AldRunObserver
    {
        // ==== 판정 상수 ====
        //
        // 근거 (메인 프로그램 소스 ALD_반암\ALD\Thread_Process.cs, 2026-09-18 확인):
        //   - Process_Idx = 70 : 마지막 Main Recipe Step 이 끝났을 때 "//Process End" 주석과 함께 설정
        //                        (Thread_Process.cs 1070행, 1088행). case 70 부터는 샘플 배출·후처리이며
        //                        정상 종료는 71→…→80→100("Automation End", 1447행) → Stop() 으로 끝난다.
        //   - Process_Idx = 900: 각 단계에서 Utility.ProcessStop 또는 오류 시 설정, case 900 은
        //                        "Process Stop" 로그 후 Stop() (Thread_Process.cs 1476행).
        //   - Stop() 이 Utility.Process_Bool = false 로 내린다 (Thread_Process.cs 124~131행).
        //   - Form_Command.cs 364~370행: 시작 시 Process_Bool = true, ProcessStop = false 를 먼저 쓰고
        //                        그 다음 Process_Idx = 0 으로 초기화한다. 즉 Process_Bool 이 true 가 된 직후
        //                        아주 짧게 이전 공정의 Process_Idx(100/900) 가 보일 수 있다 → 아래 ResetSeen 가드.
        public const int IdxPostProcessStart = 70;   // 이 값 이상을 봤으면 레시피 본체는 끝까지 돈 것
        public const int IdxStopOrError = 900;       // 정지/오류 처리 단계

        /// <summary>StartRequested 후 이 시간 안에 Process_Bool==true 를 못 보면 미확인으로 닫는다.</summary>
        public const double DefaultStartTimeoutSec = 600;

        /// <summary>시작 이후 Process_Bool==false 를 이 횟수만큼 연속으로 봐야 종료로 본다.</summary>
        public const int FalseStreakForFinish = 2;

        // ==== 상태 ====

        public bool StartRequested { get; private set; }
        public DateTime? StartRequestedAt { get; private set; }
        public DateTime? StartedAt { get; private set; }
        public DateTime? FinishedAt { get; private set; }

        public bool AlarmObserved { get; private set; }
        public bool StopObserved { get; private set; }
        public int IdxMax { get; private set; } = int.MinValue;
        public bool Idx900Seen { get; private set; }

        /// <summary>시작 이후 Process_Idx &lt; 70 을 한 번이라도 봤는지(이전 공정의 잔존 값 무시용)</summary>
        public bool ResetSeen { get; private set; }

        public string Result { get; private set; } = "";
        public string Reason { get; private set; } = "";
        public bool Done { get; private set; }

        private int _falseStreak;
        private readonly double _startTimeoutSec;

        public AldRunObserver(double startTimeoutSec = DefaultStartTimeoutSec)
        {
            _startTimeoutSec = startTimeoutSec;
        }

        /// <summary>START 명령이 실제로 메인 프로그램에 전달된 시점에 호출.</summary>
        public void RequestStart(DateTime now)
        {
            if (StartRequested) return;
            StartRequested = true;
            StartRequestedAt = now;
        }

        /// <summary>
        /// 한 번의 관측값을 넣는다. 읽기 실패한 값은 null 로 넘기면 그 값은 건너뛴다.
        /// </summary>
        public AldRunEvent Feed(DateTime now, bool? processBool, int? state, bool? processStop, int? processIdx)
        {
            if (Done) return AldRunEvent.None;
            if (!StartRequested) return AldRunEvent.None;

            // 1) 시작 관측
            if (!StartedAt.HasValue)
            {
                if (processBool == true)
                {
                    StartedAt = now;
                    _falseStreak = 0;
                    Accumulate(state, processStop, processIdx);
                    return AldRunEvent.Started;
                }

                if (StartRequestedAt.HasValue &&
                    (now - StartRequestedAt.Value).TotalSeconds >= _startTimeoutSec)
                {
                    Done = true;
                    Result = "미확인";
                    Reason = $"시작 미관측({(int)_startTimeoutSec}초)";
                    return AldRunEvent.StartTimeout;
                }
                return AldRunEvent.None;
            }

            // 2) 실행 중 누적
            Accumulate(state, processStop, processIdx);

            // 3) 종료 관측: Process_Bool==false 연속 2회
            if (processBool == false)
            {
                _falseStreak++;
                if (_falseStreak >= FalseStreakForFinish)
                {
                    FinishedAt = now;
                    Done = true;
                    var (r, why) = Decide(AlarmObserved, StopObserved, IdxMaxOrZero, Idx900Seen);
                    Result = r;
                    Reason = why;
                    return AldRunEvent.Finished;
                }
            }
            else if (processBool == true)
            {
                _falseStreak = 0;
            }
            // processBool == null (읽기 실패) 는 streak 를 유지한 채 건너뛴다.

            return AldRunEvent.None;
        }

        private int IdxMaxOrZero => IdxMax == int.MinValue ? 0 : IdxMax;

        private void Accumulate(int? state, bool? processStop, int? processIdx)
        {
            if (state == -1) AlarmObserved = true;
            if (processStop == true) StopObserved = true;

            if (processIdx.HasValue)
            {
                int idx = processIdx.Value;
                if (!ResetSeen)
                {
                    // 이전 공정의 잔존 값(100/900 등)을 무시: 새 공정의 값(<70)을 본 뒤부터 누적
                    if (idx < IdxPostProcessStart) ResetSeen = true;
                    else return;
                }
                if (idx > IdxMax) IdxMax = idx;
                if (idx == IdxStopOrError) Idx900Seen = true;
            }
        }

        /// <summary>
        /// 결과 판정(우선순위 고정):
        ///   alarm → 실패 / stop → STOP / idxMax>=70 && !900 → 성공 / 그 외 → 미확인
        /// </summary>
        public static (string Result, string Reason) Decide(bool alarmObserved, bool stopObserved, int idxMax, bool idx900Seen)
        {
            if (alarmObserved)
                return ("실패", "알람 관측(State=-1)");

            if (stopObserved)
                return ("STOP", "ProcessStop 관측(사용자/외부 정지)");

            if (idxMax >= IdxPostProcessStart && !idx900Seen)
                return ("성공", "");

            return ("미확인", $"정상 완료 신호 미확인 (Process_Idx max={idxMax}, 900={idx900Seen})");
        }
    }
}
