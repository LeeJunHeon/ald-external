using Xunit;

namespace ALD.External.Tests
{
    // 9) 관측 판정 단위 테스트
    public class AldRunObserverTests
    {
        [Theory]
        [InlineData(true, false, 100, false, "실패", "알람 관측(State=-1)")]
        [InlineData(true, true, 100, true, "실패", "알람 관측(State=-1)")]
        [InlineData(false, true, 100, false, "STOP", "ProcessStop 관측(사용자/외부 정지)")]
        [InlineData(false, true, 30, true, "STOP", "ProcessStop 관측(사용자/외부 정지)")]
        [InlineData(false, false, 70, false, "성공", "")]
        [InlineData(false, false, 100, false, "성공", "")]
        [InlineData(false, false, 900, true, "미확인", "정상 완료 신호 미확인 (Process_Idx max=900, 900=True)")]
        [InlineData(false, false, 40, false, "미확인", "정상 완료 신호 미확인 (Process_Idx max=40, 900=False)")]
        [InlineData(false, false, 0, false, "미확인", "정상 완료 신호 미확인 (Process_Idx max=0, 900=False)")]
        public void Decide_Table(bool alarm, bool stop, int idxMax, bool idx900, string result, string reason)
        {
            var (r, why) = AldRunObserver.Decide(alarm, stop, idxMax, idx900);
            Assert.Equal(result, r);
            Assert.Equal(reason, why);
        }

        [Fact]
        public void Nothing_Happens_Before_StartRequested()
        {
            var o = new AldRunObserver();
            var t = new DateTime(2026, 9, 18, 9, 0, 0);
            Assert.Equal(AldRunEvent.None, o.Feed(t, true, 0, false, 10));
            Assert.Null(o.StartedAt);
        }

        [Fact]
        public void Start_Then_Two_Consecutive_False_Finishes_Success()
        {
            var o = new AldRunObserver();
            var t = new DateTime(2026, 9, 18, 9, 0, 0);
            o.RequestStart(t);

            Assert.Equal(AldRunEvent.None, o.Feed(t, false, 0, false, 100));         // 아직 안 뜸(이전 잔존 100 무시)
            Assert.Equal(AldRunEvent.Started, o.Feed(t.AddSeconds(1), true, 0, false, 100)); // 잔존값은 무시
            Assert.Equal(t.AddSeconds(1), o.StartedAt);
            Assert.False(o.ResetSeen);

            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(2), true, 0, false, 0));
            Assert.True(o.ResetSeen);
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(3), true, 0, false, 40));
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(4), true, 0, false, 71));

            // false 1회 → 아직 아님, true 로 돌아오면 카운트 리셋
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(5), false, 0, false, 80));
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(6), true, 0, false, 80));
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(7), false, 0, false, 100));
            Assert.Equal(AldRunEvent.Finished, o.Feed(t.AddSeconds(8), false, 0, false, 100));

            Assert.Equal(t.AddSeconds(8), o.FinishedAt);
            Assert.Equal("성공", o.Result);
            Assert.Equal("", o.Reason);
            Assert.True(o.Done);
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(9), false, -1, true, 900)); // 종료 후 무시
        }

        [Fact]
        public void Alarm_During_Run_Yields_Fail_Even_With_Stop()
        {
            var o = new AldRunObserver();
            var t = DateTime.Now;
            o.RequestStart(t);
            o.Feed(t, true, 0, false, 0);
            o.Feed(t.AddSeconds(1), true, -1, false, 30);
            o.Feed(t.AddSeconds(2), true, 0, true, 900);
            o.Feed(t.AddSeconds(3), false, 0, false, 900);
            Assert.Equal(AldRunEvent.Finished, o.Feed(t.AddSeconds(4), false, 0, false, 900));
            Assert.Equal("실패", o.Result);
        }

        [Fact]
        public void Stop_Yields_STOP()
        {
            var o = new AldRunObserver();
            var t = DateTime.Now;
            o.RequestStart(t);
            o.Feed(t, true, 0, false, 0);
            o.Feed(t.AddSeconds(1), true, 0, true, 30);
            o.Feed(t.AddSeconds(2), false, 0, true, 900);
            Assert.Equal(AldRunEvent.Finished, o.Feed(t.AddSeconds(3), false, 0, false, 900));
            Assert.Equal("STOP", o.Result);
        }

        [Fact]
        public void Null_Reads_Are_Skipped_And_Do_Not_Break_Streak()
        {
            var o = new AldRunObserver();
            var t = DateTime.Now;
            o.RequestStart(t);
            o.Feed(t, true, null, null, null);
            o.Feed(t.AddSeconds(1), true, 0, false, 5);
            o.Feed(t.AddSeconds(2), true, 0, false, 75);
            o.Feed(t.AddSeconds(3), false, null, null, null);
            o.Feed(t.AddSeconds(4), null, null, null, null);     // 읽기 실패: streak 유지
            Assert.Equal(AldRunEvent.Finished, o.Feed(t.AddSeconds(5), false, 0, false, 100));
            Assert.Equal("성공", o.Result);
        }

        // 이전 공정의 잔존 알람/정지/Idx 는 새 공정의 Process_Idx<70 을 보기 전까지 누적하지 않는다
        [Fact]
        public void Stale_Alarm_And_Stop_Before_Reset_Are_Ignored()
        {
            var o = new AldRunObserver();
            var t = DateTime.Now;
            o.RequestStart(t);
            o.Feed(t, true, -1, true, 900);           // Process_Bool=true 직후 이전 공정 값이 보임
            Assert.False(o.AlarmObserved); Assert.False(o.StopObserved); Assert.False(o.Idx900Seen);
            o.Feed(t.AddSeconds(1), true, 0, false, 0);
            Assert.True(o.ResetSeen);
            o.Feed(t.AddSeconds(2), true, 0, false, 80);
            o.Feed(t.AddSeconds(3), false, 0, false, 100);
            Assert.Equal(AldRunEvent.Finished, o.Feed(t.AddSeconds(4), false, 0, false, 100));
            Assert.Equal("성공", o.Result);
        }

        // Process_Idx 를 읽지 못하면 ResetSeen 이 서지 않아 알람/정지도 누적되지 않고 종료 시 미확인 (의도된 동작)
        [Fact]
        public void Without_ProcessIdx_Alarm_And_Stop_Are_Not_Accumulated_And_Result_Is_Unknown()
        {
            var o = new AldRunObserver();
            var t = DateTime.Now;
            o.RequestStart(t);
            o.Feed(t, true, -1, true, null);
            o.Feed(t.AddSeconds(1), true, -1, true, null);
            Assert.False(o.ResetSeen); Assert.False(o.AlarmObserved); Assert.False(o.StopObserved);
            o.Feed(t.AddSeconds(2), false, -1, true, null);
            Assert.Equal(AldRunEvent.Finished, o.Feed(t.AddSeconds(3), false, -1, true, null));
            Assert.Equal("미확인", o.Result);
            Assert.Equal("정상 완료 신호 미확인 (Process_Idx max=0, 900=False)", o.Reason);
        }

        [Fact]
        public void Start_Timeout_600s()
        {
            var o = new AldRunObserver();
            var t = new DateTime(2026, 9, 18, 9, 0, 0);
            o.RequestStart(t);
            Assert.Equal(AldRunEvent.None, o.Feed(t.AddSeconds(599), false, 0, false, 0));
            Assert.Equal(AldRunEvent.StartTimeout, o.Feed(t.AddSeconds(600), false, 0, false, 0));
            Assert.Equal("미확인", o.Result);
            Assert.Equal("시작 미관측(600초)", o.Reason);
            Assert.True(o.Done);
            Assert.Null(o.StartedAt);
        }
    }
}
