using System.Text.Json;
using Xunit;

namespace ALD.External.Tests
{
    // 10) HandleJsonWithHostLog: START 계열만 기록, 응답 JSON 은 기존과 동일
    public class AldModuleHostLogTests : IDisposable
    {
        private readonly TempDirs _d = new();

        public AldModuleHostLogTests()
        {
            AldModule.ConfigureHostLog(_d.Nas, _d.Local);
            AldModule.HostLog.RetryIntervalMs = 200;
            AldModule.HostLog.EnsureWorker();
        }

        public void Dispose()
        {
            AldModule.ConfigureHostLog(null, null);
            _d.Dispose();
        }

        private static string Norm(string json)
            => JsonSerializer.Serialize(JsonDocument.Parse(json).RootElement);

        [Fact]
        public void Get_Status_Is_Not_Logged_And_Response_Unchanged()
        {
            string req = "{\"request_id\":\"s1\",\"command\":\"GET_ALD_STATUS\"}";
            string resp = AldModule.HandleJsonWithHostLog(req, DateTime.Now, "10.0.0.1:1");

            // 메인 프로그램이 없으므로 기존 코드는 상태 읽기 실패 응답을 낸다. 형식은 그대로.
            using var doc = JsonDocument.Parse(resp);
            Assert.Equal("s1", doc.RootElement.GetProperty("request_id").GetString());
            Assert.Equal("GET_ALD_STATUS_RESULT", doc.RootElement.GetProperty("command").GetString());
            Assert.Equal("fail", doc.RootElement.GetProperty("data").GetProperty("result").GetString());

            Thread.Sleep(500);
            Assert.False(File.Exists(AldModule.HostLog.PendingPath));
            Assert.Empty(Directory.GetFiles(_d.Nas, "Robot_*.csv"));
            Assert.Empty(Directory.GetFiles(_d.Local, "Robot_*.csv"));
        }

        [Fact]
        public void Start_Ald_Without_Csv_Path_Is_Rejected_Once_With_Same_Response()
        {
            string req = "{\"request_id\":123,\"command\":\"START_ALD\",\"data\":{}}";
            var recv = new DateTime(2026, 9, 18, 17, 30, 15, 250);
            string resp = AldModule.HandleJsonWithHostLog(req, recv, "10.0.0.2:4444");

            // 기존 HandleJson 과 동일한 응답
            string expected = "{\"request_id\":123,\"command\":\"START_ALD_RESULT\",\"data\":{\"result\":\"fail\",\"message\":\"csv_path not provided\"}}";
            Assert.Equal(Norm(expected), Norm(resp));

            string nas = Path.Combine(_d.Nas, "Robot_20260918.csv");
            Assert.True(TestUtil.WaitUntil(() => File.Exists(nas) && TestUtil.PendingCount(AldModule.HostLog) == 0));

            var lines = TestUtil.ReadLines(nas);
            Assert.Equal(2, lines.Length);
            var f = TestUtil.ParseCsv(lines[1]);
            Assert.Equal("ALD", f[0]);
            Assert.Equal("17:30:15", f[1]);
            Assert.Equal("", f[2]);
            Assert.Equal("거절", f[5]);
            Assert.Equal("csv_path not provided", f[6]);
            Assert.Equal("123", f[10]);                    // 숫자 request_id 는 원문
            Assert.Equal("10.0.0.2:4444", f[11]);
            Assert.Equal("ald", f[13]);
            Assert.Equal("ald-20260918173015250-123", f[14]);
        }

        [Fact]
        public void Start_Ald_With_Missing_Csv_File_Records_Recipe_Name()
        {
            string csv = Path.Combine(_d.Root, "my_recipe.csv").Replace("\\", "\\\\");
            string req = "{\"request_id\":\"r9\",\"command\":\"START_ALD\",\"data\":{\"csv_path\":\"" + csv + "\"}}";
            string resp = AldModule.HandleJsonWithHostLog(req, DateTime.Now, "10.0.0.3:1");

            using var doc = JsonDocument.Parse(resp);
            Assert.Equal("fail", doc.RootElement.GetProperty("data").GetProperty("result").GetString());
            string msg = doc.RootElement.GetProperty("data").GetProperty("message").GetString() ?? "";

            string nas = Path.Combine(_d.Nas, $"Robot_{DateTime.Now:yyyyMMdd}.csv");
            Assert.True(TestUtil.WaitUntil(() => File.Exists(nas) && TestUtil.PendingCount(AldModule.HostLog) == 0));
            var f = TestUtil.ParseCsv(TestUtil.ReadLines(nas)[1]);
            Assert.Equal("거절", f[5]);
            Assert.Equal(HostProcessLog.OneLine(msg), f[6]);
            Assert.Equal("my_recipe.csv", f[8]);
        }

        [Fact]
        public void Start_Preheat_Without_Data_Is_Rejected()
        {
            string req = "{\"request_id\":\"p1\",\"command\":\"START_ALD_PREHEAT\"}";
            string resp = AldModule.HandleJsonWithHostLog(req, DateTime.Now, "10.0.0.4:1");
            using var doc = JsonDocument.Parse(resp);
            Assert.Equal("START_ALD_PREHEAT_RESULT", doc.RootElement.GetProperty("command").GetString());

            string nas = Path.Combine(_d.Nas, $"Robot_{DateTime.Now:yyyyMMdd}.csv");
            Assert.True(TestUtil.WaitUntil(() => File.Exists(nas) && TestUtil.PendingCount(AldModule.HostLog) == 0));
            var f = TestUtil.ParseCsv(TestUtil.ReadLines(nas)[1]);
            Assert.Equal("거절", f[5]);
            Assert.Equal("p1", f[10]);
        }

        [Fact]
        public void Invalid_Json_Throws_Like_Before_And_Logs_Nothing()
        {
            Assert.ThrowsAny<JsonException>(() => AldModule.HandleJsonWithHostLog("not json", DateTime.Now, "x"));
            Assert.False(File.Exists(AldModule.HostLog.PendingPath));
        }
    }
}
