using System.Text;
using Xunit;

// AldModule 은 정적 상태를 가지므로 테스트 클래스를 병렬로 돌리지 않는다.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace ALD.External.Tests
{
    internal sealed class TempDirs : IDisposable
    {
        public string Root { get; }
        public string Nas { get; }
        public string Local { get; }

        public TempDirs(bool createNas = true)
        {
            Root = Path.Combine(Path.GetTempPath(), "ald_hostlog_test_" + Guid.NewGuid().ToString("N"));
            Nas = Path.Combine(Root, "nas");
            Local = Path.Combine(Root, "local");
            Directory.CreateDirectory(Local);
            if (createNas) Directory.CreateDirectory(Nas);
        }

        public void Dispose()
        {
            try { Directory.Delete(Root, true); } catch { }
        }
    }

    internal static class TestUtil
    {
        public static string[] ReadLines(string path)
        {
            byte[] bytes = File.ReadAllBytes(path);
            string text = Encoding.UTF8.GetString(bytes);
            if (text.Length > 0 && text[0] == '﻿') text = text.Substring(1);
            return text.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        }

        public static bool HasBom(string path)
        {
            byte[] b = File.ReadAllBytes(path);
            return b.Length >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF;
        }

        /// <summary>RFC4180 한 줄 파싱</summary>
        public static List<string> ParseCsv(string line)
        {
            var fields = new List<string>();
            var sb = new StringBuilder();
            bool inQ = false;
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (inQ)
                {
                    if (c == '"')
                    {
                        if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
                        else inQ = false;
                    }
                    else sb.Append(c);
                }
                else
                {
                    if (c == '"') inQ = true;
                    else if (c == ',') { fields.Add(sb.ToString()); sb.Clear(); }
                    else sb.Append(c);
                }
            }
            fields.Add(sb.ToString());
            return fields;
        }

        public static int PendingCount(HostProcessLog log)
            => File.Exists(log.PendingPath) ? ReadLines(log.PendingPath).Length : 0;

        public static bool WaitUntil(Func<bool> cond, int timeoutMs = 10000, int stepMs = 50)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                if (cond()) return true;
                Thread.Sleep(stepMs);
            }
            return cond();
        }
    }
}
