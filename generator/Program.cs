using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NLipsum.Core;

namespace generator
{
    class Program
    {
        private static int BatchCount = 500000;
        private static int BatchSize = 600;
        static void Main(string[] args)
        {
            LipsumGenerator generator = new LipsumGenerator();
            var writer = new StreamWriter(File.Open("Data.txt", FileMode.Create));
            var words = generator.PrepareWords().Take(50).ToArray();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            Parallel.For(0, BatchCount, (int batchIndex) =>
            {
                string[] lines = new string[BatchSize];
                Random localRandom = new Random();

                Parallel.For(0, BatchSize, (index) =>
                {
                    int wordsCount = localRandom.Next(1, 3);
                    string[] sentence = new string[wordsCount];
                    for (int i = 0; i < wordsCount; i++)
                    {
                        var wordIndex = localRandom.Next(1, words.Length);
                        sentence[i] = words[wordIndex];
                    }
                    lines[index] = $"{localRandom.Next(1, short.MaxValue)}. {string.Join(" ", sentence)}";
                });
                var result = string.Join(Environment.NewLine, lines);
                lock (writer)
                {
                    writer.Write(result);
                    writer.Write(Environment.NewLine);
                }
            });
            writer.Dispose();
            stopwatch.Stop();
            Console.WriteLine($"Done! Spent time: {stopwatch.Elapsed.ToString()}");
        }
    }
}