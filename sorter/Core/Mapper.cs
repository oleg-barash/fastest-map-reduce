using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Sorter.Core
{
    public interface IMapper
    {
        Task Run(string source, int keyLength = 1);
    }

    public class Mapper : IMapper
    {
        private readonly int _maxFileSize;
        private readonly ILineProcessor _lineProcessor;
        private readonly Func<string, string> _generateFileName;
        
        private const char Delimiter = '.';
        private const int _500_MiB = 524_288_000;
        private const int _300_MiB = 314_572_800;
        private const int _100_MiB = 104_857_600;
        private const int _10_MiB = 10_485_760;
        private const int BatchSize = 1000000;
        private static ConcurrentDictionary<string, object> _lockObjects = new ConcurrentDictionary<string, object>();


        public Mapper(ILineProcessor lineProcessor, string dataDirectory, int maxFileSize = _500_MiB)
        {
            _lineProcessor = lineProcessor;
            _maxFileSize = maxFileSize;
            _generateFileName = s => Path.Combine(dataDirectory, "map_" + s + ".txt");
            Directory.Delete(dataDirectory, true);
            Directory.CreateDirectory(dataDirectory);
        }

        public async Task Run(string source, int keyLength = 1)
        {
            await Run(source, keyLength, true);
        }

        private async Task Run(string source, int keyLength, bool reverseItems = false)
        {
            ConcurrentDictionary<string, ChunkInfo> writers
                = new ConcurrentDictionary<string, ChunkInfo>();
            try
            {
                var reader = File.OpenText(source);
                List<Task> resultTasks = new List<Task>();
                await Task.Run(() =>
                {
                    char[] temp = new char[BatchSize];
                    do
                    {
                        reader.Read(temp, 0, BatchSize);
                        StringBuilder result = new StringBuilder(new string(temp));
                        if (reader.EndOfStream) break;
                        var tail = reader.ReadLine();
                        result.Append(tail);
                        resultTasks.Add(ProcessBatch(keyLength, reverseItems, result.ToString(), writers));
                    } while (!reader.EndOfStream);
                });

                await Task.WhenAll(resultTasks);
                reader.Close();
                reader.Dispose();
            }
            finally
            {
                foreach (var writer in writers)
                {
                    writer.Value.StreamWriter.Close();
                    writer.Value.StreamWriter.Dispose();
                }
            }

            await Task.WhenAll(writers.Select(async s =>
            {
                string file = _generateFileName(s.Key);
                var info = new FileInfo(file);
                if (info.Length > _maxFileSize)
                {
                    await Run(file, keyLength + 2, false);
                    File.Delete(file);
                }
            }));
        }

        private async Task ProcessBatch(int keyLength, bool reverseItems, string data,
            ConcurrentDictionary<string, ChunkInfo> writers)
        {
            await Task.Run(() =>
            {
                foreach (var line in data.Split(Environment.NewLine))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    string key;
                    string item;
                    if (reverseItems)
                    {
                        string[] values = _lineProcessor.Parse(line);
                        if (values.Length < 2) continue;
                        string trimmedValue = values[1].Replace(" ", string.Empty);
                        key = trimmedValue.Length > keyLength
                            ? trimmedValue.Substring(0, keyLength)
                            : trimmedValue;
                        item = $"{values[1]}{Delimiter}{values[0]}";
                    }
                    else
                    {
                        // Items already swapped
                        string trimmedValue = line.Replace(" ", string.Empty)
                            .Replace("\r", string.Empty);
                        key = trimmedValue.Length > keyLength ? trimmedValue.Substring(0, keyLength) : trimmedValue;
                        item = line;
                    }

                    object currentStreamLock = _lockObjects.GetOrAdd(key, new object());

                    lock (currentStreamLock)
                    {
                        ChunkInfo currentStream = writers.GetOrAdd(key, (val) =>
                        {
                            string file = _generateFileName(val);
                            return new ChunkInfo
                            {
                                File = file,
                                Name = val,
                                Buffer = new StringBuilder(),
                                StreamWriter =
                                    new StreamWriter(File.Open(file, FileMode.OpenOrCreate))
                            };
                        });
                        currentStream.Buffer.Append(item);
                        currentStream.Buffer.Append(Environment.NewLine);
                    }
                }

                foreach (var writer in writers)
                {
                    if (writer.Value.Buffer.Length > 0)
                    {
                        var currentStreamLock = _lockObjects[writer.Key];
                        lock (currentStreamLock)
                        {
                            writer.Value.StreamWriter.Write(writer.Value.Buffer);
                            writer.Value.Buffer.Clear();
                        }
                    }
                }
            });
        }
    }
}