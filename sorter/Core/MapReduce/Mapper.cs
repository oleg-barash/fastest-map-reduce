using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sorter.Core.MapReduce
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
        
        private const int _500_MiB = 524_288_000;
        private const int _300_MiB = 314_572_800;
        private const int _100_MiB = 104_857_600;
        private const int _10_MiB = 10_485_760;
        private const int BatchSize = 1000000;
        private static ConcurrentDictionary<string, object> _lockObjects = new ConcurrentDictionary<string, object>();
        private static ConcurrentDictionary<string, ChunkInfo> _writers = new ConcurrentDictionary<string, ChunkInfo>();

        public Mapper(ILineProcessor lineProcessor, string dataDirectory, int maxFileSize = _300_MiB)
        {
            _lineProcessor = lineProcessor;
            _maxFileSize = maxFileSize;
            _generateFileName = s => Path.Combine(dataDirectory, "map_" + s + ".txt");
            Directory.Delete(dataDirectory, true);
            Directory.CreateDirectory(dataDirectory);
        }

        public async Task Run(string source, int keyLength = 1)
        {
            try
            {
                var reader = File.OpenText(source);
                List<Task> resultTasks = new List<Task>();
                await Task.Run(() =>
                {
                    char[] temp = new char[BatchSize];
                    do
                    {
                        var count = reader.Read(temp, 0, BatchSize);
                        StringBuilder result = new StringBuilder(new string(temp.Take(count).ToArray()));
                        if (reader.EndOfStream)
                        {
                            resultTasks.Add(ProcessBatch(keyLength, result.ToString()));
                            break;
                        }
                        var tail = reader.ReadLine();
                        result.Append(tail);
                        resultTasks.Add(ProcessBatch(keyLength, result.ToString()));

                    } while (!reader.EndOfStream);
                });

                await Task.WhenAll(resultTasks);
                reader.Close();
                reader.Dispose();
            }
            finally
            {
                foreach (var writer in _writers)
                {
                    writer.Value.StreamWriter.Close();
                    writer.Value.StreamWriter.Dispose();
                }
                _writers.Clear();
            }

            await Task.WhenAll(_writers.Select(async s =>
            {
                string file = _generateFileName(s.Key);
                var info = new FileInfo(file);
                if (info.Length > _maxFileSize)
                {
                    await Run(file, keyLength + 2);
                    File.Delete(file);
                }
            }));
        }

        private async Task ProcessBatch(int keyLength, string data)
        {
            await Task.Run(() =>
            {
                foreach (var line in data.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (string.IsNullOrEmpty(line)) continue;
                    string[] items = _lineProcessor.Parse(line);
                    string trimmedValue = items[1].Replace("\r", string.Empty);
                    string key = trimmedValue.Length > keyLength ? trimmedValue.Substring(0, keyLength) : trimmedValue;
                    object currentStreamLock = _lockObjects.GetOrAdd(key, new object());

                    lock (currentStreamLock)
                    {
                        ChunkInfo currentStream = _writers.GetOrAdd(key, (val) =>
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
                        currentStream.Buffer.Append(line);
                        currentStream.Buffer.Append(Environment.NewLine);
                    }
                }

                foreach (var writer in _writers)
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