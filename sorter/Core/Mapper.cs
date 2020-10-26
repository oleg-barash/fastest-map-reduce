using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        private object _readLockObject = new object();
        
        private const char Delimiter = '.';
        private const int _300_MiB = 314_572_800;
        private const int _100_MiB = 104_857_600;
        private const int _10_MiB = 10_485_760;
        private const int BatchSize = 1000000;
        private ConcurrentQueue<string> _buffer = new ConcurrentQueue<string>();


        public Mapper(ILineProcessor lineProcessor, string dataDirectory, int maxFileSize = _10_MiB)
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
                FileInfo info = new FileInfo(source);
                var reader = File.OpenText(source);
                List<Task> resultTasks = new List<Task>();
                await Task.Run(() =>
                {
                    var currentPosition = 0;
                    do
                    {
                        char[] temp = new char[BatchSize];
                        reader.Read(temp, 0, currentPosition + BatchSize <= info.Length ? BatchSize : (int)info.Length - currentPosition);
                        StringBuilder result = new StringBuilder(new string(temp));
                        StringBuilder tail = new StringBuilder();
                        do
                        {
                            if (reader.EndOfStream) break;
                            tail.Append((char) reader.Read());
                        } while (!tail.ToString().Contains(Environment.NewLine));

                        result.Append(tail);
                        _buffer.Enqueue(result.ToString());
                        currentPosition += result.Length;
                        resultTasks.Add(Task.Run(() =>
                        {
                            if (!_buffer.TryDequeue(out string data)) return;
                            var items = data.Split(Environment.NewLine);
                            foreach (var line in items)
                            {
                                string key;
                                string item;
                                string[] values = _lineProcessor.Parse(line);
                                if (values.Length < 2) continue;
                                if (reverseItems)
                                {
                                    key = values[1].Length > keyLength
                                        ? values[1].Substring(0, keyLength)
                                        : values[1];
                                    item = $"{values[1]}{Delimiter}{values[0]}";
                                }
                                else
                                {
                                    key = values[0].Length > keyLength ? values[0].Substring(0, keyLength) : values[0];
                                    item = line;
                                }

                                lock (_readLockObject)
                                {
                                    ChunkInfo currentStream = writers.GetOrAdd(key, (val) =>
                                    {
                                        string file = _generateFileName(val);
                                        return new ChunkInfo
                                        {
                                            File = file,
                                            Name = val,
                                            Buffer = new ConcurrentQueue<string>(),
                                            StreamWriter = new StreamWriter(File.Open(file, FileMode.OpenOrCreate))
                                        };
                                    });
                                    currentStream.Buffer.Enqueue(item);
                                }

                            }

                            foreach (var writer in writers)
                            {
                                lock (writer.Value)
                                {
                                    while (writer.Value.Buffer.TryDequeue(out string val))
                                    {
                                        writer.Value.StreamWriter.WriteLine(val);
                                    }

                                    writer.Value.StreamWriter.Flush();
                                }
                            }
                        }));
                    } while (!reader.EndOfStream);
                });

                await Task.WhenAll(resultTasks);
            }
            finally
            {
                await Task.WhenAll(writers.Select(w => w.Value.StreamWriter.DisposeAsync().AsTask()).ToArray());
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
    }
}