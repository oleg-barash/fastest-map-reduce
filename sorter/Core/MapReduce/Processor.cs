using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Sorter.Core.MapReduce
{
    public class Processor
    {
        private readonly ILineProcessor _lineProcessor;
        private readonly StringPairComparer _comparer = new StringPairComparer();
        private readonly Func<string, string> _generateFileName;
        private readonly string _dataDirectory;
        private const int BoundedCapacity = 1500;

        private readonly ExecutionDataflowBlockOptions _dataflowOptions = new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount,
            BoundedCapacity = BoundedCapacity
        };
        private readonly DataflowLinkOptions _defaultLinkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        public Processor(ILineProcessor lineProcessor, string dataDirectory)
        {
            _dataDirectory = dataDirectory;
            _lineProcessor = lineProcessor;
            _generateFileName = s => Path.Combine(dataDirectory, s);
        }

        public async Task Run()
        {
            string[] fileNames = Directory.GetFiles(_dataDirectory).Select(Path.GetFileName).ToArray();
            await Run(fileNames);
        }

        /// <summary>
        /// Data sort stage
        /// </summary>
        /// <param name="depth">Current algorithm depth. Merge type depends on it: in memory or in file system</param>
        /// <param name="source">Current stage filenames</param>
        private async Task Run(IEnumerable<string> source)
        {
            var readFile = new TransformBlock<string, Tuple<string, IEnumerable<string>>>(
                filename =>
                {
                    string sourceFile = _generateFileName(filename);
                    var lines = File.ReadLines(sourceFile);
                    return new Tuple<string, IEnumerable<string>>(filename, lines);
                },
                _dataflowOptions
                );
            var sortFile 
                = new TransformBlock<Tuple<string, IEnumerable<string>>, Tuple<string, IEnumerable<string>>>(
                lines =>
                {
                    var sortedData = lines.Item2
                        .Select(s => _lineProcessor.Parse(s))
                        .OrderBy(x => x, _comparer)
                        .Select(s => _lineProcessor.Combine(s))
                        .ToArray();
                    return new Tuple<string, IEnumerable<string>>(lines.Item1, sortedData);
                },
                _dataflowOptions);
            var writeFile = new ActionBlock<Tuple<string, IEnumerable<string>>>(
                lines =>
                {
                    string resultFile = $"sorted_{lines.Item1}";
                    using (var file = File.CreateText(_generateFileName(resultFile)))
                    {
                        foreach (var line in lines.Item2)
                        {
                            file.WriteLine(line);
                        }
                    }
                },
                _dataflowOptions);
            readFile.LinkTo(sortFile, _defaultLinkOptions);
            sortFile.LinkTo(writeFile, _defaultLinkOptions);
            
            foreach (var file in source)
            {
                await readFile.SendAsync(file);
            }
            readFile.Complete();
            await sortFile.Completion;
            await writeFile.Completion;
            foreach (var file in source)
            {
                File.Delete(_generateFileName(file));
            }
        }
    }
}