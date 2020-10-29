using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Sorter.Core.MapReduce;

namespace Sorter.Core.CharTree
{
    public class SortTree {
        private readonly Dictionary<char, Tree<char>> _data = new Dictionary<char, Tree<char>>();
        private const int BoundedCapacity = 2000;
        private readonly int _batchSize;
        private readonly ILineProcessor _lineProcessor;
        private readonly string _outputFilename;

        private readonly ExecutionDataflowBlockOptions _executionOptions = new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount, 
            BoundedCapacity = BoundedCapacity
        };

        public SortTree(ILineProcessor lineProcessor, string outputFilename = "out.txt", int batchSize = 1000)
        {
            _lineProcessor = lineProcessor;
            _outputFilename = outputFilename;
            _batchSize = batchSize;
        }

        public async Task Run(string filename)
        {
            var splitLine = new TransformBlock<string, string[]>(
                line => _lineProcessor.Parse(line),
                _executionOptions);
            
            var writeToBuffer = new ActionBlock<string[][]>(lines =>
                {
                    foreach (var line in lines)
                    {
                        string text = line[1];
                        char firstChar = text[0];
                        if (!_data.ContainsKey(firstChar))
                        {
                            _data.Add(text[0], new Tree<char>(firstChar));
                        }
                        Tree<char> currentNode = _data[firstChar];
                        foreach (var symbol in text.Skip(1))
                        {
                            if (!currentNode.ContainsKey(symbol))
                            {
                                lock (currentNode)
                                {
                                    if (!currentNode.ContainsKey(symbol))
                                    {
                                        currentNode.Add(symbol, new Tree<char>(symbol));
                                    }
                                }
                            }

                            currentNode = currentNode[symbol];
                        }
                        
                        Interlocked.Increment(ref currentNode.Occurrences[int.Parse(line[0])]);
                    }
                }, 
                _executionOptions);
            var bufferBlock = new BufferBlock<string>(
                new DataflowBlockOptions { BoundedCapacity = BoundedCapacity });
            var defaultLinkOptions = new DataflowLinkOptions { PropagateCompletion = true };
            bufferBlock.LinkTo(splitLine, defaultLinkOptions);
            var batchLinesBlock = new BatchBlock<string[]>(_batchSize);
            splitLine.LinkTo(batchLinesBlock, defaultLinkOptions);
            batchLinesBlock.LinkTo(writeToBuffer, defaultLinkOptions);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            foreach (var line in await File.ReadAllLinesAsync(filename))
            {
                if (!string.IsNullOrWhiteSpace(line))
                {
                    await bufferBlock.SendAsync(line);
                }
            }

            bufferBlock.Complete();
            writeToBuffer.Completion.Wait(); 
            stopwatch.Stop();
            Console.WriteLine($"Data preparing took {stopwatch.Elapsed.ToString()}");
            File.Delete(_outputFilename);
            using var streamWriter = new StreamWriter(File.OpenWrite(_outputFilename));
            foreach (var item in _data.OrderBy(x => x.Key))
            {
                item.Value.Write(streamWriter, string.Empty);
            }
            
        }
    }
}