using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace Sorter.Core
{
    public class Node
    {
        public char Key;
        public ConcurrentDictionary<int, int> ValueCount;

        public Node(char key)
        {
            Key = key;
        }

        public void AddValue(int value)
        {
            ValueCount.AddOrUpdate(value, v => 1, (v, c) => c + 1);
        }
    }
    
    public class Tree<T> : ConcurrentDictionary<T, Tree<T>>
    {
        public T Data { get; private set; }
        public ConcurrentDictionary<int, int> Occurrences = new ConcurrentDictionary<int, int>();

        public Tree(T data)
        {
            Data = data;
        }

        public void Write(StreamWriter writer, string prefix)
        {
            string value = prefix + Data;
            foreach (var occurrence in Occurrences.OrderBy(x => x.Key))
            {
                for (int i = 0; i < occurrence.Value; i++)
                {
                    writer.WriteLine($"{occurrence.Key}. {value}");
                }
            }

            foreach (var item in Values.OrderBy(x => x.Data))
            {
                item.Write(writer, value);
            }
        }
        
    }
    
    public class SortTree {
        private ConcurrentDictionary<char, Tree<char>> _data = new ConcurrentDictionary<char, Tree<char>>();
        private static string _separator = ". ";
        private const int BoundedCapacity = 15000;
        private TransformBlock<string, string[]> splitLine = new TransformBlock<string, string[]>(
            line =>
            {
                return line.Split(_separator, StringSplitOptions.RemoveEmptyEntries);
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount, 
                BoundedCapacity = BoundedCapacity
            });
        
        public void Run(string filename)
        {
            var writeToBuffer = new ActionBlock<string[][]>(lines =>
                {
                    foreach (var line in lines)
                    {
                        string text = line[1];
                        var currentNode = _data.GetOrAdd(text[0], c => new Tree<char>(c));
                        foreach (var symbol in text.Skip(1))
                        {
                            currentNode = currentNode.GetOrAdd(symbol, c => new Tree<char>(c));
                        }

                        int number = int.Parse(line[0]);
                        currentNode.Occurrences.AddOrUpdate(number, 1, (v, o) => o + 1);
                    }
                }, 
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });
            var bufferBlock = new BufferBlock<string>(
                new DataflowBlockOptions { BoundedCapacity = BoundedCapacity });
            var defaultLinkOptions = new DataflowLinkOptions { PropagateCompletion = true };
            bufferBlock.LinkTo(splitLine, defaultLinkOptions);
            var batchLinesBlock = new BatchBlock<string[]>(5000);
            splitLine.LinkTo(batchLinesBlock, defaultLinkOptions);
            batchLinesBlock.LinkTo(writeToBuffer, defaultLinkOptions);
            // Begin producing
            foreach (var line in File.ReadLines(filename))
            {
                bufferBlock.SendAsync(line).Wait();
            }

            bufferBlock.Complete();
            // End of producing

            // Wait for workers to finish their work
            writeToBuffer.Completion.Wait(); 
            using var streamWriter = new StreamWriter(File.OpenWrite("out.txt"));
            foreach (var item in _data.OrderBy(x => x.Key))
            {
                item.Value.Write(streamWriter, string.Empty);
            }
            
        }
    }
}