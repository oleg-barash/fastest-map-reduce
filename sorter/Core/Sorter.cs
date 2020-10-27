using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sorter.Core
{
    public class Sorter
    {
        private readonly ILineProcessor _lineProcessor;
        private readonly StringPairComparer _comparer = new StringPairComparer();
        private readonly Func<string, string> _generateFileName;
        private readonly string _dataDirectory;

        public Sorter(ILineProcessor lineProcessor, string dataDirectory)
        {
            _dataDirectory = dataDirectory;
            _lineProcessor = lineProcessor;
            _generateFileName = s => Path.Combine(dataDirectory, s);
        }

        public void Run()
        {
            string[] fileNames = Directory.GetFiles(_dataDirectory).Select(Path.GetFileName).ToArray();
            Run(fileNames);
        }

        /// <summary>
        /// Data sort stage
        /// </summary>
        /// <param name="depth">Current algorithm depth. Merge type depends on it: in memory or in file system</param>
        /// <param name="source">Current stage filenames</param>
        private void Run(IEnumerable<string> source)
        {
            Parallel.ForEach(source, fileName =>
            {
                // in memory
                //if (depth <= _reduceInMemoryLimit)
                //{
                SortInMemory(fileName, true);
                //}
                // in file system
                //else
                //{
                //    SortInFileSystem(fileNames);
                //}

            });
        }

        private string SortInFileSystem(IGrouping<string, string> fileNames)
        {
            string resultFile = $"sorted_{fileNames.Key}";
            using var file = File.CreateText(resultFile);

            var notProceededFileStreams = fileNames.Select(File.OpenText).ToList();

            Stack<Tuple<StreamReader, string[]>> sortedLines = new Stack<Tuple<StreamReader, string[]>>(
                notProceededFileStreams
                    .Select(s => new Tuple<StreamReader, string[]>(s, _lineProcessor.Parse(s.ReadLine())))
                    .OrderBy(x => x.Item2, _comparer));
            var currentMinimum = sortedLines.Pop();
            do
            {
                var nextMinimum = sortedLines.Pop();
                string currentLine;
                do
                {
                    file.WriteLine(currentMinimum.Item2);
                    currentLine = currentMinimum.Item1.ReadLine();
                    if (currentLine == null)
                    {
                        break;
                    }

                    int compareResult = _comparer.Compare(_lineProcessor.Parse(currentLine), nextMinimum.Item2);
                    if (compareResult <= 0)
                    {
                        currentMinimum =
                            new Tuple<StreamReader, string[]>(currentMinimum.Item1, _lineProcessor.Parse(currentLine));
                    }
                } while (_comparer.Compare(_lineProcessor.Parse(currentLine), nextMinimum.Item2) <= 0);

                notProceededFileStreams = notProceededFileStreams.Where(s => !s.EndOfStream).ToList();
            } while (!notProceededFileStreams.All(s => s.EndOfStream));

            return resultFile;
        }

        private void SortInMemory(string fileName, bool reverseItems = false)
        {
            string resultFile = $"sorted_{fileName}";
            string sourceFile = _generateFileName(fileName);
            using (var reader = File.OpenText(sourceFile))
            {
                var sortedPairs = reader.ReadToEnd()
                    .Split(Environment.NewLine)
                    .Select(s => _lineProcessor.Parse(s))
                    .OrderBy(x => x, _comparer)
                    .Select(s => _lineProcessor.Combine(reverseItems ? s.Reverse().ToArray() : s));
                using (var file = File.CreateText(_generateFileName(resultFile)))
                {
                    file.Write(string.Join(Environment.NewLine, sortedPairs));
                }
            }

            File.Delete(sourceFile);
        }
    }
}