using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Sorter.Helpers;

namespace Sorter.Core
{
    public class Reducer
    {
        private readonly Func<string, string> _generateFileName;
        private readonly string _dataDirectory;
        private string _keysPattern = @"sorted_map_([^_]+)_?(.+)?.txt";
        public Reducer(string dataDirectory)
        {
            _dataDirectory = dataDirectory;
            _generateFileName = s => Path.Combine(dataDirectory, s);
        }
        
        public void Run()
        {
            while (Directory.GetFiles(_dataDirectory).Length > 1)
            {
                string[] source = Directory.GetFiles(_dataDirectory).Select(Path.GetFileName).ToArray();
                Parallel.ForEach(source.OrderBy(s => s).Batch(5), fileNames =>
                {
                    var fromMatches = Regex.Matches(fileNames.First(), _keysPattern);
                    var from = fromMatches.First().Groups[1];
                    var toMatches = Regex.Matches(fileNames.Last(), _keysPattern).Last();
                    var to = string.IsNullOrWhiteSpace(toMatches.Groups[2].Value) ? toMatches.Groups[1] : toMatches.Groups[2];
                    using var file = File.CreateText(_generateFileName($"sorted_map_{from.Value}_{to.Value}.txt"));
                    foreach (var name in fileNames)
                    {
                        string sourceFile = _generateFileName(name);
                        file.Write(File.ReadAllText(sourceFile));
                        File.Delete(sourceFile);
                    }
                });
            }
        }
    }
}