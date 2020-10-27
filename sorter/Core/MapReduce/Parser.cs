using System;

namespace Sorter.Core.MapReduce
{
    public interface ILineProcessor
    {
        string[] Parse(string sourceLine);
        string Combine(string[] sourceItems);
    }
    public class LineProcessor : ILineProcessor
    {
        private readonly string _delimiter;
        public LineProcessor(string delimiter)
        {
            _delimiter = delimiter;
        }

        public string[] Parse(string sourceLine)
        {
            return sourceLine.Split(_delimiter);
        }

        public string Combine(string[] sourceItems)
        {
            return String.Join(_delimiter, sourceItems);
        }
    }
}