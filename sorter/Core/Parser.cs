using System;
using System.Text.RegularExpressions;

namespace Sorter.Core
{
    public interface ILineProcessor
    {
        string[] Parse(string sourceLine);
        string Combine(string[] sourceItems);
    }
    
    public abstract class LineProcessor : ILineProcessor
    {
        protected readonly string Delimiter;

        protected LineProcessor(string delimiter)
        {
            Delimiter = delimiter;
        }

        public abstract string[] Parse(string sourceLine);

        public string Combine(string[] sourceItems)
        {
            return String.Join(Delimiter, sourceItems);
        }
    }
    
    public class SplitLineProcessor : LineProcessor
    {
        public SplitLineProcessor(string delimiter) : base(delimiter) { }

        public override string[] Parse(string sourceLine)
        {
            return sourceLine.Split(Delimiter);
        }
    }
    public class RegexLineProcessor : LineProcessor
    {
        private static Regex _linePattern;
        public RegexLineProcessor(string delimiter) : base(delimiter)
        {
            _linePattern = new Regex(@$"(\d+){delimiter}(.*)$", RegexOptions.Compiled);
        }

        public override string[] Parse(string sourceLine)
        {
            var groups = _linePattern.Match(sourceLine).Groups;
            return new[] { groups[1].Value, groups[2].Value };
        }
    }
}