using System;
using System.Collections.Generic;

namespace Sorter.Core.MapReduce
{
    public class StringPairComparer : IComparer<string[]>
    {
        public int Compare(string[] first, string[] second)
        {
            if (first == null || first.Length != 2)
            {
                return (second != null && second.Length == 2) ? -1 : 0;
            }
            
            if (second == null || second.Length != 2)
            {
                return 1;
            }
            
            var result = String.CompareOrdinal(first[0], second[0]);
            if (result == 0)
            {
                IntParseFast(first[1], out int numberFirstValue);
                IntParseFast(second[1], out int numberSecondValue);
                result = numberFirstValue.CompareTo(numberSecondValue);
            }

            return result;
        }
        private bool IntParseFast(string s, out int result)
        {
            int value = 0;
            var length = s.Length;
            for (int i = 0; i < length; i++)
            {
                var c = s[i];
                if (!char.IsDigit(c))
                {
                    result = -1;
                    return false;
                }
                value = 10 * value + (c - 48);
            }
            result = value;
            return true;
        }
    }
}