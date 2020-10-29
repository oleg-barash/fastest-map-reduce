using System;
using System.Collections.Generic;

namespace Sorter.Core.MapReduce
{
    public class StringPairComparer : IComparer<string[]>
    {
        public int Compare(string[] first, string[] second)
        {
            var result = String.CompareOrdinal(first[1], second[1]);
            if (result == 0)
            {
                IntParseFast(first[0], out int firstNumberValue);
                IntParseFast(second[0], out int secondNumberValue);
                result = firstNumberValue.CompareTo(secondNumberValue);
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