using System;
using System.Collections.Generic;

namespace Sorter.Core
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
                int numberFirstValue = int.Parse(first[1]);
                int numberSecondValue = int.Parse(second[1]);
                result = numberFirstValue.CompareTo(numberSecondValue);
            }

            return result;
        }
    }
}