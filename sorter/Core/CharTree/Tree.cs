using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sorter.Core.CharTree
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
    
    public class Tree<T> : Dictionary<T, Tree<T>>
    {
        public T Data { get; private set; }
        public int[] Occurrences = new int[short.MaxValue];

        public Tree(T data)
        {
            Data = data;
        }

        public void Write(StreamWriter writer, string prefix)
        {
            string value = prefix + Data;
            for (int i = 0; i < Occurrences.Length; i++)
            {
                int currentCount = Occurrences[i];
                for (int c = 1; c <= currentCount; c++)
                {
                    writer.Write(i);
                    writer.Write(". ");
                    writer.Write(value);
                    writer.Write(Environment.NewLine);
                }
            }
            foreach (var item in Values.OrderBy(x => x.Data))
            {
                item.Write(writer, value);
            }
        }
        
    }

}