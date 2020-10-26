using System.Collections.Concurrent;
using System.IO;

namespace Sorter.Core
{
    public class ChunkInfo
    {
        public string Name { get; set; }
        public string File { get; set; }
        public StreamWriter StreamWriter { get; set; }
        public ConcurrentQueue<string> Buffer { get; set; }
    }
}