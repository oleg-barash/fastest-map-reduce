using System.IO;
using System.Text;

namespace Sorter.Core.MapReduce
{
    public class ChunkInfo
    {
        public string Name { get; set; }
        public string File { get; set; }
        public StreamWriter StreamWriter { get; set; }
        public StringBuilder Buffer { get; set; }
    }
}