using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Sorter
{
    public class FileSource : IEnumerable<string>, IDisposable
    {
        private readonly StreamReader _sourceReader;
        public FileSource(string filePath)
        {
            if (!File.Exists(filePath))
            {
                throw new ArgumentException($"File \"{filePath}\" doesn't exists");
            }

            _sourceReader = File.OpenText(filePath);
        }
        public IEnumerator<string> GetEnumerator()
        {
            do
            {
                string line = _sourceReader.ReadLine();
                yield return line;
            } while (!_sourceReader.EndOfStream);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
            _sourceReader?.Dispose();
        }
    }
}