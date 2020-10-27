using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Sorter.Core;

namespace Sorter
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args[0] == null || args.Length < 1)
            {
                throw new ArgumentNullException(nameof(args),"At least one argument needed: file path");
            }

            string workingDirectory = Path.Combine(AppContext.BaseDirectory, "data");
            
            Stopwatch stopwatch = new Stopwatch();
            Stopwatch totalStopwatch = new Stopwatch();
            totalStopwatch.Start();

            SortTree sorter = new SortTree();
            Console.WriteLine("File mapping started ... ");
            stopwatch.Start();
            sorter.Run(args[0]);
            Console.WriteLine($"Map stage took {stopwatch.Elapsed.ToString()}");
            
            // Mapper mapper = new Mapper(new LineProcessor("."), workingDirectory);
            // Console.WriteLine("File mapping started ... ");
            // stopwatch.Start();
            // await mapper.Run(args[0]);
            // Console.WriteLine($"Map stage took {stopwatch.Elapsed.ToString()}");
            //
            // Core.Sorter sorter = new Core.Sorter(new LineProcessor("."), workingDirectory);
            // Console.WriteLine("File sorting started ... ");
            // stopwatch.Restart();
            // sorter.Run();
            // Console.WriteLine($"Sort stage took {stopwatch.Elapsed.ToString()}");
            //
            // Reducer reducer = new Reducer( workingDirectory);
            // Console.WriteLine("File merge started ... ");
            // stopwatch.Restart();
            // reducer.Run();
            // Console.WriteLine($"Merge stage took {stopwatch.Elapsed.ToString()}");
            
            Console.WriteLine($"Total consumed: {totalStopwatch.Elapsed.ToString()}");

        }

    }
}
