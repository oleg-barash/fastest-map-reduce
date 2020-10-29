using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Sorter.Core;
using Sorter.Core.CharTree;
using Sorter.Core.MapReduce;

namespace Sorter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args[0] == null || args.Length < 1)
            {
                throw new ArgumentNullException(nameof(args),"At least one argument needed: file path");
            }

            string sourceFile = args[0];
            Stopwatch stopwatch = new Stopwatch();
            Stopwatch totalStopwatch = new Stopwatch();
            string delimiter = ". ";
            ILineProcessor lineProcessor = new RegexLineProcessor(delimiter);
            totalStopwatch.Start();
            Console.WriteLine($"Started at { DateTime.Now.TimeOfDay.ToString() } ");
            if (args.Length > 1 && args[1] == "UseTreeAlgorithm")
            {
                SortTree sorter = new SortTree(lineProcessor);
                stopwatch.Start();
                await sorter.Run(sourceFile);
                Console.WriteLine($"Work finished in {stopwatch.Elapsed.ToString()}");
            }
            else
            {
                string workingDirectory = "Data";
                Mapper mapper = new Mapper(lineProcessor, workingDirectory);
                Console.WriteLine("File mapping started ... ");
                stopwatch.Start();
                await mapper.Run(sourceFile);
                Console.WriteLine($"Map stage took {stopwatch.Elapsed.ToString()}");

                Processor processor = new Processor(lineProcessor, workingDirectory);
                Console.WriteLine("File sorting started ... ");
                stopwatch.Restart();
                await processor.Run();
                Console.WriteLine($"Sort stage took {stopwatch.Elapsed.ToString()}");

                Reducer reducer = new Reducer(workingDirectory);
                Console.WriteLine("File merge started ... ");
                stopwatch.Restart();
                reducer.Run();
                Console.WriteLine($"Merge stage took {stopwatch.Elapsed.ToString()}");
            }

            Console.WriteLine($"Total consumed: {totalStopwatch.Elapsed.ToString()}");

        }

    }
}
