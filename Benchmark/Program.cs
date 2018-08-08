using System;
using System.IO;
using System.Threading;
using Benchmark.Benchmark;
using PipBenchmark.Console;
using PipBenchmark.Runner;
using PipBenchmark.Runner.Config;

namespace Benchmark
{
    class Program
    {
        private static void ExecuteBatchMode(CommandLineArgs args, BenchmarkRunner runner)
        {
            try
            {
                if (args.ShowHelp)
                {
                    HelpPrinter.Print();
                    return;
                }

                // Load assemblies
                foreach (string assemblyFile in args.Assemblies)
                    runner.Benchmarks.AddSuitesFromAssembly(assemblyFile);

                // Load configuration
                if (args.ConfigurationFile != null)
                    runner.Parameters.LoadFromFile(args.ConfigurationFile);

                // Set parameters
                if (args.Parameters.Count > 0)
                    runner.Parameters.Set(args.Parameters);

                if (args.ShowBenchmarks)
                {
                    PrintBenchmarks(runner);
                    return;
                }

                if (args.ShowParameters)
                {
                    PrintParameters(runner);
                    return;
                }

                // Benchmark the environment
                if (args.BenchmarkEnvironment)
                {
                    System.Console.Out.WriteLine("Benchmarking Environment (wait up to 2 mins)...");
                    runner.Environment.Measure(true, true, true);
                    System.Console.Out.WriteLine("CPU: {0}, Video: {1}, Disk: {2}",
                        runner.Environment.CpuMeasurement.ToString("0.##"),
                        runner.Environment.VideoMeasurement.ToString("0.##"),
                        runner.Environment.DiskMeasurement.ToString("0.##")
                    );
                }

                // Configure benchmarking
                runner.Configuration.ForceContinue = true;//args.IsForceContinue;
                runner.Configuration.MeasurementType = args.MeasurementType;
                runner.Configuration.NominalRate = args.NominalRate;
                runner.Configuration.ExecutionType = args.ExecutionType;
                runner.Configuration.NumberOfThreads = 1;
                runner.Configuration.Duration = 600000;

                // Enable benchmarks
                if (args.Benchmarks.Count == 0)
                    runner.Benchmarks.SelectAll();
                else
                {
                    foreach (var benchmark in args.Benchmarks)
                        runner.Benchmarks.SelectByName(new string[] { benchmark });
                }

                // Perform benchmarking
                runner.Start();

                if (runner.Configuration.ExecutionType == ExecutionType.Proportional)
                {
                    Thread.Sleep(runner.Configuration.Duration);
                    runner.Stop();
                }

                if (runner.Results.All.Count > 0)
                {
                    System.Console.Out.WriteLine("Average Perfomance: {0}", runner.Results.All[0].PerformanceMeasurement.AverageValue);
                    System.Console.Out.WriteLine("Average Memory Usage: {0}", runner.Results.All[0].MemoryUsageMeasurement.AverageValue);
                }

                // Generate report
                if (args.ReportFile != null)
                {
                    using (FileStream stream = File.OpenWrite(args.ReportFile))
                    {
                        using (StreamWriter writer = new StreamWriter(stream))
                            writer.Write(runner.Report.Generate());
                    }
                }
            }
            catch (Exception ex)
            {
                System.Console.Out.WriteLine("Error: {0}", ex.Message);
            }
        }

        private static void PrintBenchmarks(BenchmarkRunner runner)
        {
            System.Console.Out.WriteLine("Pip.Benchmark Console Runner. (c) Conceptual Vision Consulting LLC 2018");
            System.Console.Out.WriteLine();
            System.Console.Out.WriteLine("Benchmarks:");

            foreach (var suite in runner.Suites)
            {
                foreach (var benchmark in suite.Benchmarks)
                {
                    System.Console.Out.WriteLine("{0} - {1}", benchmark.FullName, benchmark.Description);
                }
            }
        }

        private static void PrintParameters(BenchmarkRunner runner)
        {
            System.Console.Out.WriteLine("Pip.Benchmark Console Runner. (c) Conceptual Vision Consulting LLC 2018");
            System.Console.Out.WriteLine();
            System.Console.Out.WriteLine("Parameters:");

            runner.Benchmarks.SelectAll();
            foreach (var parameter in runner.Parameters.UserDefined)
            {
                var defaultValue = string.IsNullOrEmpty(parameter.DefaultValue) ? "" : " (Default: " + parameter.DefaultValue + ")";
                System.Console.Out.WriteLine("{0} - {1}{2}", parameter.Name, parameter.Description, defaultValue);
            }
        }

        static void Main(string[] args)
        {
            var runner = new BenchmarkRunner();
            CommandLineArgs processedArgs = new CommandLineArgs(args);

            var suite = new RedisBenchmarkSuite();

            runner.Benchmarks.AddSuite(suite);

            ExecuteBatchMode(processedArgs, runner);

            var report = runner.Report.Generate();
            Console.Out.WriteLine();
            Console.Out.Write(report);

            Console.Out.WriteLine("Press ENTER to exit...");
            Console.In.ReadLine();
        }
    }
}
