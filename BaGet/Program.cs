using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Hosting;
using System.CommandLine.Invocation;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            ThreadPool.SetMinThreads(ParallelHelper.MaxDegreeOfParallelism, completionPortThreads: 4);
            ServicePointManager.DefaultConnectionLimit = ParallelHelper.MaxDegreeOfParallelism;
            ServicePointManager.MaxServicePointIdleTime = 10000;

            var parser = CommandLine.Create()
                .UseDefaults()
                .UseHost(host =>
                {
                    host.ConfigureAppConfiguration((ctx, config) =>
                    {
                        config.SetBasePath(Directory.GetCurrentDirectory());
                        config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                    });

                    host.ConfigureLogging((ctx, logging) =>
                    {
                        logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning);
                        logging.AddConsole();
                    });

                    host.ConfigureServices((ctx, services) =>
                    {
                        services.AddBaGet();

                        services.Configure<Configuration>(ctx.Configuration);
                        services.AddSingleton<ProcessCatalogCommand>();
                        services.AddSingleton<RebuildCommand>();

                        services.AddSingleton<ICatalogLeafItemBatchProcessor>(provider =>
                        {
                            var parseResult = provider.GetRequiredService<ParseResult>();

                            if (parseResult.HasOption("enqueue"))
                            {
                                return provider.GetRequiredService<QueueCatalogLeafItems>();
                            }

                            var leafProcesor = provider.GetRequiredService<ProcessCatalogLeafItem>();

                            return new DefaultCatalogLeafItemBatchProcessor(leafProcesor);
                        });
                    });
                })
                .Build();

            await parser.InvokeAsync(args);
        }
    }
}
