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
using Microsoft.Extensions.Options;

namespace BaGet
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            ThreadPool.SetMinThreads(ParallelHelper.MaxDegreeOfParallelism, completionPortThreads: 4);
            ServicePointManager.DefaultConnectionLimit = ParallelHelper.MaxDegreeOfParallelism;
            ServicePointManager.MaxServicePointIdleTime = 10000;

            var appBuilder = BaGetCommand.Create();

            var parser = appBuilder
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
                        services.AddCommands(appBuilder);
                        services.Configure<Configuration>(ctx.Configuration);

                        services.AddSingleton(provider =>
                        {
                            var leafProcesor = provider.GetRequiredService<ProcessCatalogLeafItem>();

                            return new DefaultCatalogLeafItemBatchProcessor(leafProcesor);
                        });

                        services.AddSingleton<ICatalogLeafItemBatchProcessor>(provider =>
                        {
                            var options = provider.GetRequiredService<IOptions<AddPackagesOptions>>();

                            if (options.Value.Enqueue)
                            {
                                return provider.GetRequiredService<QueueCatalogLeafItems>();
                            }
                            else
                            {
                                return provider.GetRequiredService<DefaultCatalogLeafItemBatchProcessor>();
                            }
                        });
                    });
                })
                .Build();

            await parser.InvokeAsync(args);
        }
    }
}
