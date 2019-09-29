using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Hosting;
using System.CommandLine.Invocation;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Search;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BaGet
{
    using CloudStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount;
    using TableStorageAccount = Microsoft.Azure.Cosmos.Table.CloudStorageAccount;

    public class Program
    {
        public static async Task Main(string[] args)
        {
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
                    services.Configure<Configuration>(ctx.Configuration);
                    services.AddBaGet();

                    services.AddSingleton(provider =>
                    {
                        var parseResult = provider.GetRequiredService<ParseResult>();

                        ICatalogLeafItemBatchProcessor leafProcessor;
                        if (parseResult.HasOption("enqueue"))
                        {
                            leafProcessor = provider.GetRequiredService<QueueCatalogLeafItems>();
                        }
                        else
                        {
                            leafProcessor = new DefaultCatalogLeafItemBatchProcessor(
                                provider.GetRequiredService<ProcessCatalogLeafItem>());
                        }

                        var clientFactory = provider.GetRequiredService<NuGetClientFactory>();
                        var cursor = provider.GetRequiredService<ICursor>();
                        var logger = provider.GetRequiredService<ILogger<ProcessCatalogCommand>>();

                        return new ProcessCatalogCommand(
                            clientFactory,
                            leafProcessor,
                            cursor,
                            logger);
                    });

                    services.AddSingleton(provider =>
                    {
                        var queue = provider.GetRequiredService<IQueueClient>();
                        var leafProcessor = provider.GetRequiredService<ProcessCatalogLeafItem>();
                        var logger = provider.GetRequiredService<ILogger<ProcessQueueCommand>>();

                        return new ProcessQueueCommand(
                            queue,
                            leafProcessor,
                            logger);
                    });
                });
            })
            .Build();

            await parser.InvokeAsync(args);
        }
    }
}
