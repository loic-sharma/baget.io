using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Hosting;
using System.CommandLine.Invocation;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

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
                    services.Configure<AppConfiguration>(ctx.Configuration);

                    services.AddSingleton(provider =>
                    {
                        return new HttpClient(new HttpClientHandler
                        {
                            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                        });
                    });

                    services.AddSingleton(provider =>
                    {
                        return new NuGetClientFactory(
                            provider.GetRequiredService<HttpClient>(),
                            "https://api.nuget.org/v3/index.json");
                    });

                    services.AddSingleton(provider =>
                    {
                        var config = provider.GetRequiredService<IOptionsSnapshot<AppConfiguration>>();

                        return TableStorageAccount
                            .Parse(config.Value.TableStorageConnectionString)
                            .CreateCloudTableClient();
                    });

                    services.AddSingleton(provider =>
                    {
                        var config = provider.GetRequiredService<IOptionsSnapshot<AppConfiguration>>();
                        var queueClient = CloudStorageAccount
                            .Parse(config.Value.StorageQueueConnectionString)
                            .CreateCloudQueueClient();

                        return queueClient.GetQueueReference(config.Value.StorageQueueName);
                    });

                    services.AddSingleton(provider =>
                    {
                        var config = provider.GetRequiredService<IOptionsSnapshot<AppConfiguration>>();
                        var blobClient = CloudStorageAccount
                            .Parse(config.Value.BlobStorageConnectionString)
                            .CreateCloudBlobClient();

                        return blobClient.GetContainerReference(config.Value.BlobContainerName);
                    });

                    services.AddSingleton<IPackageService, TablePackageService>();
                    services.AddSingleton<ICursor, BlobCursor>();

                    services.AddSingleton<ProcessCatalogLeafItem>();
                    services.AddSingleton<QueueCatalogLeafItem>();

                    services.AddSingleton(provider =>
                    {
                        var parseResult = provider.GetRequiredService<ParseResult>();

                        var leafProcessor = parseResult.HasOption("enqueue")
                            ? (ICatalogLeafItemProcessor)provider.GetRequiredService<QueueCatalogLeafItem>()
                            : (ICatalogLeafItemProcessor)provider.GetRequiredService<ProcessCatalogLeafItem>();

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
                        var queue = provider.GetRequiredService<CloudQueue>();
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
