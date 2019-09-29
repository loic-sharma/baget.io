using System.IO;
using System.Net;
using System.Net.Http;
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
            var host = new HostBuilder()
                .UseConsoleLifetime()
                .ConfigureAppConfiguration((ctx, config) =>
                {
                    config.SetBasePath(Directory.GetCurrentDirectory());
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                })
                .ConfigureLogging((ctx, logging) =>
                {
                    logging.AddConsole();
                })
                .ConfigureServices((ctx, services) =>
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

                    services.AddSingleton<ICatalogLeafItemProcessor>(provider =>
                    {
                        // TODO: Config
                        //return provider.GetRequiredService<ProcessCatalogLeafItem>();
                        return provider.GetRequiredService<QueueCatalogLeafItem>();
                    });

                    services.AddHostedCommand<ProcessCatalogCommand>();
                    //services.AddHostedCommand<ProcessQueueCommand>();
                })
                .Build();

            await host.RunAsync();
        }
    }
}
