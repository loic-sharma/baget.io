using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace baget.io
{
    using CloudStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount;
    using TableStorageAccount = Microsoft.Azure.Cosmos.Table.CloudStorageAccount;

    class Program
    {
        static async Task Main(string[] args)
        {
            var host = new HostBuilder()
                .UseConsoleLifetime()
                .ConfigureLogging((ctx, logging) =>
                {
                    logging.AddConsole();
                })
                .ConfigureServices(services =>
                {
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

                    services.AddSingleton<IPackageService>(provider =>
                    {
                        // TODO: Config
                        var account = TableStorageAccount.Parse(
                            "UseDevelopmentStorage=true");
                        var tableClient = account.CreateCloudTableClient();

                        var logger = provider.GetRequiredService<ILogger<TablePackageService>>();

                        return new TablePackageService(tableClient, logger);
                    });

                    services.AddSingleton(provider =>
                    {
                        // TODO: Config
                        var account = CloudStorageAccount.Parse(
                            "UseDevelopmentStorage=true");
                        var queueClient = account.CreateCloudQueueClient();

                        return queueClient.GetQueueReference("catalog-leafs");
                    });

                    services.AddSingleton(provider =>
                    {
                        var account = CloudStorageAccount.Parse(
                            "UseDevelopmentStorage=true");

                        return account.CreateCloudBlobClient();
                    });

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
