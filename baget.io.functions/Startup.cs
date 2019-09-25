extern alias AzureStorageCommon;
extern alias AzureStorageBlob;

using System.Net;
using System.Net.Http;
using BaGet.Azure;
using BaGet.Core;
using BaGet.Protocol;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using AzureStorageBlob.Microsoft.WindowsAzure.Storage.Blob;

[assembly: FunctionsStartup(typeof(baget.io.functions.Startup))]

namespace baget.io.functions
{
    using CloudStorageAccount = AzureStorageCommon.Microsoft.WindowsAzure.Storage.CloudStorageAccount;
    using TableStorageAccount = Microsoft.Azure.Cosmos.Table.CloudStorageAccount;

    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton(provider =>
            {
                return new HttpClient(new HttpClientHandler
                {
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                });
            });

            builder.Services.AddSingleton(provider =>
            {
                return new NuGetClientFactory(
                    provider.GetRequiredService<HttpClient>(),
                    "https://api.nuget.org/v3/index.json");
            });

            builder.Services.AddSingleton<IPackageService>(provider =>
            {
                // TODO: Config
                var account = TableStorageAccount.Parse(
                    "UseDevelopmentStorage=true");
                var tableClient = account.CreateCloudTableClient();

                var logger = provider.GetRequiredService<ILogger<TablePackageService>>();

                return new TablePackageService(tableClient, logger);
            });

            builder.Services.AddSingleton(provider =>
            {
                var account = CloudStorageAccount.Parse(
                    "UseDevelopmentStorage=true");

                return account.CreateCloudBlobClient();
            });

            builder.Services.AddSingleton<ProcessCatalogLeafItem>();
            
            builder.Services.AddSingleton<ICatalogLeafItemProcessor>(provider =>
            {
                // TODO: Config
                return provider.GetRequiredService<ProcessCatalogLeafItem>();
            });
        }
    }
}
