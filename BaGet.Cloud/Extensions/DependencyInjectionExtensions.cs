using System;
using System.Net;
using System.Net.Http;
using BaGet.Azure;
using BaGet.Azure.Search;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Search;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BaGet
{
    using CloudStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount;
    using TableStorageAccount = Microsoft.Azure.Cosmos.Table.CloudStorageAccount;

    public static class DependencyInjectionExtensions
    {
        public static IServiceCollection AddBaGet(this IServiceCollection services)
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

            services.AddSingleton(provider =>
            {
                var config = provider.GetRequiredService<IOptions<Configuration>>();

                return TableStorageAccount
                    .Parse(config.Value.TableStorage.ConnectionString)
                    .CreateCloudTableClient();
            });

            services.AddSingleton<IQueueClient>(provider =>
            {
                var config = provider.GetRequiredService<IOptions<Configuration>>();
                var builder = new ServiceBusConnectionStringBuilder(
                    config.Value.ServiceBus.ConnectionString);

                return new QueueClient(builder, ReceiveMode.PeekLock);
            });

            services.AddSingleton(provider =>
            {
                var config = provider.GetRequiredService<IOptions<Configuration>>();
                var blobClient = CloudStorageAccount
                    .Parse(config.Value.BlobStorage.ConnectionString)
                    .CreateCloudBlobClient();

                return blobClient.GetContainerReference(config.Value.BlobStorage.ContainerName);
            });

            services.AddSingleton<ISearchIndexClient>(provider =>
            {
                var config = provider.GetRequiredService<IOptions<Configuration>>();

                return new SearchIndexClient(
                    config.Value.Search.ServiceName,
                    config.Value.Search.IndexName,
                    new SearchCredentials(config.Value.Search.ApiKey));
            });

            // TODO: Move BaGet to ISearchServiceClient and remove this line:
            services.AddSingleton<ISearchServiceClient>(p => p.GetRequiredService<SearchServiceClient>());
            services.AddSingleton(provider =>
            {
                var config = provider.GetRequiredService<IOptions<Configuration>>();

                return new SearchServiceClient(
                    config.Value.Search.ServiceName,
                    new SearchCredentials(config.Value.Search.ApiKey));
            });

            services.AddSingleton<IPackageService, TablePackageService>();
            services.AddSingleton<ICursor, BlobCursor>();
            services.AddSingleton<IUrlGenerator, UrlGenerator>();

            services.AddSingleton<ProcessCatalogLeafItem>();
            services.AddSingleton<QueueCatalogLeafItems>();
            services.AddSingleton<BatchQueueClient>();
            services.AddSingleton<PackageIndexer>();

            services.AddSingleton<AzureSearchBatchIndexer>();
            services.AddSingleton<IndexActionBuilder>();

            return services;
        }
    }
}
