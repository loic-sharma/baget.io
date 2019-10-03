using System.Net;
using System.Net.Http;
using BaGet.Azure;
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

    using Microsoft.Rest;
    using Newtonsoft.Json;
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

                // TODO https://github.com/loic-sharma/BaGet/issues/362
                return new FakeSearchIndexClient();
            });

            services.AddSingleton<IPackageService, TablePackageService>();
            services.AddSingleton<ICursor, BlobCursor>();
            services.AddSingleton<IUrlGenerator, UrlGenerator>();

            services.AddSingleton<ProcessCatalogLeafItem>();
            services.AddSingleton<QueueCatalogLeafItems>();
            services.AddSingleton<BatchQueueClient>();
            services.AddSingleton<PackageIndexer>();

            return services;
        }

        // Work around for Azure Functions silliness
        internal class FakeSearchIndexClient : ISearchIndexClient
        {
            public SearchCredentials SearchCredentials => throw new System.NotImplementedException();

            public bool UseHttpGetForQueries { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }

            public JsonSerializerSettings SerializationSettings => throw new System.NotImplementedException();

            public JsonSerializerSettings DeserializationSettings => throw new System.NotImplementedException();

            public ServiceClientCredentials Credentials => throw new System.NotImplementedException();

            public string ApiVersion => throw new System.NotImplementedException();

            public string SearchServiceName { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
            public string SearchDnsSuffix { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
            public string IndexName { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
            public string AcceptLanguage { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
            public int? LongRunningOperationRetryTimeout { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
            public bool? GenerateClientRequestId { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }

            public IDocumentsOperations Documents => throw new System.NotImplementedException();

            public void Dispose()
            {
                throw new System.NotImplementedException();
            }

            public void TargetDifferentIndex(string newIndexName)
            {
                throw new System.NotImplementedException();
            }
        }

    }
}
