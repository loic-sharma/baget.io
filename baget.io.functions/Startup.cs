extern alias AzureStorageCommon;

using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.WindowsAzure.Storage;

[assembly: FunctionsStartup(typeof(baget.io.functions.Startup))]

namespace baget.io.functions
{


    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            
            throw new System.NotImplementedException();
        }

        /*
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
                    "DefaultEndpointsProtocol=https;AccountName=baget002;AccountKey=u9N1HdlVB++9Hg5LEhXHMfPQ5L56YiRPVdgaETi6rGj1cXu7vCOUaWhlD28khgQh8ixGprtzXV9H6/YaIj6qiA==;EndpointSuffix=core.windows.net");
                var tableClient = account.CreateCloudTableClient();

                var logger = provider.GetRequiredService<ILogger<TablePackageService>>();

                return new TablePackageService(tableClient, logger);
            });

            builder.Services.AddSingleton(provider =>
            {
                // TODO: Config
                var account = CloudStorageAccount.Parse(
                    "DefaultEndpointsProtocol=https;AccountName=baget002;AccountKey=u9N1HdlVB++9Hg5LEhXHMfPQ5L56YiRPVdgaETi6rGj1cXu7vCOUaWhlD28khgQh8ixGprtzXV9H6/YaIj6qiA==;EndpointSuffix=core.windows.net");
                var queueClient = account.CreateCloudQueueClient();

                return queueClient.GetQueueReference("catalog-leafs");
            });
            builder.Services.AddSingleton(provider =>
            {
                var account = CloudStorageAccount.Parse(
                    "DefaultEndpointsProtocol=https;AccountName=baget002;AccountKey=u9N1HdlVB++9Hg5LEhXHMfPQ5L56YiRPVdgaETi6rGj1cXu7vCOUaWhlD28khgQh8ixGprtzXV9H6/YaIj6qiA==;EndpointSuffix=core.windows.net");

                return account.CreateCloudBlobClient();
            });

            builder.Services.AddSingleton<ProcessCatalogLeafItem>();
            
            builder.Services.AddSingleton<ICatalogLeafItemProcessor>(provider =>
            {
                // TODO: Config
                return provider.GetRequiredService<ProcessCatalogLeafItem>();
                //return provider.GetRequiredService<QueueCatalogLeafItem>();
            });*/
    }
}
