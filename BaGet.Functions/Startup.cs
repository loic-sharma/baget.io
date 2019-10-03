using System;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

[assembly: FunctionsStartup(typeof(BaGet.Functions.Startup))]

namespace BaGet.Functions
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.Configure<Configuration>(config =>
            {
                config.RootUrl = Config("RootUrl");

                config.BlobStorage = new BlobStorageConfiguration
                {
                    ConnectionString = Config("BlobStorageConnectionString"),
                    ContainerName = Config("BlobContainerName")
                };

                config.TableStorage = new ConnectionStringConfiguration
                {
                    ConnectionString = Config("TableStorageConnectionString")
                };
            });

            builder.Services.AddBaGet();
        }

        private static string Config(string name)
        {
            var value = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            return value;
        }
    }
}
