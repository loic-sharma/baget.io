using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BaGet
{
    public static class CommandLine
    {
        public static CommandLineBuilder Create()
        {
            return new CommandLineBuilder(BuildRootCommand());
        }

        private static RootCommand BuildRootCommand()
        {
            var rootCommand = new RootCommand();

            rootCommand.Handler = CommandHandler.Create<IHelpBuilder>(
                help => help.Write(rootCommand));

            rootCommand.Add(CreateSearchIndexCommand());
            rootCommand.Add(ImportCommand());
            rootCommand.Add(RebuildCommand());

            return rootCommand;

            Command CreateSearchIndexCommand()
            {
                var command = new Command("create-search-index", "Create an Azure Search index");

                command.Handler = CommandHandler.Create<IHost, CancellationToken>(CreateAzureSearchIndexAsync);

                return command;
            }

            Command ImportCommand()
            {
                var command = new Command("import", "Import data from NuGet.org");

                command.Handler = CommandHandler.Create<IHelpBuilder>(
                    help => help.Write(command));

                command.Add(ImportCatalogCommand());
                command.Add(ImportDownloadsCommand());

                return command;
            }

            Command ImportCatalogCommand()
            {
                var command = new Command("catalog", "Import packages from NuGet.org using the V3 Catalog resource")
                {
                    new Option("--enqueue", "Adds catalog leafs to queue instead of processing directly")
                };

                command.Handler = CommandHandler.Create<IHost, bool, CancellationToken>(ImportCatalogAsync);

                return command;
            }

            Command ImportDownloadsCommand()
            {
                var command = new Command("downloads", "Import package downloads from NuGet.org");

                command.Handler = CommandHandler.Create<IHost, CancellationToken>(ImportDownloadsAsync);

                return command;
            }

            Command RebuildCommand()
            {
                var command = new Command("rebuild", "Rebuild the generated resources")
                {
                    new Option("--enqueue", "Adds rebuild operations to queue instead of processing directly")
                };

                command.Handler = CommandHandler.Create<IHost, bool, CancellationToken>(RebuildAsync);

                return command;
            }
        }

        public static async Task CreateAzureSearchIndexAsync(IHost host, CancellationToken cancellationToken)
        {
            await host
                .Services
                .GetRequiredService<CreateAzureSearchIndexCommand>()
                .RunAsync(cancellationToken);
        }

        public static async Task ImportCatalogAsync(IHost host, bool enqueue, CancellationToken cancellationToken)
        {
            await host
                .Services
                .GetRequiredService<ImportCatalogCommand>()
                .RunAsync(cancellationToken);
        }

        public static async Task ImportDownloadsAsync(IHost host, CancellationToken cancellationToken)
        {
            await host
                .Services
                .GetRequiredService<ImportDownloadsCommand>()
                .RunAsync(cancellationToken);
        }

        public static async Task RebuildAsync(IHost host, bool enqueue, CancellationToken cancellationToken)
        {
            await host
                .Services
                .GetRequiredService<RebuildCommand>()
                .RunAsync(cancellationToken);
        }
    }
}
