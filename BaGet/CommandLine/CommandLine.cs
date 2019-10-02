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

            rootCommand.Handler = CommandHandler.Create((IHelpBuilder help) =>
            {
                help.Write(rootCommand);
            });

            rootCommand.Add(BuildProcessCatalogCommand());

            return rootCommand;
        }

        private static Command BuildProcessCatalogCommand()
        {
            var command = new Command("process-catalog", "Process the NuGet Catalog V3 resource")
            {
                new Option("--enqueue", "Adds catalog leafs to queue instead of processing directly")
            };

            command.Handler = CommandHandler.Create<IHost, bool, CancellationToken>(ProcessCatalogAsync);

            return command;
        }

        public static async Task ProcessCatalogAsync(IHost host, bool enqueue, CancellationToken cancellationToken)
        {
            await host
                .Services
                .GetRequiredService<ProcessCatalogCommand>()
                .RunAsync(cancellationToken);
        }
    }
}
