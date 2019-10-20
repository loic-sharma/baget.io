using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.Threading;
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

            rootCommand.Handler = ShowHelp(rootCommand);
            rootCommand.Add(V3Command());
            rootCommand.Add(SearchCommand());

            Command V3Command()
            {
                var command = new Command("v3", "Description");

                command.Handler = ShowHelp(command);
                command.Add(RebuildCommand());
                command.Add(UpdateCommand());

                return command;

                Command RebuildCommand()
                {
                    var createCommand = new Command("rebuild", "Rebuild the generated V3 resources");

                    createCommand.AddOption(new Option("--enqueue", "Add work to queue instead of processing directly"));
                    createCommand.Handler = RunCommand<RebuildV3Command>();

                    return createCommand;
                }

                Command UpdateCommand()
                {
                    var updateCommand = new Command("update", "Update the generated V3 resources");

                    updateCommand.AddOption(new Option("--enqueue", "Add work to queue instead of processing directly"));
                    updateCommand.Handler = RunCommand<UpdateV3Command>();

                    return updateCommand;
                }
            }

            Command SearchCommand()
            {
                var command = new Command("search", "Manage BaGet's Azure Search resource");

                command.Handler = ShowHelp(command);
                command.Add(CreateCommand());
                command.Add(RebuildCommand());

                return command;

                Command CreateCommand()
                {
                    var createCommand = new Command("create", "Create the Azure Search resource");

                    createCommand.Handler = RunCommand<CreateSearchCommand>();

                    return createCommand;
                }

                Command RebuildCommand()
                {
                    var rebuildCommand = new Command("rebuild", "Replace the Azure Search index with latest data");

                    rebuildCommand.Handler = RunCommand<RebuildSearchCommand>();

                    return rebuildCommand;
                }
            }

            ICommandHandler ShowHelp(Command command)
            {
                return CommandHandler.Create<IHelpBuilder>(help => help.Write(command));
            }

            ICommandHandler RunCommand<TCommand>()
                where TCommand : ICommand
            {
                return CommandHandler.Create<IHost, CancellationToken>(async (host, cancellationToken) =>
                {
                    var command = host.Services.GetRequiredService<TCommand>();

                    await command.RunAsync(cancellationToken);
                });
            }

            return rootCommand;
        }
    }
}
