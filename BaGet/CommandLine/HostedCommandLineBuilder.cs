using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BaGet
{
    public class HostedCommandLineBuilder : CommandLineBuilder
    {
        private readonly List<Action<IServiceCollection>> _serviceConfigurations;

        public HostedCommandLineBuilder()
        {
            _serviceConfigurations = new List<Action<IServiceCollection>>();
        }

        public IReadOnlyList<Action<IServiceCollection>> ServiceConfigurations => _serviceConfigurations;

        public ICommandHandler BuildHelpHandler(Command command)
        {
            return CommandHandler.Create<IHelpBuilder>(help => help.Write(command));
        }

        public ICommandHandler BuildHandler<TCommand>()
            where TCommand : class, ICommand
        {
            _serviceConfigurations.Add(services =>
            {
                services.AddSingleton<TCommand>();
            });

            return CommandHandler.Create<IHost, CancellationToken>(async (host, cancellationToken) =>
            {
                var command = host.Services.GetRequiredService<TCommand>();

                await command.RunAsync(cancellationToken);
            });
        }

        public ICommandHandler BuildHandler<TCommand, TOptions>(
            Action<ParseResult, TOptions> configureOptions)
            where TCommand : class, ICommand
            where TOptions : class
        {
            _serviceConfigurations.Add(services =>
            {
                services.AddSingleton<TCommand>();
                services.Configure(configureOptions);
            });

            return CommandHandler.Create<IHost, CancellationToken>(async (host, cancellationToken) =>
            {
                var command = host.Services.GetRequiredService<TCommand>();

                await command.RunAsync(cancellationToken);
            });
        }
    }
}
