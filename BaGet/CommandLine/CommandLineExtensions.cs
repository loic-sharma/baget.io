using System;
using System.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BaGet
{
    public static class CommandLineExtensions
    {
        public static IServiceCollection AddCommands(
            this IServiceCollection services,
            HostedCommandLineBuilder builder)
        {
            foreach (var configureService in builder.ServiceConfigurations)
            {
                configureService(services);
            }

            return services;
        }

        public static IServiceCollection Configure<TOptions>(
            this IServiceCollection services,
            Action<ParseResult, TOptions> configureOptions)
          where TOptions : class
        {
            services.AddSingleton<IConfigureOptions<TOptions>, ConfigureCommandOptions<TOptions>>();
            services.AddSingleton(configureOptions);

            return services;
        }

        public static Command AddOptionalArgument<TType>(
            this Command command,
            string name,
            Action<Argument> configureArgument)
          where TType : class
        {
            var argument = new Argument<TType>(name, defaultValue: null);

            configureArgument(argument);
            command.AddArgument(argument);

            return command;
        }

        public static Command AddOption(
            this Command command,
            string name,
            Action<Option> configureOption)
        {
            var option = new Option(name);

            configureOption(option);
            command.AddOption(option);

            return command;
        }
    }
}
