using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BaGet
{
    public interface IHostedCommand
    {
        Task RunAsync(CancellationToken cancellationToken = default);
    }

    public class HostedCommandsService : IHostedService
    {
        private readonly IApplicationLifetime _host;
        private readonly IReadOnlyList<IHostedCommand> _commands;

        public HostedCommandsService(IApplicationLifetime host, IEnumerable<IHostedCommand> commands)
        {
            _host = host;
            _commands = commands.ToList();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await Task.WhenAll(
                _commands.Select(c => c.RunAsync(cancellationToken)));

            _host.StopApplication();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public static class IServiceCollectionExtensions
    {
        public static IServiceCollection AddHostedCommand<TCommand>(this IServiceCollection services)
            where TCommand : class, IHostedCommand
        {
            if (!services.Any(s => s.ServiceType == typeof(HostedCommandsService)))
            {
                services.AddHostedService<HostedCommandsService>();
            }

            services.AddSingleton<IHostedCommand, TCommand>();
            return services;
        }
    }
}
