using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BaGet
{
    public class UpdatePackagesCommand : ICommand
    {
        private readonly IOptions<UpdatePackagesOptions> _options;
        private readonly ILogger<UpdatePackagesCommand> _logger;

        public UpdatePackagesCommand(
            IOptions<UpdatePackagesOptions> options,
            ILogger<UpdatePackagesCommand> logger)
        {
            _options = options;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (!_options.Value.UpdateDownloads && !_options.Value.UpdateOwners)
            {
                _logger.LogError("You must provide either the '--downloads' or '--owners' option.");
                return;
            }

            await Task.Yield();
        }
    }
}
