using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BaGet
{
    public class RebuildV3Command : ICommand
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly BatchQueueClient _queue;
        private readonly ICursor _cursor;
        private readonly IOptions<RebuildV3Options> _options;
        private readonly ILogger<RebuildV3Command> _logger;

        public RebuildV3Command(
            NuGetClientFactory clientFactory,
            BatchQueueClient queue,
            ICursor cursor,
            IOptions<RebuildV3Options> options,
            ILogger<RebuildV3Command> logger)
        {
            _clientFactory = clientFactory;
            _queue = queue;
            _cursor = cursor;
            _options = options;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (!_options.Value.Enqueue)
            {
                _logger.LogError("V3 rebuild does not support direct processing at this time, please use --enqueue");
                return;
            }

            var minCursor = DateTimeOffset.MinValue;
            var maxCursor = await _cursor.GetAsync(cancellationToken);
            if (maxCursor == null)
            {
                maxCursor = DateTimeOffset.MinValue;
            }

            _logger.LogInformation("Finding catalog leafs committed before time {Cursor}...", maxCursor);

            var catalogClient = _clientFactory.CreateCatalogClient();
            var catalogLeafItems = await catalogClient.GetLeafItemsAsync(
                minCursor,
                maxCursor.Value,
                _logger,
                cancellationToken);

            var messages = catalogLeafItems
                .Select(l => l.PackageId.ToLowerInvariant())
                .Distinct()
                .Select(ToMessage)
                .ToList();

            await _queue.SendAsync(messages, cancellationToken);
        }

        private Message ToMessage(string packageId)
        {
            return new Message
            {
                Body = Encoding.UTF8.GetBytes(packageId),
                Label = "rebuild",
                ContentType = "application/json;charset=unicode",
            };
        }
    }
}
