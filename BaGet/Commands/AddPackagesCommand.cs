using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NuGet.Packaging.Core;

namespace BaGet
{
    public class AddPackagesCommand : ICommand
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly ICatalogLeafItemBatchProcessor _leafProcessor;
        private readonly ICursor _cursor;
        private readonly IOptions<AddPackagesOptions> _options;
        private readonly ILogger<AddPackagesCommand> _logger;

        public AddPackagesCommand(
            NuGetClientFactory clientFactory,
            ICatalogLeafItemBatchProcessor leafProcessor,
            ICursor cursor,
            IOptions<AddPackagesOptions> options,
            ILogger<AddPackagesCommand> logger)
        {
            _clientFactory = clientFactory;
            _leafProcessor = leafProcessor;
            _cursor = cursor;
            _options = options;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (_options.Value.PackageId != null)
            {
                _logger.LogError("Adding individual packages is not supported at this time");
                return;
            }

            var maxCursor = DateTimeOffset.MaxValue;
            var minCursor = await _cursor.GetAsync(cancellationToken);
            if (minCursor == null)
            {
                minCursor = DateTimeOffset.MinValue;
            }

            _logger.LogInformation("Finding catalog leafs committed after time {Cursor}...", minCursor);

            var catalogClient = _clientFactory.CreateCatalogClient();
            var catalogIndexAndLeafItems = await catalogClient.GetIndexAndLeafItemsAsync(
                minCursor.Value,
                maxCursor,
                _logger,
                cancellationToken);

            _logger.LogInformation("Removing duplicate catalog leafs...");

            var catalogIndex = catalogIndexAndLeafItems.Item1;
            var catalogLeafItems = catalogIndexAndLeafItems.Item2;

            catalogLeafItems = catalogLeafItems
                .GroupBy(l => new PackageIdentity(l.PackageId, l.ParsePackageVersion()))
                .Select(g => g.OrderByDescending(l => l.CommitTimestamp).First())
                .Where(l => l.IsPackageDetails())
                .ToList();

            _logger.LogInformation("Processing {CatalogLeafs} catalog leafs...", catalogLeafItems.Count());

            await _leafProcessor.ProcessAsync(catalogLeafItems, cancellationToken);
            await _cursor.SetAsync(catalogIndex.CommitTimestamp, cancellationToken);

            _logger.LogInformation("Finished processing catalog leafs");
        }
    }
}
