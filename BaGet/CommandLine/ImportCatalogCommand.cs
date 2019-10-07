using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using NuGet.Packaging.Core;

namespace BaGet
{
    public class ImportCatalogCommand
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly ICatalogLeafItemBatchProcessor _leafProcessor;
        private readonly ICursor _cursor;
        private readonly ILogger<ImportCatalogCommand> _logger;

        public ImportCatalogCommand(
            NuGetClientFactory clientFactory,
            ICatalogLeafItemBatchProcessor leafProcessor,
            ICursor cursor,
            ILogger<ImportCatalogCommand> logger)
        {
            _clientFactory = clientFactory;
            _leafProcessor = leafProcessor;
            _cursor = cursor;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            var maxCursor = DateTimeOffset.MaxValue;
            var minCursor = await _cursor.GetAsync(cancellationToken);
            if (minCursor == null)
            {
                minCursor = DateTimeOffset.MinValue;
            }

            _logger.LogInformation("Finding catalog leafs comitted after time {Cursor}...", minCursor);

            var catalogClient = await _clientFactory.CreateCatalogClientAsync(cancellationToken);
            var (catalogIndex, catalogLeafItems) = await catalogClient.LoadCatalogAsync(
                minCursor.Value,
                maxCursor,
                _logger,
                cancellationToken);

            catalogLeafItems = DeduplicateCatalogLeafItems(catalogLeafItems);

            _logger.LogInformation("Importing {CatalogLeafs} catalog leafs...", catalogLeafItems.Count());

            await _leafProcessor.ProcessAsync(catalogLeafItems, cancellationToken);
            await _cursor.SetAsync(catalogIndex.CommitTimestamp, cancellationToken);

            _logger.LogInformation("Finished importing catalog leafs");
        }

        private IEnumerable<CatalogLeafItem> DeduplicateCatalogLeafItems(IEnumerable<CatalogLeafItem> catalogLeafItems)
        {
            // Grab only the latest catalog leaf for each package id and version.
            // Skip packages that were deleted.
            _logger.LogInformation("Removing duplicate leafs...");

            return catalogLeafItems
                .GroupBy(l => new PackageIdentity(l.PackageId, l.ParsePackageVersion()))
                .Select(g => g.OrderByDescending(l => l.CommitTimestamp).First())
                .Where(l => l.IsPackageDetails())
                .ToList();
        }
    }
}
