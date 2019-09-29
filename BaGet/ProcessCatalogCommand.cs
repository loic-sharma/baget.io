using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using NuGet.Packaging.Core;

namespace baget.io
{
    class ProcessCatalogCommand : IHostedCommand
    {
        private const int MaxDegreeOfParallelism = 32;

        private readonly NuGetClientFactory _clientFactory;
        private readonly ICatalogLeafItemProcessor _leafProcessor;
        private readonly ICursor _cursor;
        private readonly ILogger<ProcessCatalogCommand> _logger;

        public ProcessCatalogCommand(
            NuGetClientFactory clientFactory,
            ICatalogLeafItemProcessor leafProcessor,
            ICursor cursor,
            ILogger<ProcessCatalogCommand> logger)
        {
            _clientFactory = clientFactory;
            _leafProcessor = leafProcessor;
            _cursor = cursor;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // Prepare the processing.
            ThreadPool.SetMinThreads(MaxDegreeOfParallelism, completionPortThreads: 4);
            ServicePointManager.DefaultConnectionLimit = MaxDegreeOfParallelism;
            ServicePointManager.MaxServicePointIdleTime = 10000;

            var cursor = await _cursor.GetAsync(cancellationToken);
            if (cursor == null)
            {
                cursor = DateTimeOffset.MinValue;
            }

            _logger.LogInformation("Finding catalog leafs comitted after time {Cursor}...", cursor);

            var catalogClient = await _clientFactory.CreateCatalogClientAsync(cancellationToken);
            var catalogIndex = await catalogClient.GetIndexAsync(cancellationToken);
            var catalogLeafItems = await GetCatalogLeafItems(catalogClient, catalogIndex, cursor.Value, cancellationToken);

            catalogLeafItems = DeduplicateCatalogLeafItems(catalogLeafItems);

            _logger.LogInformation("Processing {CatalogLeafs} catalog leafs...", catalogLeafItems.Count());

            await ProcessCatalogLeafsAsync(catalogLeafItems, cancellationToken);
            await _cursor.SetAsync(catalogIndex.CommitTimestamp);

            _logger.LogInformation("Finished processing catalog leafs");
        }

        private async Task<IEnumerable<CatalogLeafItem>> GetCatalogLeafItems(
            ICatalogClient catalogClient,
            CatalogIndex catalogIndex,
            DateTimeOffset cursor,
            CancellationToken cancellationToken)
        {
            var catalogLeafItems = new ConcurrentBag<CatalogLeafItem>();
            var catalogPageUrls = new ConcurrentBag<string>(
                catalogIndex
                    .Items
                    .Where(i => i.CommitTimestamp > cursor)
                    .Select(i => i.CatalogPageUrl));

            await ProcessInParallel(catalogPageUrls, async catalogPageUrl =>
            {
                _logger.LogInformation("Processing catalog page {CatalogPageUrl}...", catalogPageUrl);

                var page = await catalogClient.GetPageAsync(catalogPageUrl, cancellationToken);
                        
                foreach (var catalogLeafItem in page.Items.Where(i => i.CommitTimestamp > cursor))
                {
                    catalogLeafItems.Add(catalogLeafItem);
                }

                _logger.LogInformation("Processed catalog page {CatalogPageUrl}", catalogPageUrl);
            });

            return catalogLeafItems;
        }

        private IEnumerable<CatalogLeafItem> DeduplicateCatalogLeafItems(IEnumerable<CatalogLeafItem> catalogLeafItems)
        {
            // Grab only the latest catalog leaf for each package id and version.
            // Skip packages that were deleted.
            return catalogLeafItems
                .GroupBy(l => new PackageIdentity(l.PackageId, l.ParsePackageVersion()))
                .Select(g => g.OrderByDescending(l => l.CommitTimestamp).First())
                .Where(l => l.IsPackageDetails())
                .ToList();
        }

        private async Task ProcessCatalogLeafsAsync(
            IEnumerable<CatalogLeafItem> catalogLeafItems,
            CancellationToken cancellationToken)
        {
            var work = new ConcurrentBag<CatalogLeafItem>(catalogLeafItems);

            await ProcessInParallel(work, async catalogLeafItem =>
            {
                await _leafProcessor.ProcessAsync(catalogLeafItem, cancellationToken);
            });
        }

        private async Task ProcessInParallel<T>(ConcurrentBag<T> allWork, Func<T, Task> worker)
        {
             await Task.WhenAll(
                Enumerable
                    .Repeat(allWork, MaxDegreeOfParallelism)
                    .Select(async work =>
                    {
                        while (work.TryTake(out var item))
                        {
                            try
                            {
                                await worker(item);
                            }
                            catch (Exception e)
                            {
                                _logger.LogError(e, "Unable to process item due to exception");
                            }
                        }
                    }));
        }
    }
}
