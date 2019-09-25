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

            _logger.LogInformation("Finding catalog leafs...");

            var catalogClient = await _clientFactory.CreateCatalogClientAsync(cancellationToken);
            var catalogIndex = await catalogClient.GetIndexAsync(cancellationToken);
            var catalogLeafItems = await GetCatalogLeafItems(catalogClient, catalogIndex, cancellationToken);

            catalogLeafItems = DeduplicateCatalogLeafItems(catalogLeafItems);

            _logger.LogInformation("Processing {CatalogLeafs} catalog leafs...", catalogLeafItems.Count());

            await ProcessCatalogLeafsAsync(catalogLeafItems, cancellationToken);
            await _cursor.SetAsync(catalogIndex.CommitTimestamp);

            _logger.LogInformation("Finished processing catalog leafs");
        }

        private async Task<IEnumerable<CatalogLeafItem>> GetCatalogLeafItems(
            ICatalogClient catalogClient,
            CatalogIndex catalogIndex,
            CancellationToken cancellationToken)
        {
            var catalogPageUrls = new ConcurrentBag<string>(
                catalogIndex.Items.Select(i => i.CatalogPageUrl));
            var catalogLeafItems = new ConcurrentBag<CatalogLeafItem>();

            await ProcessInParallel(async () =>
            {
                while (catalogPageUrls.TryTake(out var catalogPageUrl))
                {
                    try
                    {
                        _logger.LogInformation("Processing catalog page {CatalogPageUrl}...", catalogPageUrl);

                        var page = await catalogClient.GetPageAsync(catalogPageUrl, cancellationToken);
                        
                        foreach (var catalogLeafItem in page.Items)
                        {
                            catalogLeafItems.Add(catalogLeafItem);
                        }

                        _logger.LogInformation("Processed catalog page {CatalogPageUrl}", catalogPageUrl);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Unable to process catalog page {CatalogPageUrl}", catalogPageUrl);
                    }
                }
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

            await ProcessInParallel(async () =>
            {
                while (work.TryTake(out var catalogLeafItem))
                {
                    try
                    {
                        await _leafProcessor.ProcessAsync(catalogLeafItem, cancellationToken);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(
                            e,
                            "Failed to process catalog leaf item for leaf {CatalogLeafUrl} due to exception",
                            catalogLeafItem.CatalogLeafUrl);
                    }
                }
            });
        }

        private static async Task ProcessInParallel(Func<Task> worker)
        {
             await Task.WhenAll(
                Enumerable
                    .Repeat(worker, MaxDegreeOfParallelism)
                    .Select(x => x()));
        }
    }
}
