using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public static class ICatalogClientExtensions
    {
        public async static Task<(CatalogIndex, IEnumerable<CatalogLeafItem>)> GetIndexAndLeafItemsAsync(
            this ICatalogClient catalogClient,
            DateTimeOffset minCursor,
            DateTimeOffset maxCursor,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            var catalogIndex = await catalogClient.GetIndexAsync(cancellationToken);

            var catalogLeafItems = new ConcurrentBag<CatalogLeafItem>();
            var catalogPageUrls = new ConcurrentBag<CatalogPageItem>(
                catalogIndex.GetPagesInBounds(minCursor, maxCursor));

            await ParallelAsync.RunAsync(
                catalogPageUrls,
                ProcessCatalogPageAsync,
                cancellationToken);

            return (catalogIndex, catalogLeafItems);

            async Task ProcessCatalogPageAsync(CatalogPageItem pageItem, CancellationToken token)
            {
                logger.LogInformation("Processing catalog page {CatalogPageUrl}...", pageItem.CatalogPageUrl);

                var page = await catalogClient.GetPageAsync(pageItem.CatalogPageUrl, token);
                var leafs = page.GetLeavesInBounds(minCursor, maxCursor, excludeRedundantLeaves: true);

                foreach (var catalogLeafItem in leafs)
                {
                    catalogLeafItems.Add(catalogLeafItem);
                }

                logger.LogInformation("Processed catalog page {CatalogPageUrl}", pageItem.CatalogPageUrl);
            }
        }

        public async static Task<IEnumerable<CatalogLeafItem>> GetLeafItemsAsync(
            this ICatalogClient catalogClient,
            DateTimeOffset minCursor,
            DateTimeOffset maxCursor,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            var result = await GetIndexAndLeafItemsAsync(catalogClient, minCursor, maxCursor, logger, cancellationToken);

            return result.Item2;
        }
    }
}
