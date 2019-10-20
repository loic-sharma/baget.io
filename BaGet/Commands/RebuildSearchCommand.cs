using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Azure.Search;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;
using NuGet.Packaging.Core;

namespace BaGet
{
    public class RebuildSearchCommand : ICommand
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly ICursor _cursor;
        private readonly IPackageService _packages;
        private readonly IndexActionBuilder _actionBuilder;
        private readonly AzureSearchBatchIndexer _indexer;
        private readonly ILogger<RebuildSearchCommand> _logger;

        public RebuildSearchCommand(
            NuGetClientFactory clientFactory,
            ICursor cursor,
            IPackageService packages,
            IndexActionBuilder actionBuilder,
            AzureSearchBatchIndexer indexer,
            ILogger<RebuildSearchCommand> logger)
        {
            _clientFactory = clientFactory;
            _cursor = cursor;
            _packages = packages;
            _actionBuilder = actionBuilder;
            _indexer = indexer;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            var minCursor = DateTimeOffset.MinValue;
            var maxCursor = await _cursor.GetAsync(cancellationToken);
            if (maxCursor == null)
            {
                maxCursor = DateTimeOffset.MinValue;
            }

            _logger.LogInformation("Finding catalog leafs comitted before time {Cursor}...", maxCursor);

            var catalogClient = _clientFactory.CreateCatalogClient();
            var (catalogIndex, catalogLeafItems) = await catalogClient.LoadCatalogAsync(
                minCursor,
                maxCursor.Value,
                _logger,
                cancellationToken);

            _logger.LogInformation("Deduplicating catalog leafs...");

            var packageIds = catalogLeafItems
                .GroupBy(l => new PackageIdentity(l.PackageId, l.ParsePackageVersion()))
                .Select(g => g.OrderByDescending(l => l.CommitTimestamp).First())
                .Where(l => l.IsPackageDetails())
                .Select(l => l.PackageId)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            _logger.LogInformation("Processing {PackageCount} packages", packageIds.Count);

            var channel = Channel.CreateBounded<IndexAction<KeyedDocument>>(new BoundedChannelOptions(5000)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = true,
            });

            var produceTask = ProduceIndexActionsAsync(
                channel.Writer,
                new ConcurrentBag<string>(packageIds),
                cancellationToken);
            var consumeTask = ConsumePackageRegistrationsAsync(
                channel.Reader,
                cancellationToken);

            await Task.WhenAll(produceTask, consumeTask);

            _logger.LogInformation("Finished rebuilding search");
        }

        private async Task ProduceIndexActionsAsync(
            ChannelWriter<IndexAction<KeyedDocument>> channel,
            ConcurrentBag<string> packageIds,
            CancellationToken cancellationToken)
        {
            await ParallelHelper.ProcessInParallel(
                packageIds,
                ProduceIndexActionsAsync,
                cancellationToken);

            _logger.LogInformation("Finished producing index actions");
            channel.Complete();
            return;

            async Task ProduceIndexActionsAsync(string packageId, CancellationToken cancellationToken1)
            {
                _logger.LogInformation("Adding package {PackageId}...", packageId);

                var packages = await _packages.FindAsync(packageId, includeUnlisted: false);
                if (packages.Count == 0)
                {
                    _logger.LogWarning(
                        "Could not find any listed packages for package ID {PackageId}, skipping...",
                        packageId);
                    return;
                }

                var registration = new PackageRegistration(
                    packageId,
                    packages);

                foreach (var action in _actionBuilder.AddPackage(registration))
                {
                    if (!channel.TryWrite(action))
                    {
                        await channel.WriteAsync(action, cancellationToken);
                    }
                }
            }
        }

        private async Task ConsumePackageRegistrationsAsync(
            ChannelReader<IndexAction<KeyedDocument>> channel,
            CancellationToken cancellationToken)
        {
            while (await channel.WaitToReadAsync(cancellationToken))
            {
                while (channel.TryRead(out var action))
                {
                    await _indexer.EnqueueIndexActionAsync(action, cancellationToken);
                    await _indexer.PushBatchesAsync(onlyFull: true, cancellationToken);
                }
            }

            await _indexer.PushBatchesAsync(cancellationToken);

            _logger.LogInformation("Finished consuming index actions");
        }
    }
}
