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
        private readonly Func<AzureSearchBatchIndexer> _indexerFactory;
        private readonly ILogger<RebuildSearchCommand> _logger;

        public RebuildSearchCommand(
            NuGetClientFactory clientFactory,
            ICursor cursor,
            IPackageService packages,
            IndexActionBuilder actionBuilder,
            Func<AzureSearchBatchIndexer> indexerFactory,
            ILogger<RebuildSearchCommand> logger)
        {
            _clientFactory = clientFactory;
            _cursor = cursor;
            _packages = packages;
            _actionBuilder = actionBuilder;
            _indexerFactory = indexerFactory;
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

            var packageIds = catalogLeafItems
                .GroupBy(l => new PackageIdentity(l.PackageId, l.ParsePackageVersion()))
                .Select(g => g.OrderByDescending(l => l.CommitTimestamp).First())
                .Where(l => l.IsPackageDetails())
                .Select(l => l.PackageId)
                .ToList();

            _logger.LogInformation("Processing {PackageCount} packages", packageIds.Count);

            var channel = Channel.CreateBounded<IndexAction<KeyedDocument>>(new BoundedChannelOptions(5000)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = false,
            });

            var produceTask = ProducePackageRegistrationsAsync(
                channel.Writer,
                new ConcurrentBag<string>(packageIds),
                cancellationToken);
            var consumeTask1 = ConsumePackageRegistrationsAsync(
                channel.Reader,
                cancellationToken);
            var consumeTask2 = ConsumePackageRegistrationsAsync(
                channel.Reader,
                cancellationToken);

            await Task.WhenAll(produceTask, consumeTask1, consumeTask2);

            _logger.LogInformation("Finished rebuilding search");
        }

        private async Task ProducePackageRegistrationsAsync(
            ChannelWriter<IndexAction<KeyedDocument>> channel,
            ConcurrentBag<string> packageIds,
            CancellationToken cancellationToken)
        {
            await ParallelHelper.ProcessInParallel(
                packageIds,
                async (packageId, c) =>
                {
                    _logger.LogInformation("Adding package {PackageId}...", packageId);

                    var packages = await _packages.FindAsync(packageId, includeUnlisted: false);
                    if (packages.Count == 0)
                    {
                        _logger.LogWarning(
                            "Could not find any packages named {PackageId}, skipping...",
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
                            await channel.WriteAsync(action);
                        }
                    }
                },
                cancellationToken);
        }

        private async Task ConsumePackageRegistrationsAsync(
            ChannelReader<IndexAction<KeyedDocument>> channel,
            CancellationToken cancellationToken)
        {
            var indexer = _indexerFactory();

            while (await channel.WaitToReadAsync(cancellationToken))
            {
                while (channel.TryRead(out var action))
                {
                    await indexer.EnqueueIndexActionAsync(action, cancellationToken);
                    await indexer.PushBatchesAsync(onlyFull: true, cancellationToken);
                }
            }

            await indexer.PushBatchesAsync(cancellationToken);
        }
    }
}
