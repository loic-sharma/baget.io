using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NuGet.Packaging.Core;

namespace BaGet
{
    public class UpdatePackagesCommand : ICommand
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly ICursor _cursor;
        private readonly IPackageService _packages;
        private readonly DownloadDataClient _downloadClient;
        private readonly TableOperationBuilder _operationBuilder;
        private readonly IndexActionBuilder _indexActionBuilder;
        private readonly Func<BatchTableClient> _tableClientFactory;
        private readonly Func<BatchSearchClient> _searchClientFactory;
        private readonly IOptions<UpdatePackagesOptions> _options;
        private readonly ILogger<UpdatePackagesCommand> _logger;

        public UpdatePackagesCommand(
            NuGetClientFactory clientFactory,
            ICursor cursor,
            IPackageService packages,
            DownloadDataClient downloadClient,
            TableOperationBuilder tableOperationBuilder,
            IndexActionBuilder indexActionBuilder,
            Func<BatchTableClient> tableClientFactory,
            Func<BatchSearchClient> searchClientFactory,
            IOptions<UpdatePackagesOptions> options,
            ILogger<UpdatePackagesCommand> logger)
        {
            _clientFactory = clientFactory;
            _cursor = cursor;
            _packages = packages;
            _downloadClient = downloadClient;
            _operationBuilder = tableOperationBuilder;
            _indexActionBuilder = indexActionBuilder;
            _tableClientFactory = tableClientFactory;
            _searchClientFactory = searchClientFactory;
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

            if (_options.Value.UpdateOwners)
            {
                _logger.LogError("Updating package owners is not supported at this time");
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

            _logger.LogInformation("Removing duplicate catalog leafs...");

            var packageIds = catalogLeafItems
                .GroupBy(l => new PackageIdentity(l.PackageId, l.ParsePackageVersion()))
                .Select(g => g.OrderByDescending(l => l.CommitTimestamp).First())
                .Where(l => l.IsPackageDetails())
                .Select(l => l.PackageId);

            var knownPackageIds = new HashSet<string>(packageIds, StringComparer.OrdinalIgnoreCase);
            var idsToIndex = new ConcurrentBag<string>();
            var newDownloads = new DownloadData();
            var stopwatch = Stopwatch.StartNew();

            if (_options.Value.UpdateDownloads)
            {
                newDownloads = await _downloadClient.GetAsync(cancellationToken);

                // TODO: Remove downloads that haven't changed!

                foreach (var id in newDownloads.Keys)
                {
                    if (knownPackageIds.Contains(id))
                    {
                        idsToIndex.Add(id);
                    }
                }
            }

            var channel = Channel.CreateBounded<IndexAction<KeyedDocument>>(new BoundedChannelOptions(5000)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = false,
            });

            var updatePackagesTask = ParallelAsync.RepeatAsync(
                CreateUpdatePackagesWorker(idsToIndex, newDownloads, channel.Writer, cancellationToken));
            var updateSearchTask = ParallelAsync.RepeatAsync(
                degreesOfConcurrency: 4,
                taskFactory: CreateUpdateSearchWorker(channel.Reader, cancellationToken));

            await updatePackagesTask;
            channel.Writer.Complete();
            await updateSearchTask;

            _logger.LogInformation(
                "Updated packages in {TotalSeconds} seconds",
                stopwatch.Elapsed.TotalSeconds);
        }

        private Func<Task> CreateUpdatePackagesWorker(
            ConcurrentBag<string> idsToIndex,
            DownloadData newDownloads,
            ChannelWriter<IndexAction<KeyedDocument>> indexActionWriter,
            CancellationToken cancellationToken)
        {
            return async () =>
            {
                var worker = new UpdatePackageWorker(
                    _packages,
                    newDownloads,
                    _tableClientFactory(),
                    _operationBuilder,
                    _indexActionBuilder,
                    indexActionWriter,
                    _logger);

                while (idsToIndex.TryTake(out var packageId))
                {
                    var attempt = 0;

                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            await worker.UpdateAsync(packageId, cancellationToken);
                            break;
                        }
                        catch (Exception) when (attempt < 3)
                        {
                            attempt++;
                        }
                    }
                }
            };
        }

        private Func<Task> CreateUpdateSearchWorker(
            ChannelReader<IndexAction<KeyedDocument>> indexActionReader,
            CancellationToken cancellationToken)
        {
            return async () =>
            {
                var batchSearchClient = _searchClientFactory();

                while (await indexActionReader.WaitToReadAsync(cancellationToken))
                {
                    while (indexActionReader.TryRead(out var indexAction))
                    {
                        if (!batchSearchClient.TryAdd(indexAction))
                        {
                            await batchSearchClient.AddAsync(indexAction, cancellationToken);
                        }
                    }
                }
            };
        }
    }
}
