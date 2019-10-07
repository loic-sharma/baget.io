using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using NuGet.Versioning;

namespace BaGet
{
    public class ImportDownloadsCommand
    {
        // TODO: Share with BaGet.Azure?
        private const string TableName = "Packages";
        private const int MaxTableOperations = 100;

        private readonly NuGetClientFactory _clientFactory;
        private readonly ICursor _cursor;
        private readonly IPackageDownloadsSource _downloads;
        private readonly IPackageService _packages;
        private readonly CloudTable _table;
        private readonly ILogger<ImportDownloadsCommand> _logger;

        public ImportDownloadsCommand(
            NuGetClientFactory clientFactory,
            ICursor cursor,
            IPackageDownloadsSource downloads,
            IPackageService packages,
            CloudTableClient tableClient,
            ILogger<ImportDownloadsCommand> logger)
        {
            _clientFactory = clientFactory;
            _cursor = cursor;
            _downloads = downloads;
            _packages = packages;
            _table = tableClient.GetTableReference(TableName);
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // TODO: Rename to "GetAsync", add a cancellation token...
            var downloads = await _downloads.GetPackageDownloadsAsync();
            var work = await PrepareWorkAsync(downloads, cancellationToken);

            await ParallelHelper.ProcessInParallel(
                work,
                ImportPackageDownloadsAsync,
                cancellationToken);
        }

        private async Task<ConcurrentBag<TableBatchOperation>> PrepareWorkAsync(
            Dictionary<string, Dictionary<string, long>> downloads,
            CancellationToken cancellationToken)
        {
            var knownPackages = await ImportKnownPackagesAsync(cancellationToken);

            var result = new ConcurrentBag<TableBatchOperation>();
            var current = new TableBatchOperation();

            foreach (var packageDownloads in downloads)
            {
                var packageId = packageDownloads.Key.ToLowerInvariant();

                if (!knownPackages.ContainsKey(packageId))
                {
                    continue;
                }

                foreach (var versionDownloads in packageDownloads.Value)
                {
                    var packageVersion = NuGetVersion.Parse(versionDownloads.Key)
                        .ToNormalizedString()
                        .ToLowerInvariant();

                    if (!knownPackages[packageId].Contains(packageVersion))
                    {
                        continue;
                    }

                    var entity = new PackageDownloadsEntity
                    {
                        PartitionKey = packageId,
                        RowKey = packageVersion,
                        ETag = "*",
                        Downloads = versionDownloads.Value
                    };

                    current.Add(TableOperation.Merge(entity));

                    if (current.Count == MaxTableOperations)
                    {
                        result.Add(current);
                        current = new TableBatchOperation();
                    }
                }

                if (current.Any())
                {
                    result.Add(current);
                    current = new TableBatchOperation();
                }
            }

            return result;
        }

        private async Task<Dictionary<string, HashSet<string>>> ImportKnownPackagesAsync(CancellationToken cancellationToken)
        {
            var minCursor = DateTimeOffset.MinValue;
            var maxCursor = await _cursor.GetAsync(cancellationToken);
            if (maxCursor == null)
            {
                maxCursor = DateTimeOffset.MinValue;
            }

            _logger.LogInformation("Finding catalog leafs comitted before time {Cursor}...", maxCursor);

            var catalogClient = await _clientFactory.CreateCatalogClientAsync(cancellationToken);
            var (catalogIndex, catalogLeafItems) = await catalogClient.LoadCatalogAsync(
                minCursor,
                maxCursor.Value,
                _logger,
                cancellationToken);

            return catalogLeafItems
                .GroupBy(l => l.PackageId.ToLowerInvariant())
                .ToDictionary(
                    g => g.Key,
                    g => new HashSet<string>(
                        g.Select(
                            leaf => leaf
                                .ParsePackageVersion()
                                .ToNormalizedString()
                                .ToLowerInvariant())));
        }

        private async Task ImportPackageDownloadsAsync(
            TableBatchOperation operation,
            CancellationToken cancellationToken)
        {
            try
            {
                var packageId = operation.First().Entity.PartitionKey;

                _logger.LogInformation(
                    "Updating {Count} versions of package {PackageId}...",
                    operation.Count,
                    packageId);

                await _table.ExecuteBatchAsync(operation, cancellationToken);
            }
            catch (StorageException e) when (IsNotFoundException(e))
            {
                // TODO: Reprocess?
            }
        }

        private bool IsNotFoundException(StorageException exception)
        {
            return exception?.RequestInformation?.HttpStatusCode == (int?)HttpStatusCode.NotFound;
        }

        // TODO: Share this with BaGet.Azure?
        private class PackageDownloadsEntity : TableEntity
        {
            public long Downloads { get; set; }
        }
    }
}
