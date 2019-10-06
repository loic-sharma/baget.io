using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Core;
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

        private readonly IPackageDownloadsSource _downloads;
        private readonly CloudTable _table;
        private readonly ILogger<ImportDownloadsCommand> _logger;

        public ImportDownloadsCommand(
            IPackageDownloadsSource downloads,
            CloudTableClient tableClient,
            ILogger<ImportDownloadsCommand> logger)
        {
            _downloads = downloads;
            _table = tableClient.GetTableReference(TableName);
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // TODO: Rename to "GetAsync", add a cancellation token...
            var downloads = await _downloads.GetPackageDownloadsAsync();

            // TODO: Do this concurrently.
            var work = new ConcurrentBag<KeyValuePair<string, Dictionary<string, long>>>(downloads);

            await ParallelHelper.ProcessInParallel(
                work,
                ImportPackageDownloadsAsync,
                cancellationToken);
        }

        private async Task ImportPackageDownloadsAsync(
            KeyValuePair<string, Dictionary<string, long>> packageDownloads,
            CancellationToken cancellationToken)
        {
            HashSet<string> knownVersions = null;

            var packageId = packageDownloads.Key;
            var downloadEntities = packageDownloads
                .Value
                .Select(versionDownloads => new PackageDownloadsEntity
                {
                    PartitionKey = packageId.ToLowerInvariant(),
                    RowKey = NuGetVersion.Parse(versionDownloads.Key)
                        .ToNormalizedString()
                        .ToLowerInvariant(),
                    ETag = "*",
                    Downloads = versionDownloads.Value
                })
                .ToList();

            var next = 0;
            while (downloadEntities.Count > next)
            {
                // Build the next batch of enetities to process
                var end = next;
                var batchOperation = new TableBatchOperation();
                while (batchOperation.Count < MaxTableOperations && downloadEntities.Count > end)
                {
                    // Only include the current entity if it exists.
                    if (knownVersions == null || knownVersions.Contains(downloadEntities[end].RowKey))
                    {
                        batchOperation.Merge(downloadEntities[end]);
                    }

                    end++;
                }

                // Skip if all remaining entities were skipped.
                if (!batchOperation.Any())
                {
                    return;
                }

                // Persist the entities.
                try
                {
                    _logger.LogInformation(
                        "Updating {Count} versions of package {PackageId}...",
                        batchOperation.Count,
                        packageId);

                    await _table.ExecuteBatchAsync(batchOperation, cancellationToken);

                    next = end;
                }
                catch (StorageException e) when (IsNotFoundException(e) && knownVersions == null)
                {
                    _logger.LogError(
                        "Failed to update package downloads for {PackageId}, fetching known versions...",
                        packageId);

                    // TODO: Get known version!
                    knownVersions = new HashSet<string>();
                }
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
