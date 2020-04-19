using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class UpdatePackageWorker
    {
        private readonly IPackageService _packages;
        private readonly DownloadData _newDownloads;
        private readonly BatchTableClient _batchTableClient;
        private readonly TableOperationBuilder _operationBuilder;
        private readonly IndexActionBuilder _indexActionBuilder;
        private readonly ChannelWriter<IndexAction<KeyedDocument>> _indexActionWriter;
        private readonly ILogger _logger;

        public UpdatePackageWorker(
            IPackageService packages,
            DownloadData newDownloads,
            BatchTableClient batchTableClient,
            TableOperationBuilder operationBuilder,
            IndexActionBuilder indexActionBuilder,
            ChannelWriter<IndexAction<KeyedDocument>> indexActionWriter,
            ILogger logger)
        {
            _packages = packages;
            _newDownloads = newDownloads;
            _batchTableClient = batchTableClient;
            _operationBuilder = operationBuilder;
            _indexActionBuilder = indexActionBuilder;
            _indexActionWriter = indexActionWriter;
            _logger = logger;
        }

        public async Task UpdateAsync(string packageId, CancellationToken cancellationToken)
        {
            var packages = await _packages.FindAsync(packageId, includeUnlisted: true, cancellationToken);

            // Skip if package does not exist.
            if (!packages.Any())
            {
                return;
            }

            var latestPackageId = packages.OrderByDescending(p => p.Version).Select(p => p.Id).First();
            var packageRegistration = new PackageRegistration(
                latestPackageId,
                packages);

            _logger.LogInformation("Updating package {PackageId}...", packageId);

            // Apply the downloads to the package entities.
            var dirty = false;

            foreach (var package in packages)
            {
                var found = _newDownloads.TryGetDownloadCount(
                    packageId,
                    package.NormalizedVersionString,
                    out var downloadCount);

                // Skip the download if the version is unknown or if downloads haven't changed.
                if (found && package.Downloads != downloadCount)
                {
                    dirty = true;
                    package.Downloads = downloadCount;
                    var operation = _operationBuilder.UpdateDownloads(
                        packageId,
                        package.Version,
                        downloadCount);

                    if (!_batchTableClient.TryAdd(operation))
                    {
                        await _batchTableClient.AddAsync(operation, cancellationToken);
                    }
                }
            }

            if (dirty)
            {
                // Ensure the downloads have been persisted to Azure Table Storage.
                await _batchTableClient.FlushAsync(onlyFull: false, cancellationToken);

                // Queue the Azure Search index updates.
                var actions = _indexActionBuilder.UpdateDownloads(packageRegistration);

                foreach (var action in actions)
                {
                    if (!_indexActionWriter.TryWrite(action))
                    {
                        await _indexActionWriter.WriteAsync(action, cancellationToken);
                    }
                }
            }
        }
    }
}
