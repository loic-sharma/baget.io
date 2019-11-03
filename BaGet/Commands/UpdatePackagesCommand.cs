using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BaGet
{
    public class UpdatePackagesCommand : ICommand
    {
        private readonly IPackageService _packages;
        private readonly DownloadDataClient _downloadClient;
        private readonly TableOperationBuilder _operationBuilder;
        private readonly IndexActionBuilder _indexActionBuilder;
        private readonly BatchPusher _batchPusher;
        private readonly IOptions<UpdatePackagesOptions> _options;
        private readonly ILogger<UpdatePackagesCommand> _logger;

        public UpdatePackagesCommand(
            IPackageService packages,
            DownloadDataClient downloadClient,
            TableOperationBuilder tableOperationBuilder,
            IndexActionBuilder indexActionBuilder,
            BatchPusher batchPusher,
            IOptions<UpdatePackagesOptions> options,
            ILogger<UpdatePackagesCommand> logger)
        {
            _packages = packages;
            _downloadClient = downloadClient;
            _operationBuilder = tableOperationBuilder;
            _indexActionBuilder = indexActionBuilder;
            _batchPusher = batchPusher;
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

            var idsToIndex = new HashSet<string>();
            var newDownloads = new DownloadData();
            var stopwatch = Stopwatch.StartNew();

            if (_options.Value.UpdateDownloads)
            {
                newDownloads = await _downloadClient.GetAsync(cancellationToken);

                // TODO: Remove downloads that haven't changed!

                foreach (var id in newDownloads.Keys)
                {
                    idsToIndex.Add(id);
                }
            }

            foreach (var rawPackageId in idsToIndex)
            {
                var packages = await _packages.FindAsync(rawPackageId, includeUnlisted: true, cancellationToken);

                // Skip if package does not exist.
                if (!packages.Any())
                {
                    continue;
                }

                var packageId = packages.OrderByDescending(p => p.Version).Select(p => p.Id).First();
                var packageRegistration = new PackageRegistration(
                    packageId,
                    packages);

                _logger.LogInformation("Updating package {PackageId}...", packageId);

                // Update the Azure Storage Table.
                foreach (var package in packages)
                {
                    if (newDownloads.TryGetDownloadCount(packageId, package.NormalizedVersionString, out var downloadCount))
                    {
                        package.Downloads = downloadCount;

                        await _batchPusher.AddAsync(
                            _operationBuilder.UpdateDownloads(
                                packageId,
                                package.Version,
                                downloadCount),
                            cancellationToken);
                    }
                }

                // Update the Azure Search index.
                var actions = new List<IndexAction<KeyedDocument>>();

                // TODO: Generate real Azure Search index actions.
                //var actions = _indexActionBuilder.UpdatePackageDownloads(packageRegistration);

                foreach (var action in actions)
                {
                    await _batchPusher.AddAsync(action, cancellationToken);
                }

                // The package ID is the Table's partition key. Since a Table batch can only affect a single
                // partition, flush the Table operations before moving onto the next package ID.
                await _batchPusher.FlushAsync(
                    onlyFullOperations: false,
                    onlyFullActions: true,
                    cancellationToken);
            }

            // Persist anything that's pending.
            await _batchPusher.FlushAsync(
                onlyFullOperations: false,
                onlyFullActions: false,
                cancellationToken);

            _logger.LogInformation(
                "Updated packages in {TotalSeconds} seconds",
                stopwatch.Elapsed.TotalSeconds);
        }
    }
}
