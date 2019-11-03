using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet.Packaging;

namespace BaGet
{
    public class ProcessCatalogLeafItem : ICatalogLeafItemProcessor
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly IPackageService _packages;
        private readonly CloudBlobContainer _blobContainer;
        private readonly ISearchIndexer _indexer;
        private readonly ILogger<ProcessCatalogLeafItem> _logger;

        public ProcessCatalogLeafItem(
            NuGetClientFactory clientFactory,
            IPackageService packages,
            CloudBlobContainer blobContainer,
            ISearchIndexer indexer,
            ILogger<ProcessCatalogLeafItem> logger)
        {
            _clientFactory = clientFactory;
            _packages = packages;
            _blobContainer = blobContainer;
            _indexer = indexer;
            _logger = logger;
        }

        public async Task ProcessAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Processing catalog leaf {CatalogLeafUrl}", catalogLeafItem.CatalogLeafUrl);

            switch (catalogLeafItem.Type)
            {
                case CatalogLeafType.PackageDetails:
                    await ProcessPackageDetailsAsync(catalogLeafItem, cancellationToken);
                    break;

                case CatalogLeafType.PackageDelete:
                    await ProcessPackageDeleteAsync(catalogLeafItem, cancellationToken);
                    break;

                default:
                    throw new NotSupportedException($"Unknown catalog leaf type '{catalogLeafItem.Type}'");
            }

            _logger.LogInformation("Processed catalog leaf {CatalogLeafUrl}", catalogLeafItem.CatalogLeafUrl);
        }

        public Task CompleteAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task ProcessPackageDetailsAsync(
            CatalogLeafItem catalogLeafItem,
            CancellationToken cancellationToken)
        {
            var catalogClient = _clientFactory.CreateCatalogClient();
            var catalogLeaf = await catalogClient.GetPackageDetailsLeafAsync(catalogLeafItem.CatalogLeafUrl, cancellationToken);

            var packageId = catalogLeaf.PackageId.ToLowerInvariant();
            var packageVersion = catalogLeaf.ParsePackageVersion();

            Package package;
            Stream packageStream = null;
            Stream readmeStream = null;

            try
            {
                var contentClient = _clientFactory.CreatePackageContentClient();
                using (var stream = await contentClient.GetPackageContentStreamOrNullAsync(packageId, packageVersion, cancellationToken))
                {
                    packageStream = await stream.AsTemporaryFileStreamAsync(cancellationToken);
                }

                _logger.LogInformation(
                    "Downloaded package {PackageId} {PackageVersion}, building metadata...",
                    packageId,
                    packageVersion);

                using (var reader = new PackageArchiveReader(packageStream, leaveStreamOpen: true))
                {
                    package = reader.GetPackageMetadata();

                    package.Listed = catalogLeaf.IsListed();
                    package.Published = catalogLeaf.Published.UtcDateTime;

                    if (package.HasReadme)
                    {
                        readmeStream = await reader.GetReadmeAsync(cancellationToken);
                        readmeStream = await readmeStream.AsTemporaryFileStreamAsync(cancellationToken);
                    }

                    await IndexPackageAsync(package, readmeStream, cancellationToken);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(
                    e,
                    "Failed to process package {PackageId} {PackageVersion}",
                    packageId,
                    packageVersion);

                throw;
            }
            finally
            {
                packageStream?.Dispose();
                readmeStream?.Dispose();
            }
        }

        private async Task ProcessPackageDeleteAsync(
            CatalogLeafItem catalogLeafItem,
            CancellationToken cancellationToken)
        {
            await _packages.UnlistPackageAsync(
                catalogLeafItem.PackageId,
                catalogLeafItem.ParsePackageVersion(),
                cancellationToken);
        }

        private async Task IndexPackageAsync(
            Package package,
            Stream readmeStreamOrNull,
            CancellationToken cancellationToken)
        {
            var packageId = package.Id.ToLowerInvariant();
            var packageVersion = package.NormalizedVersionString.ToLowerInvariant();

            if (readmeStreamOrNull != null)
            {
                await UploadReadmeAsync(
                    packageId,
                    packageVersion,
                    readmeStreamOrNull,
                    cancellationToken);
            }

            // Try to add the package to the database. If the package already exists,
            // the add operation will fail and we will update the package's metadata instead.
            var addResult = await _packages.AddAsync(package, cancellationToken);
            switch (addResult)
            {
                case PackageAddResult.Success:
                    _logger.LogInformation(
                        "Added package {PackageId} {PackageVersion} to database",
                        packageId,
                        packageVersion);
                    break;

                // The package already exists. Update its metadata instead.
                case PackageAddResult.PackageAlreadyExists:
                    await UpdatePackageMetadata(package, cancellationToken);
                    break;

                default:
                    throw new NotSupportedException($"Unknown add result '{addResult}'");
            }

            // Finally, use the database to update the search service.
            await _indexer.IndexAsync(package, cancellationToken);
        }

        private async Task UploadReadmeAsync(
            string packageId,
            string packageVersion,
            Stream readme,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Uploading readme for package {PackageId} {PackageVersion}...",
                packageId,
                packageVersion);

            var blob = _blobContainer.GetBlockBlobReference($"v3/package/{packageId}/{packageVersion}/readme");

            blob.Properties.ContentType = "text/markdown";
            blob.Properties.CacheControl = "max-age=120, must-revalidate";

            await blob.UploadFromStreamAsync(readme, cancellationToken);

            _logger.LogInformation(
                "Uploaded readme for package {PackageId} {PackageVersion}",
                packageId,
                packageVersion);
        }

        private async Task UpdatePackageMetadata(Package latestPackage, CancellationToken cancellationToken)
        {
            var packageId = latestPackage.Id;
            var packageVersion = latestPackage.Version;

            _logger.LogInformation(
                "Updating package {PackageId} {PackageVersion} in database..",
                latestPackage.Id,
                latestPackage.Version);

            var currentPackage = await _packages.FindOrNullAsync(
                packageId,
                packageVersion,
                includeUnlisted: true,
                cancellationToken);

            if (currentPackage == null)
            {
                throw new InvalidOperationException(
                    $"Cannot update package {packageId} {packageVersion} because it does not exist");
            }

            // TODO: Ideally we should replace all metadata.
            if (currentPackage.Listed != latestPackage.Listed)
            {
                if (latestPackage.Listed)
                {
                    await _packages.RelistPackageAsync(packageId, packageVersion, cancellationToken);
                }
                else
                {
                    await _packages.UnlistPackageAsync(packageId, packageVersion, cancellationToken);
                }
            }

            _logger.LogInformation(
                "Updated package {PackageId} {PackageVersion} in database",
                packageId,
                packageVersion);
        }
    }
}
