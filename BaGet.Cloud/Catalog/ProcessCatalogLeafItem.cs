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
        private readonly PackageIndexer _indexer;
        private readonly ILogger<ProcessCatalogLeafItem> _logger;

        public ProcessCatalogLeafItem(
            NuGetClientFactory clientFactory,
            IPackageService packages,
            CloudBlobContainer blobContainer,
            PackageIndexer indexer,
            ILogger<ProcessCatalogLeafItem> logger)
        {
            _clientFactory = clientFactory;
            _packages = packages;
            _blobContainer = blobContainer;
            _indexer = indexer;
            _logger = logger;
        }

        public async Task ProcessAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken = default)
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

        public Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        private async Task ProcessPackageDetailsAsync(
            CatalogLeafItem catalogLeafItem,
            CancellationToken cancellationToken)
        {
            var catalogClient = await _clientFactory.CreateCatalogClientAsync(cancellationToken);
            var catalogLeaf = await catalogClient.GetPackageDetailsLeafAsync(catalogLeafItem.CatalogLeafUrl, cancellationToken);

            await IndexPackageAsync(catalogLeaf, cancellationToken);
        }

        private async Task ProcessPackageDeleteAsync(
            CatalogLeafItem catalogLeafItem,
            CancellationToken cancellationToken)
        {
            await _packages.UnlistPackageAsync(catalogLeafItem.PackageId, catalogLeafItem.ParsePackageVersion());
        }

        private async Task IndexPackageAsync(
            PackageDetailsCatalogLeaf catalogLeaf,
            CancellationToken cancellationToken)
        {
            var packageId = catalogLeaf.PackageId;
            var packageVersion = catalogLeaf.ParsePackageVersion();

            Package package;
            Stream packageStream = null;
            Stream readmeStream = null;

            try
            {
                var contentClient = await _clientFactory.CreatePackageContentClientAsync(cancellationToken);
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
                        using (var stream = await reader.GetReadmeAsync(cancellationToken))
                        {
                            readmeStream = await stream.AsTemporaryFileStreamAsync(cancellationToken);
                        }
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

        private async Task IndexPackageAsync(
            Package package,
            Stream readmeStreamOrNull,
            CancellationToken cancellationToken)
        {
            var packageId = package.Id.ToLowerInvariant();
            var packageVersion = package.Version.ToNormalizedString().ToLowerInvariant();

            if (readmeStreamOrNull != null)
            {
                _logger.LogInformation(
                    "Uploading readme for package {PackageId} {PackageVersion}...",
                    packageId,
                    packageVersion);

                var blob = _blobContainer.GetBlockBlobReference($"v3/package/{packageId}/{packageVersion}/readme");

                blob.Properties.ContentType = "text/markdown";
                blob.Properties.CacheControl = "max-age=120, must-revalidate";

                await blob.UploadFromStreamAsync(readmeStreamOrNull, cancellationToken);

                _logger.LogInformation(
                    "Uploaded readme for package {PackageId} {PackageVersion}",
                    packageId,
                    packageVersion);
            }

            // Try to add the package to the database. If the package already exists,
            // the add operation will fail and we will update the package's metadata instead.
            var addResult = await _packages.AddAsync(package);
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

            // Lastly, use the database to update static resources and the search service.
            await _indexer.BuildAsync(package.Id);
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
                    await _packages.RelistPackageAsync(packageId, packageVersion);
                }
                else
                {
                    await _packages.UnlistPackageAsync(packageId, packageVersion);
                }
            }

            _logger.LogInformation(
                "Updated package {PackageId} {PackageVersion} in database",
                packageId,
                packageVersion);
        }
    }
}
