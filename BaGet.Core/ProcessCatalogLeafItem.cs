using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Core;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using NuGet.Packaging;

namespace baget.io
{
    public class ProcessCatalogLeafItem : ICatalogLeafItemProcessor
    {
        private readonly NuGetClientFactory _clientFactory;
        private readonly IPackageService _packages;
        private readonly ILogger<ProcessCatalogLeafItem> _logger;

        public ProcessCatalogLeafItem(
            NuGetClientFactory clientFactory,
            IPackageService packages,
            ILogger<ProcessCatalogLeafItem> logger)
        {
            _clientFactory = clientFactory;
            _packages = packages;
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

        private async Task ProcessPackageDetailsAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken)
        {
            var packageMetadata = await GetPackageMetadata(catalogLeafItem, cancellationToken);
            var result = await _packages.AddAsync(packageMetadata);

            switch (result)
            {
                // The package has been added successfully, nothing else to do.
                case PackageAddResult.Success:
                    return;

                // The package already exists. Update its metadata if necessary.
                case PackageAddResult.PackageAlreadyExists:
                    await UpdatePackageMetadata(catalogLeafItem, packageMetadata, cancellationToken);
                    return;

                default:
                    throw new NotSupportedException($"Unknown add result '{result}'");
            }
        }

        private async Task ProcessPackageDeleteAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken)
        {
            await _packages.UnlistPackageAsync(catalogLeafItem.PackageId, catalogLeafItem.ParsePackageVersion());
        }

        private async Task<Package> GetPackageMetadata(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken)
        {
            var contentClient = await _clientFactory.CreatePackageContentClientAsync(cancellationToken);
            var catalogClient = await _clientFactory.CreateCatalogClientAsync(cancellationToken);

            var id = catalogLeafItem.PackageId;
            var version = catalogLeafItem.ParsePackageVersion();
            var catalogLeaf = await catalogClient.GetPackageDetailsLeafAsync(catalogLeafItem.CatalogLeafUrl, cancellationToken);

            Stream packageStream = null;

            try
            {
                using (var stream = await contentClient.GetPackageContentStreamOrNullAsync(id, version, cancellationToken))
                {
                    packageStream = await stream.AsTemporaryFileStreamAsync(cancellationToken);
                }

                _logger.LogInformation(
                    "Downloaded package {PackageId} {PackageVersion}, building metadata...",
                    id,
                    version);

                using (var reader = new PackageArchiveReader(packageStream))
                {
                    var package = reader.GetPackageMetadata();

                    package.Listed = catalogLeaf.IsListed();
                    package.Published = catalogLeaf.Published.UtcDateTime;

                    return package;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(
                    e,
                    "Failed to process package {PackageId} {PackageVersion}",
                    id,
                    version);

                throw;
            }
            finally
            {
                packageStream?.Dispose();
            }
        }

        private async Task UpdatePackageMetadata(CatalogLeafItem catalogLeafItem, Package latestPackage, CancellationToken cancellationToken)
        {
            var packageId = catalogLeafItem.PackageId;
            var packageVersion = catalogLeafItem.ParsePackageVersion();

            _logger.LogInformation(
                "Updating package {PackageId} {PackageVersion}..",
                packageId,
                packageVersion);

            var currentPackage = await _packages.FindOrNullAsync(
                packageId,
                packageVersion,
                includeUnlisted: true,
                cancellationToken);

            if (currentPackage == null)
            {
                _logger.LogError(
                    "Could not update package {PackageId} {PackageVersion} because it does not exist",
                    packageId,
                    packageVersion);

                return;
            }

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
                "Updated package {PackageId} {PackageVersion}",
                packageId,
                packageVersion);
        }
    }
}
