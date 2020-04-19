using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure;
using BaGet.Core;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace BaGet
{
    public class PackageIndexer
    {
        internal static readonly JsonSerializer Serializer = JsonSerializer.Create(JsonSettings);

        internal static readonly JsonSerializerSettings JsonSettings = new JsonSerializerSettings
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            DateParseHandling = DateParseHandling.DateTimeOffset,
            NullValueHandling = NullValueHandling.Ignore,
        };

        private readonly IPackageService _packages;
        private readonly IUrlGenerator _url;
        private readonly CloudBlobContainer _blobContainer;
        private readonly RegistrationBuilder _registrationBuilder;
        private readonly IndexActionBuilder _actionBuilder;
        private readonly AzureSearchBatchIndexer _search;
        private readonly ILogger<PackageIndexer> _logger;

        public PackageIndexer(
            IPackageService packages,
            IUrlGenerator url,
            CloudBlobContainer blobContainer,
            RegistrationBuilder registrationBuilder,
            IndexActionBuilder actionBuilder,
            AzureSearchBatchIndexer search,
            ILogger<PackageIndexer> logger)
        {
            _packages = packages;
            _url = url;
            _blobContainer = blobContainer;
            _registrationBuilder = registrationBuilder;
            _actionBuilder = actionBuilder;
            _search = search;
            _logger = logger;
        }

        public async Task BuildAsync(string packageId, CancellationToken cancellationToken)
        {
            var packages = await _packages.FindAsync(packageId, includeUnlisted: true, cancellationToken);
            if (!packages.Any())
            {
                _logger.LogError("Could not index package {PackageId} because it does not exist", packageId);
                return;
            }

            var packageRegistration = new PackageRegistration(
                packageId,
                packages);

            // Update the package metadata resource.
            _logger.LogInformation(
                "Updating the package metadata resource for {PackageId}...",
                packageId);

            var index = _registrationBuilder.BuildIndex(packageRegistration);

            await UploadRegistrationIndexAsync(packageId, index, cancellationToken);

            // Update the search service.
            _logger.LogInformation(
                "Updating the search service for {PackageId}...",
                packageId);

            var actions = _actionBuilder.UpdatePackage(packageRegistration);

            await _search.IndexAsync(actions, cancellationToken);

            _logger.LogInformation("Indexed package {PackageId}", packageId);
        }

        private async Task UploadRegistrationIndexAsync(
            string packageId,
            BaGetRegistrationIndexResponse registrationIndex,
            CancellationToken cancellationToken)
        {
            var blob = _blobContainer.GetBlockBlobReference(
                $"v3/registration/{packageId.ToLowerInvariant()}/index.json");

            blob.Properties.ContentEncoding = "gzip";
            blob.Properties.ContentType = "application/json";
            blob.Properties.CacheControl = "max-age=120, must-revalidate";

            using (var memoryStream = new MemoryStream())
            {
                using (var zipStream = new GZipStream(memoryStream, CompressionMode.Compress, leaveOpen: true))
                using (var streamWriter = new StreamWriter(zipStream))
                using (var jsonWriter = new JsonTextWriter(streamWriter))
                {
                    Serializer.Serialize(jsonWriter, registrationIndex);
                }

                memoryStream.Position = 0;
                await blob.UploadFromStreamAsync(memoryStream, cancellationToken);
            }
        }
    }
}
