using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure.Search;
using BaGet.Core;
using BaGet.Protocol.Models;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
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
        private readonly ISearchIndexClient _searchClient;
        private readonly ILogger<PackageIndexer> _logger;

        public PackageIndexer(
            IPackageService packages,
            IUrlGenerator url,
            CloudBlobContainer blobContainer,
            ISearchIndexClient searchClient,
            ILogger<PackageIndexer> logger)
        {
            _packages = packages;
            _url = url;
            _blobContainer = blobContainer;
            _searchClient = searchClient;
            _logger = logger;
        }

        public async Task BuildAsync(string packageId, CancellationToken cancellationToken = default)
        {
            var packages = await _packages.FindAsync(packageId, includeUnlisted: true);
            if (!packages.Any())
            {
                _logger.LogError("Could not index package {PackageId} because it does not exist", packageId);
                return;
            }

            // Update the package metadata resource.
            _logger.LogInformation(
                "Updating the package metadata resource for {PackageId}...",
                packageId);

            await UploadRegistrationIndexAsync(
                packageId,
                BuildRegistrationIndex(packageId, packages),
                cancellationToken);

            // Update the search service.
            _logger.LogInformation(
                "Updating the search service for {PackageId}...",
                packageId);

            await _searchClient.Documents.IndexAsync(
                BuildSearchActions(packageId, packages),
                cancellationToken: cancellationToken);

            _logger.LogInformation("Indexed package {PackageId}", packageId);
        }

        private BaGetRegistrationIndexResponse BuildRegistrationIndex(
            string packageId,
            IReadOnlyList<Package> packages)
        {
            var versions = packages.Select(p => p.Version).ToList();

            return new BaGetRegistrationIndexResponse
            {
                RegistrationIndexUrl = _url.GetRegistrationIndexUrl(packageId),
                Type = RegistrationIndexResponse.DefaultType,
                Count = 1,
                TotalDownloads = packages.Sum(p => p.Downloads),
                Pages = new[]
                {
                    new RegistrationIndexPage
                    {
                        RegistrationPageUrl = _url.GetRegistrationIndexUrl(packages.First().Id),
                        Count = packages.Count(),
                        Lower = versions.Min().ToNormalizedString().ToLowerInvariant(),
                        Upper = versions.Max().ToNormalizedString().ToLowerInvariant(),
                        ItemsOrNull = packages.Select(ToRegistrationIndexPageItem).ToList(),
                    }
                }
            };
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

        private IndexBatch<PackageDocument> BuildSearchActions(
            string packageId,
            IReadOnlyList<Package> packages)
        {
            var searchDocuments = BuildSearchDocuments(packageId, packages);

            return IndexBatch.New(searchDocuments.Select(IndexAction.Upload));
        }

        private IReadOnlyList<PackageDocument> BuildSearchDocuments(
            string packageId,
            IReadOnlyList<Package> packages)
        {
            var result = new List<PackageDocument>();
            for (var i = 0; i < 4; i++)
            {
                var includePrerelease = (i & 1) != 0;
                var includeSemVer2 = (i & 2) != 0;
                var searchFilters = (SearchFilters)i;

                IEnumerable<Package> filtered = packages;
                if (!includePrerelease)
                {
                    filtered = filtered.Where(p => !p.IsPrerelease);
                }

                if (!includeSemVer2)
                {
                    filtered = filtered.Where(p => p.SemVerLevel != SemVerLevel.SemVer2);
                }

                var versions = filtered.OrderBy(p => p.Version).ToList();
                if (versions.Count == 0)
                {
                    continue;
                }

                var latest = versions.Last();
                var dependencies = latest
                    .Dependencies
                    .Select(d => d.Id?.ToLowerInvariant())
                    .Where(d => d != null)
                    .Distinct()
                    .ToArray();

                var document = new PackageDocument();
                var encodedId = EncodeSearchKey(packageId.ToLowerInvariant());

                document.Key = $"{encodedId}-{searchFilters}";
                document.Id = latest.Id;
                document.Version = latest.Version.ToFullString();
                document.Description = latest.Description;
                document.Authors = latest.Authors;
                document.IconUrl = latest.IconUrlString;
                document.LicenseUrl = latest.LicenseUrlString;
                document.ProjectUrl = latest.ProjectUrlString;
                document.Published = latest.Published;
                document.Summary = latest.Summary;
                document.Tags = latest.Tags;
                document.Title = latest.Title;
                document.TotalDownloads = versions.Sum(p => p.Downloads);
                document.DownloadsMagnitude = document.TotalDownloads.ToString().Length;
                document.Versions = versions.Select(p => p.Version.ToFullString()).ToArray();
                document.VersionDownloads = versions.Select(p => p.Downloads.ToString()).ToArray();
                document.Dependencies = dependencies;
                document.PackageTypes = latest.PackageTypes.Select(t => t.Name).ToArray();
                document.Frameworks = latest.TargetFrameworks.Select(f => f.Moniker.ToLowerInvariant()).ToArray();
                document.SearchFilters = searchFilters.ToString();

                result.Add(document);
            }

            return result;
        }

        private RegistrationIndexPageItem ToRegistrationIndexPageItem(Package package) =>
            new RegistrationIndexPageItem
            {
                RegistrationLeafUrl = _url.GetRegistrationLeafUrl(package.Id, package.Version),
                PackageContentUrl = _url.GetPackageDownloadUrl(package.Id, package.Version),
                PackageMetadata = new BaGetPackageMetadata
                {
                    PackageId = package.Id,
                    Version = package.Version.ToFullString(),
                    Authors = string.Join(", ", package.Authors),
                    Description = package.Description,
                    Downloads = package.Downloads,
                    HasReadme = package.HasReadme,
                    IconUrl = package.IconUrlString,
                    Language = package.Language,
                    LicenseUrl = package.LicenseUrlString,
                    Listed = package.Listed,
                    MinClientVersion = package.MinClientVersion,
                    PackageContentUrl = _url.GetPackageDownloadUrl(package.Id, package.Version),
                    PackageTypes = package.PackageTypes.Select(t => t.Name).ToList(),
                    ProjectUrl = package.ProjectUrlString,
                    RepositoryUrl = package.RepositoryUrlString,
                    RepositoryType = package.RepositoryType,
                    Published = package.Published,
                    RequireLicenseAcceptance = package.RequireLicenseAcceptance,
                    Summary = package.Summary,
                    Tags = package.Tags,
                    Title = package.Title,
                    DependencyGroups = ToDependencyGroups(package)
                },
            };

        private IReadOnlyList<DependencyGroupItem> ToDependencyGroups(Package package)
        {
            return package.Dependencies
                .GroupBy(d => d.TargetFramework)
                .Select(group => new DependencyGroupItem
                {
                    TargetFramework = group.Key,

                    // A package that supports a target framework but does not have dependencies while on
                    // that target framework is represented by a fake dependency with a null "Id" and "VersionRange".
                    // This fake dependency should not be included in the output.
                    Dependencies = group
                        .Where(d => d.Id != null && d.VersionRange != null)
                        .Select(d => new DependencyItem
                        {
                            Id = d.Id,
                            Range = d.VersionRange
                        })
                        .ToList()
                })
                .ToList();
        }

        private string EncodeSearchKey(string searchKey)
        {
            // Keys can only contain letters, digits, underscore(_), dash(-), or equal sign(=).
            var bytes = Encoding.UTF8.GetBytes(searchKey);
            var base64 = Convert.ToBase64String(bytes);

            return base64.Replace('+', '-').Replace('/', '_');
        }
    }
}
