using BaGet.Core;
using Microsoft.Extensions.Options;
using NuGet.Versioning;

namespace BaGet
{
    public class UrlGenerator : IUrlGenerator
    {
        private readonly IOptionsSnapshot<Configuration> _config;

        public UrlGenerator(IOptionsSnapshot<Configuration> config)
        {
            _config = config;
        }

        public string GetAutocompleteResourceUrl()
        {
            throw new System.NotImplementedException();
        }

        public string GetPackageContentResourceUrl()
        {
            throw new System.NotImplementedException();
        }

        public string GetPackageDownloadUrl(string id, NuGetVersion version)
        {
            var packageId = id.ToLowerInvariant();
            var packageVersion = version.ToNormalizedString().ToLowerInvariant();

            return $"https://api.nuget.org/v3-flatcontainer/{packageId}/{packageVersion}/{packageId}.{packageVersion}.nupkg";
        }

        public string GetPackageManifestDownloadUrl(string id, NuGetVersion version)
        {
            var packageId = id.ToLowerInvariant();
            var packageVersion = version.ToNormalizedString().ToLowerInvariant();

            return $"https://api.nuget.org/v3-flatcontainer/{packageId}/{packageVersion}/{packageId}.{packageVersion}.nuspec";
        }

        public string GetPackageMetadataResourceUrl()
        {
            throw new System.NotImplementedException();
        }

        public string GetPackagePublishResourceUrl()
        {
            throw new System.NotImplementedException();
        }

        public string GetPackageVersionsUrl(string id)
        {
            throw new System.NotImplementedException();
        }

        public string GetRegistrationIndexUrl(string id)
        {
            var packageId = id.ToLowerInvariant();

            return $"{_config.Value.RootUrl}/v3/registration/{packageId}.json";
        }

        public string GetRegistrationLeafUrl(string id, NuGetVersion version)
        {
            return GetRegistrationIndexUrl(id);
        }

        public string GetRegistrationPageUrl(string id, NuGetVersion lower, NuGetVersion upper)
        {
            throw new System.NotImplementedException();
        }

        public string GetSearchResourceUrl()
        {
            throw new System.NotImplementedException();
        }

        public string GetSymbolPublishResourceUrl()
        {
            throw new System.NotImplementedException();
        }
    }
}
