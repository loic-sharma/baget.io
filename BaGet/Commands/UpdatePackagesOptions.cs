using NuGet.Versioning;

namespace BaGet
{
    public class UpdatePackagesOptions
    {
        public string PackageId { get; set; }
        public NuGetVersion PackageVersion { get; set; }

        public bool UpdateOwners { get; set; }
        public bool UpdateDownloads { get; set; }
    }
}
