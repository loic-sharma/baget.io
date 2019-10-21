using NuGet.Versioning;

namespace BaGet
{
    public class AddPackagesOptions
    {
        public string PackageId { get; set; }
        public NuGetVersion PackageVersion { get; set; }

        //public string Source { get; set; }
        public bool Enqueue { get; set; }
    }
}
