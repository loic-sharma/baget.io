using System;
using System.Collections;
using System.Collections.Generic;

namespace BaGet.Cloud
{
    // Based off: https://github.com/NuGet/NuGet.Services.Metadata/blob/e921a7eef360ee6bcc11b7ca9f94d96e93193a1c/src/NuGet.Services.AzureSearch/AuxiliaryFiles/DownloadData.cs
    public class DownloadData : IReadOnlyDictionary<string, DownloadByVersionData>
    {
        private readonly Dictionary<string, DownloadByVersionData> _ids
            = new Dictionary<string, DownloadByVersionData>(StringComparer.OrdinalIgnoreCase);

        public long GetDownloadCount(string id)
        {
            if (!_ids.TryGetValue(id, out var versionData))
            {
                return 0;
            }

            return versionData.Total;
        }

        public bool TryGetDownloadCount(string id, out long downloadCount)
        {
            if (!_ids.TryGetValue(id, out var versionData))
            {
                downloadCount = 0;
                return false;
            }

            downloadCount = versionData.Total;
            return true;
        }

        public long GetDownloadCount(string id, string version)
        {
            if (!_ids.TryGetValue(id, out var versionData))
            {
                return 0;
            }

            return versionData.GetDownloadCount(version);
        }

        public bool TryGetDownloadCount(string id, string version, out long downloadCount)
        {
            if (!_ids.TryGetValue(id, out var versionData))
            {
                downloadCount = 0;
                return false;
            }

            return versionData.TryGetValue(version, out downloadCount);
        }

        public void SetDownloadCount(string id, string version, long downloads)
        {
            if (downloads < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(downloads), "The download count must not be negative.");
            }

            if (_ids.TryGetValue(id, out var versions))
            {
                // Remove the previous version so that the latest case is retained. IDs are case insensitive but we
                // should try to respect the latest intent.
                _ids.Remove(id);
            }
            else
            {
                versions = new DownloadByVersionData();
            }

            versions.SetDownloadCount(version, downloads);

            // Only store the download count if the value is not zero.
            if (versions.Total != 0)
            {
                _ids.Add(id, versions);
            }
        }

        public IEnumerable<string> Keys => _ids.Keys;
        public IEnumerable<DownloadByVersionData> Values => _ids.Values;
        public int Count => _ids.Count;
        public DownloadByVersionData this[string key] => _ids[key];
        public IEnumerator<KeyValuePair<string, DownloadByVersionData>> GetEnumerator() => _ids.GetEnumerator();
        public bool TryGetValue(string key, out DownloadByVersionData value) => _ids.TryGetValue(key, out value);
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public bool ContainsKey(string key) => _ids.ContainsKey(key);
    }
}
