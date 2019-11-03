using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BaGet.Cloud
{
    // Based off: https://github.com/NuGet/NuGet.Services.Metadata/blob/e921a7eef360ee6bcc11b7ca9f94d96e93193a1c/src/NuGet.Services.AzureSearch/AuxiliaryFiles/DownloadDataClient.cs#L17
    public class DownloadDataClient
    {
        private const string DownloadsV1Url = "https://nugetprod0.blob.core.windows.net/ng-search-data/downloads.v1.json";

        private readonly HttpClient _httpClient;
        private readonly ILogger<DownloadDataClient> _logger;

        protected DownloadDataClient()
        {
        }

        public DownloadDataClient(HttpClient httpClient, ILogger<DownloadDataClient> logger)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public virtual async Task<DownloadData> GetAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Downloading downloads.v1.json...");

            using (var response = await _httpClient.GetAsync(DownloadsV1Url, cancellationToken))
            {
                response.EnsureSuccessStatusCode();

                using (var stream = await response.Content.ReadAsStreamAsync())
                {
                    var downloads = new DownloadData();

                    ReadStream(
                        stream,
                        (id, version, downloadCount) =>
                        {
                            //id = stringCache.Dedupe(id);
                            //version = stringCache.Dedupe(version);
                            downloads.SetDownloadCount(id, version, downloadCount);
                        });

                    return downloads;
                }
            }
        }

        private static void ReadStream(
            Stream stream,
            Action<string, string, long> addVersion)
        {
            using (var textReader = new StreamReader(stream))
            using (var jsonReader = new JsonTextReader(textReader))
            {
                Assert(jsonReader.Read(), "The blob should be readable.");
                Assert(jsonReader.TokenType == JsonToken.StartArray, "The first token should be the start of an array.");
                Assert(jsonReader.Read(), "There should be a token after the start of an array.");

                while (jsonReader.TokenType == JsonToken.StartArray)
                {
                    Assert(jsonReader.Read(), "There should be a token after the start of an ID array.");
                    Assert(jsonReader.TokenType == JsonToken.String, "The token after the start of an ID array should be a string.");

                    // We assume the package ID has valid characters.
                    var id = (string)jsonReader.Value;

                    Assert(jsonReader.Read(), "There should be a token after the package ID.");

                    while (jsonReader.TokenType == JsonToken.StartArray)
                    {
                        Assert(jsonReader.Read(), "There should be a token after the start of a version array.");
                        Assert(jsonReader.TokenType == JsonToken.String, "The token after the start of a version array should be a string.");

                        // We assume the package version is already normalized.
                        var version = (string)jsonReader.Value;

                        Assert(jsonReader.Read(), "There should be a token after the package version.");
                        Assert(jsonReader.TokenType == JsonToken.Integer, "The token after the package version should be an integer.");

                        var downloads = (long)jsonReader.Value;

                        Assert(jsonReader.Read(), "There should be a token after the download count.");
                        Assert(jsonReader.TokenType == JsonToken.EndArray, "The token after the download count should be the end of the version array.");
                        Assert(jsonReader.Read(), "There should be a token after the version array.");

                        addVersion(id, version, downloads);
                    }

                    Assert(jsonReader.TokenType == JsonToken.EndArray, "The token after the package versions should be the end of the ID array.");
                    Assert(jsonReader.Read(), "There should be a token after the end of the ID array.");
                }

                Assert(jsonReader.TokenType == JsonToken.EndArray, "The last token should be the end of an array.");
                Assert(!jsonReader.Read(), "There should be no token after the end of the object.");
            }
        }

        private static void Assert(bool condition, string message)
        {
            if (!condition)
            {
                throw new InvalidOperationException(message);
            }
        }
    }
}
