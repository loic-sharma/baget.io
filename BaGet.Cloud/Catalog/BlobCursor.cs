using System;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Catalog;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace BaGet
{
    public class BlobCursor : ICursor
    {
        private readonly CloudBlockBlob _blob;

        public BlobCursor(CloudBlobContainer blobContainer)
        {
            if (blobContainer == null) throw new ArgumentNullException(nameof(blobContainer));

            _blob = blobContainer.GetBlockBlobReference("cursor.json");
        }

        public async Task<DateTimeOffset?> GetAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                var jsonString = await _blob.DownloadTextAsync(cancellationToken);
                var data = JsonConvert.DeserializeObject<Data>(jsonString);

                return data.Value;
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                return null;
            }
        }

        public async Task SetAsync(DateTimeOffset value, CancellationToken cancellationToken = default)
        {
            var data = new Data { Value = value };
            var jsonString = JsonConvert.SerializeObject(data);

            await _blob.UploadTextAsync(jsonString);
        }

        private class Data
        {
            [JsonProperty("value")]
            public DateTimeOffset Value { get; set; }
        }
    }
}
