using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace baget.io
{
    public class QueueCatalogLeafItem : ICatalogLeafItemProcessor
    {
        private readonly CloudQueue _queue;
        private readonly ILogger<QueueCatalogLeafItem> _logger;

        private readonly JsonSerializer _serializer;

        public QueueCatalogLeafItem(CloudQueue queue, ILogger<QueueCatalogLeafItem> logger)
        {
            _queue = queue;
            _logger = logger;

            _serializer = new JsonSerializer();
        }

        public async Task ProcessAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Processing catalog leaf {CatalogLeafUrl}", catalogLeafItem.CatalogLeafUrl);

            await _queue.AddMessageAsync(ToMessage(catalogLeafItem));

            _logger.LogInformation("Processed catalog leaf {CatalogLeafUrl}", catalogLeafItem.CatalogLeafUrl);
        }

        private CloudQueueMessage ToMessage(CatalogLeafItem catalogLeafItem)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    _serializer.Serialize(writer, catalogLeafItem);
                }

                return new CloudQueueMessage(stream.ToArray());
            }
        }
    }
}
