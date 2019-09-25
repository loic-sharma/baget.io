using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace baget.io.functions
{
    public class CatalogLeafProcessor
    {
        private readonly ICatalogLeafItemProcessor _leafProcessor;

        public CatalogLeafProcessor(ICatalogLeafItemProcessor leafProcessor)
        {
            _leafProcessor = leafProcessor;
        }

        [FunctionName("ProcessCatalogLeaf")]
        public async Task Run(
            [QueueTrigger("catalog-leafs", Connection = "StorageQueueConnectionString")]
            string message,
            ILogger log,
            CancellationToken cancellationToken)
        {
            var leafItem = JsonConvert.DeserializeObject<CatalogLeafItem>(message);

            await _leafProcessor.ProcessAsync(leafItem, cancellationToken);
        }
    }
}
