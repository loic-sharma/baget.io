using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;

namespace BaGet.Functions
{
    public class CatalogLeafProcessor
    {
        private readonly ProcessCatalogLeafItem _leafProcessor;

        public CatalogLeafProcessor(ProcessCatalogLeafItem leafProcessor)
        {
            _leafProcessor = leafProcessor;
        }

        [FunctionName("ProcessCatalogLeaf")]
        public async Task Run(
            [ServiceBusTrigger("index", Connection = "ServiceBusConnectionString")]
            string message,
            CancellationToken cancellationToken)
        {
            var leafItem = JsonConvert.DeserializeObject<CatalogLeafItem>(message);

            await _leafProcessor.ProcessAsync(leafItem, cancellationToken);
            await _leafProcessor.CompleteAsync(cancellationToken);
        }
    }
}
