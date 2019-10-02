using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;

namespace BaGet.Functions
{
    public class Functions
    {
        private readonly ProcessCatalogLeafItem _leafProcessor;
        private readonly PackageIndexer _indexer;

        public Functions(
            ProcessCatalogLeafItem leafProcessor,
            PackageIndexer indexer)
        {
            _leafProcessor = leafProcessor;
            _indexer = indexer;
        }

        [FunctionName("ProcessMessage")]
        public async Task ProcessMessage(
            [ServiceBusTrigger("index", Connection = "ServiceBusConnectionString")]
            string message,
            string label,
            CancellationToken cancellationToken)
        {
            switch (label)
            {
                case "catalog-leaf":
                    var leafItem = JsonConvert.DeserializeObject<CatalogLeafItem>(message);

                    await _leafProcessor.ProcessAsync(leafItem, cancellationToken);
                    await _leafProcessor.CompleteAsync(cancellationToken);
                    break;

                case "packageId":
                    await _indexer.BuildAsync(message, cancellationToken);
                    break;
            }
        }
    }
}
