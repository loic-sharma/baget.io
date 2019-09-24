using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace baget.io.functions
{
    public static class CatalogLeafProcessor
    {
        [FunctionName("ProcessCatalogLeaf")]
        public static void Run(
            [QueueTrigger("catalog-leafs", Connection = "")]
            string myQueueItem,
            ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
        }
    }
}
