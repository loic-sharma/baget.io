using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;

namespace BaGet
{
    public class DefaultCatalogLeafItemBatchProcessor : ICatalogLeafItemBatchProcessor
    {
        private readonly ICatalogLeafItemProcessor _leafProcessor;

        public DefaultCatalogLeafItemBatchProcessor(ICatalogLeafItemProcessor leafProcessor)
        {
            _leafProcessor = leafProcessor;
        }

        public async Task ProcessAsync(IEnumerable<CatalogLeafItem> catalogLeafItems, CancellationToken cancellationToken = default)
        {
            await ParallelAsync.RunAsync(
                new ConcurrentBag<CatalogLeafItem>(catalogLeafItems),
                _leafProcessor.ProcessAsync,
                cancellationToken);

            await _leafProcessor.CompleteAsync(cancellationToken);
        }
    }
}
