using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;

namespace BaGet
{
    public interface ICatalogLeafItemBatchProcessor
    {
        Task ProcessAsync(
            IEnumerable<CatalogLeafItem> catalogLeafItems,
            CancellationToken cancellationToken = default);
    }
}
