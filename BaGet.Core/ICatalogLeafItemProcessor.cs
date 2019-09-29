using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;

namespace baget.io
{
    public interface ICatalogLeafItemProcessor
    {
        Task ProcessAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken = default);
    }
}
