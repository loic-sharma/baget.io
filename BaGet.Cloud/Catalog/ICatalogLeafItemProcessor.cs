using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;

namespace BaGet
{
    public interface ICatalogLeafItemProcessor
    {
        Task ProcessAsync(CatalogLeafItem catalogLeafItem, CancellationToken cancellationToken = default);

        Task CompleteAsync(CancellationToken cancellationToken = default);
    }
}
