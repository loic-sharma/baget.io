using System.Threading;
using System.Threading.Tasks;

namespace BaGet
{
    public interface ICommand
    {
        Task RunAsync(CancellationToken cancellationToken = default);
    }
}
