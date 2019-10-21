using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaGet
{
    public static class ParallelAsync
    {
        public const int MaxDegreeOfConcurrency = 32;
        private const int MaxRetries = 3;

        public static async Task RunAsync<T>(
            ConcurrentBag<T> allWork,
            Func<T, CancellationToken, Task> worker,
            CancellationToken cancellationToken = default)
        {
            await RepeatAsync(async () =>
            {
                while (allWork.TryTake(out var item))
                {
                    var attempt = 0;

                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            await worker(item, cancellationToken);
                            break;
                        }
                        catch (Exception) when (attempt < MaxRetries)
                        {
                            attempt++;
                        }
                    }
                }
            });
        }

        public static async Task RepeatAsync(Func<Task> taskFactory, int? degreesOfConcurrency = null)
        {
             await Task.WhenAll(
                Enumerable
                    .Repeat(taskFactory, degreesOfConcurrency ?? MaxDegreeOfConcurrency)
                    .Select(f => f()));
        }
    }
}
