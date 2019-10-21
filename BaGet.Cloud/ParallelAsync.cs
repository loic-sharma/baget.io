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
            CancellationToken cancellationToken)
        {
            await RunAsync(allWork, worker, MaxDegreeOfConcurrency, cancellationToken);
        }

        public static async Task RunAsync<T>(
            ConcurrentBag<T> allWork,
            Func<T, CancellationToken, Task> worker,
            int degreesOfConcurrency,
            CancellationToken cancellationToken)
        {
             await Task.WhenAll(
                Enumerable
                    .Repeat(allWork, degreesOfConcurrency)
                    .Select(async work =>
                    {
                        while (work.TryTake(out var item))
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
                    }));
        }
    }
}
