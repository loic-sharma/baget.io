using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class BatchQueueClient
    {
        private const long MaxBatchSizeBytes = 262000;

        private readonly IQueueClient _queue;
        private readonly ILogger<BatchQueueClient> _logger;

        private int _headerSizeEstimateBytes = 100;

        public BatchQueueClient(IQueueClient queue, ILogger<BatchQueueClient> logger)
        {
            _queue = queue;
            _logger = logger;
        }

        public async Task SendAsync(
            IReadOnlyList<Message> messages,
            CancellationToken cancellationToken = default)
        {
            var batchStart = 0;

            while (batchStart < messages.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batchEnd = FindBatchEnd(messages, batchStart);

                try
                {
                    // Create the batch of messages to send. It should include the element at index
                    // "batchStart" but not the element at index "batchEnd".
                    var batch = messages
                        .Skip(batchStart)
                        .Take(batchEnd - batchStart)
                        .ToList();

                    _logger.LogInformation(
                        "Enqueueing batch of {Messages} messages...",
                        batch.Count);

                    await _queue.SendAsync(batch);

                    _logger.LogInformation(
                        "Enqueued batch of {Messages} messages",
                        batch.Count);

                    batchStart = batchEnd;
                }
                catch (MessageSizeExceededException e)
                {
                    // The batch was too big. Increase our estimated header size and try again.
                    _headerSizeEstimateBytes = (int)(_headerSizeEstimateBytes * 1.5);

                    _logger.LogWarning(
                        e,
                        "Enqueued batch exceeded max message size. " +
                        "Increased header size estimate to {EstimatedHeaderBytes} bytes",
                        _headerSizeEstimateBytes);
                }
            }
        }

        public async Task CompleteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Completed enqueueing messages");
            await _queue.CloseAsync();
        }

        private int FindBatchEnd(IReadOnlyList<Message> messages, int batchStart)
        {
            // Start the batch by including one element. Keep increasing the batch
            // until we exceed the max size, or, until we run out of messages. Note
            // that the batch does not include the element at index "batchEnd".
            long estimatedBytes = 0;
            var batchEnd = batchStart + 1;

            while (batchEnd < messages.Count)
            {
                estimatedBytes += messages[batchEnd - 1].Size + _headerSizeEstimateBytes;

                if (estimatedBytes > MaxBatchSizeBytes)
                {
                    return batchEnd;
                }

                batchEnd++;
            }

            return batchEnd;
        }
    }
}
