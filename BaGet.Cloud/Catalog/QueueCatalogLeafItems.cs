using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BaGet
{
    public class QueueCatalogLeafItems : ICatalogLeafItemBatchProcessor
    {
        private const long MaxBatchSizeBytes = 262000;
        private const int MaxBatchElements = 10_000;

        private readonly IQueueClient _queue;
        private readonly ILogger<QueueCatalogLeafItems> _logger;

        private readonly JsonSerializer _serializer;
        private int _headerSizeEstimateBytes = 100;

        public QueueCatalogLeafItems(IQueueClient queue, ILogger<QueueCatalogLeafItems> logger)
        {
            _queue = queue;
            _logger = logger;

            _serializer = new JsonSerializer();
        }

        public async Task ProcessAsync(
            IEnumerable<CatalogLeafItem> catalogLeafItems,
            CancellationToken cancellationToken = default)
        {
            var messages = catalogLeafItems.Select(ToMessage).ToList();

            // Split large message batches and process them in parallel.
            if (messages.Count > MaxBatchElements)
            {
                var messageBatches = SplitMessages(messages);

                await ParallelHelper.ProcessInParallel(
                    messageBatches,
                    ProcessInternalAsync,
                    cancellationToken);
            }
            else
            {
                await ProcessInternalAsync(messages, cancellationToken);
            }

            _logger.LogInformation("Completed enqueueing messages");
            await _queue.CloseAsync();
        }

        private async Task ProcessInternalAsync(
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

        private ConcurrentBag<IReadOnlyList<Message>> SplitMessages(IReadOnlyList<Message> messages)
        {
            var result = new ConcurrentBag<IReadOnlyList<Message>>();

            for (var i = 0; i < messages.Count; i += MaxBatchElements)
            {
                var batchSize = Math.Min(MaxBatchElements, messages.Count - i);
                var batch = messages.Skip(i).Take(batchSize).ToList();

                result.Add(batch);
            }

            return result;
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

        private Message ToMessage(CatalogLeafItem catalogLeafItem)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    _serializer.Serialize(writer, catalogLeafItem);
                }

                return new Message
                {
                    Body = stream.ToArray(),
                    Label = "catalog-leaf",
                    ContentType = "application/json;charset=unicode",
                };
            }
        }
    }
}
