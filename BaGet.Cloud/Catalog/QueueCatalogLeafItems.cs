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
        private const int MaxBatchElements = 10_000;

        private readonly BatchQueueClient _queue;
        private readonly ILogger<QueueCatalogLeafItems> _logger;

        private readonly JsonSerializer _serializer;

        public QueueCatalogLeafItems(BatchQueueClient queue, ILogger<QueueCatalogLeafItems> logger)
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
                    _queue.SendAsync,
                    cancellationToken);
            }
            else
            {
                await _queue.SendAsync(messages, cancellationToken);
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
