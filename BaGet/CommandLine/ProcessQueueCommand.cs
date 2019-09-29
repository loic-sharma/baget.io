using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace BaGet
{
    public class ProcessQueueCommand
    {
        private const int MaxDegreeOfParallelism = 32;

        private readonly CloudQueue _queue;
        private readonly ICatalogLeafItemProcessor _leafProcessor;
        private readonly ILogger<ProcessQueueCommand> _logger;

        public ProcessQueueCommand(
            CloudQueue queue,
            ICatalogLeafItemProcessor leafProcessor,
            ILogger<ProcessQueueCommand> logger)
        {
            _queue = queue;
            _leafProcessor = leafProcessor;
            _logger = logger;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // Prepare the processing.
            ThreadPool.SetMinThreads(MaxDegreeOfParallelism, completionPortThreads: 4);
            ServicePointManager.DefaultConnectionLimit = MaxDegreeOfParallelism;
            ServicePointManager.MaxServicePointIdleTime = 10000;

            var channelOptions = new BoundedChannelOptions(MaxDegreeOfParallelism * 2)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = false,
            };

            var channel = Channel.CreateBounded<CloudQueueMessage>(channelOptions);

            var receiveTask = ReceiveQueueMessages(channel.Writer, cancellationToken);
            var processTask = ProcessInParallel(async () =>
            {
                while (await channel.Reader.WaitToReadAsync(cancellationToken))
                {
                    while (channel.Reader.TryRead(out var message))
                    {
                        try
                        {
                            var catalogLeafItem = JsonConvert.DeserializeObject<CatalogLeafItem>(message.AsString);

                            await _leafProcessor.ProcessAsync(catalogLeafItem, cancellationToken);
                            await _queue.DeleteMessageAsync(message, cancellationToken);
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(e, "Failed to process message due to exception");
                        }
                    }
                }
            });

            await Task.WhenAll(receiveTask, processTask);
        }

        private async Task ReceiveQueueMessages(ChannelWriter<CloudQueueMessage> channel, CancellationToken cancellationToken)
        {
            while (await channel.WaitToWriteAsync())
            {
                var message = await _queue.GetMessageAsync(cancellationToken);
                if (message == null)
                {
                    _logger.LogInformation("Finished processing queue messages");
                    channel.Complete();
                    return;
                }

                if (!channel.TryWrite(message))
                {
                    await channel.WriteAsync(message, cancellationToken);
                }
            }
        }

        private static async Task ProcessInParallel(Func<Task> worker)
        {
             await Task.WhenAll(
                Enumerable
                    .Repeat(worker, MaxDegreeOfParallelism)
                    .Select(x => x()));
        }
    }
}
