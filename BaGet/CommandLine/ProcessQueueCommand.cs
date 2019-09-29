using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BaGet
{
    public class ProcessQueueCommand
    {
        private const int MaxDegreeOfParallelism = 32;

        private readonly IQueueClient _queue;
        private readonly ICatalogLeafItemProcessor _leafProcessor;
        private readonly ILogger<ProcessQueueCommand> _logger;

        public ProcessQueueCommand(
            IQueueClient queue,
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

            var options = new MessageHandlerOptions(ProcessException)
            {
                AutoComplete = true,
                MaxConcurrentCalls = MaxDegreeOfParallelism,
            };

            _queue.RegisterMessageHandler(ProcessMessageAsync, options);

            // Wait up to a day before shutting down.
            await Task.Delay(TimeSpan.FromDays(1), cancellationToken);

            _logger.LogInformation("Shutting down...");

            await _leafProcessor.CompleteAsync(cancellationToken);
        }

        private async Task ProcessMessageAsync(Message message, CancellationToken cancellationToken)
        {
            var messageString = Encoding.UTF8.GetString(message.Body);
            var catalogLeafItem = JsonConvert.DeserializeObject<CatalogLeafItem>(messageString);

            await _leafProcessor.ProcessAsync(catalogLeafItem, cancellationToken);
        }

        private Task ProcessException(ExceptionReceivedEventArgs exceptionArgs)
        {
            _logger.LogError(
                exceptionArgs.Exception,
                "Received exception from Service Bus");

            return Task.CompletedTask;
        }
    }
}
