using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class BatchTableClient
    {
        private const string TableName = "Packages";
        private const int MaxTableOperations = 100;

        private readonly CloudTable _table;
        private readonly ILogger<BatchTableClient> _logger;

        private TableBatchOperation _tableOperations;

        protected BatchTableClient()
        {
        }

        public BatchTableClient(
            CloudTableClient tableClient,
            ILogger<BatchTableClient> logger)
        {
            _table = tableClient.GetTableReference(TableName);
            _logger = logger;

            _tableOperations = new TableBatchOperation();
        }

        public virtual bool TryAdd(TableOperation operation)
        {
            if (_tableOperations.Count >= MaxTableOperations)
            {
                return false;
            }

            _tableOperations.Add(operation);

            return true;
        }

        public virtual async Task AddAsync(TableOperation operation, CancellationToken cancellationToken)
        {
            await FlushAsync(onlyFull: true, cancellationToken);

            _tableOperations.Add(operation);
        }

        public virtual async Task FlushAsync(bool onlyFull, CancellationToken cancellationToken)
        {
            if (_tableOperations.Count > 0)
            {
                if (!onlyFull || _tableOperations.Count >= MaxTableOperations)
                {
                    _logger.LogDebug("Executing batch of {Count} table operations...", _tableOperations.Count);

                    await _table.ExecuteBatchAsync(_tableOperations, cancellationToken);
                    _tableOperations = new TableBatchOperation();
                }
            }
        }
    }
}
