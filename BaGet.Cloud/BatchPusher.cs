using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class BatchPusher
    {
        private const string TableName = "Packages";
        private const int MaxTableOperations = 100;

        private readonly CloudTable _table;
        private readonly AzureSearchBatchIndexer _indexer;
        private readonly ILogger<BatchPusher> _logger;

        private TableBatchOperation _tableOperations;
        private readonly List<IndexAction<KeyedDocument>> _indexActions;

        protected BatchPusher()
        {
        }

        public BatchPusher(
            CloudTableClient tableClient,
            AzureSearchBatchIndexer indexer,
            ILogger<BatchPusher> logger)
        {
            _table = tableClient.GetTableReference(TableName);
            _indexer = indexer;
            _logger = logger;

            _tableOperations = new TableBatchOperation();
            _indexActions = new List<IndexAction<KeyedDocument>>();
        }

        public virtual async Task AddAsync(TableOperation operation, CancellationToken cancellationToken)
        {
            _tableOperations.Add(operation);

            await FlushAsync(
                onlyFullOperations: true,
                onlyFullActions: true,
                cancellationToken);
        }

        public virtual async Task AddAsync(IndexAction<KeyedDocument> action, CancellationToken cancellationToken)
        {
            _indexActions.Add(action);

            await FlushAsync(
                onlyFullOperations: true,
                onlyFullActions: true,
                cancellationToken);
        }

        public virtual async Task FlushAsync(
            bool onlyFullOperations,
            bool onlyFullActions,
            CancellationToken cancellationToken = default)
        {
            var operationsFull =  _tableOperations.Count >= MaxTableOperations;
            var actionsFull = _indexActions.Count >= AzureSearchBatchIndexer.MaxBatchSize;

            if (!onlyFullOperations || operationsFull || actionsFull)
            {
                _logger.LogDebug("Executing batch of {Count} table operations...", _tableOperations.Count);

                await _table.ExecuteBatchAsync(_tableOperations, cancellationToken);
                _tableOperations = new TableBatchOperation();
            }

            if (!onlyFullActions || actionsFull)
            {
                _logger.LogDebug("Executing batch of {Count} index actions...", _indexActions.Count);

                await _indexer.IndexAsync(_indexActions, cancellationToken);
                _indexActions.Clear();
            }
        }
    }
}
