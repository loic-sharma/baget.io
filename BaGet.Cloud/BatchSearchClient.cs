using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class BatchSearchClient
    {
        private readonly AzureSearchBatchIndexer _indexer;
        private readonly ILogger<BatchSearchClient> _logger;

        private readonly List<IndexAction<KeyedDocument>> _indexActions;

        protected BatchSearchClient()
        {
        }

        public BatchSearchClient(
            AzureSearchBatchIndexer indexer,
            ILogger<BatchSearchClient> logger)
        {
            _indexer = indexer;
            _logger = logger;

            _indexActions = new List<IndexAction<KeyedDocument>>();
        }

        public virtual bool TryAdd(IndexAction<KeyedDocument> indexAction)
        {
            if (_indexActions.Count >= AzureSearchBatchIndexer.MaxBatchSize)
            {
                return false;
            }

            _indexActions.Add(indexAction);

            return true;
        }

        public virtual async Task AddAsync(IndexAction<KeyedDocument> indexAction, CancellationToken cancellationToken)
        {
            await FlushAsync(onlyFull: true, cancellationToken);

            _indexActions.Add(indexAction);
        }

        public virtual async Task FlushAsync(bool onlyFull, CancellationToken cancellationToken)
        {
            if (_indexActions.Count > 0)
            {
                if (!onlyFull || _indexActions.Count >= AzureSearchBatchIndexer.MaxBatchSize)
                {
                    _logger.LogDebug("Executing batch of {Count} index actions...", _indexActions.Count);

                    await _indexer.IndexAsync(_indexActions, cancellationToken);
                    _indexActions.Clear();
                }
            }
        }
    }
}
