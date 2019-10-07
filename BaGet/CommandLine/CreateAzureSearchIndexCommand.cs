using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure.Search;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;

namespace BaGet
{
    public class CreateAzureSearchIndexCommand
    {
        private readonly ISearchServiceClient _searchClient;
        private readonly ILogger<CreateAzureSearchIndexCommand> _logger;

        public CreateAzureSearchIndexCommand(
            ISearchServiceClient searchClient,
            ILogger<CreateAzureSearchIndexCommand> logger)
        {
            _searchClient = searchClient ?? throw new ArgumentNullException(nameof(searchClient));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            if (await _searchClient.Indexes.ExistsAsync(PackageDocument.IndexName))
            {
                _logger.LogInformation("Search index already exists");
                return;
            }

            _logger.LogInformation("Search index does not exist, creating...");

            var index = new Index
            {
                Name = PackageDocument.IndexName,
                Fields = FieldBuilder.BuildForType<PackageDocument>(),
                Analyzers = new List<Analyzer>
                {
                    ExactMatchCustomAnalyzer.Instance
                }
            };

            await _searchClient.Indexes.CreateAsync(index, cancellationToken: cancellationToken);

            _logger.LogInformation("Created search index");
        }
    }
}
