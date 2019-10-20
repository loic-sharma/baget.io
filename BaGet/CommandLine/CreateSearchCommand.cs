using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Azure.Search;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BaGet
{
    public class CreateSearchCommand : ICommand
    {
        private readonly ISearchServiceClient _searchClient;
        private readonly IOptionsSnapshot<Configuration> _options;
        private readonly ILogger<CreateSearchCommand> _logger;

        public CreateSearchCommand(
            ISearchServiceClient searchClient,
            IOptionsSnapshot<Configuration> options,
            ILogger<CreateSearchCommand> logger)
        {
            _searchClient = searchClient ?? throw new ArgumentNullException(nameof(searchClient));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (await _searchClient.Indexes.ExistsAsync(_options.Value.Search.IndexName))
            {
                _logger.LogError(
                    "Azure Search index {IndexName} already exists",
                    _options.Value.Search.IndexName);
                return;
            }

            await _searchClient.Indexes.CreateAsync(new Index
            {
                Name = _options.Value.Search.IndexName,
                Fields = FieldBuilder.BuildForType<PackageDocument>(),
                Analyzers = new List<Analyzer>
                {
                    ExactMatchCustomAnalyzer.Instance
                }
            });

            _logger.LogInformation(
                "Created Azure Search index {IndexName}",
                _options.Value.Search.IndexName);
        }
    }
}
