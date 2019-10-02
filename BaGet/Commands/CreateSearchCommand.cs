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
        private readonly IOptions<Configuration> _options;
        private readonly IOptions<CreateSearchOptions> _commandOptions;
        private readonly ILogger<CreateSearchCommand> _logger;

        public CreateSearchCommand(
            ISearchServiceClient searchClient,
            IOptions<Configuration> options,
            IOptions<CreateSearchOptions> commandOptions,
            ILogger<CreateSearchCommand> logger)
        {
            _searchClient = searchClient ?? throw new ArgumentNullException(nameof(searchClient));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _commandOptions = commandOptions ?? throw new ArgumentNullException(nameof(commandOptions));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (await _searchClient.Indexes.ExistsAsync(_options.Value.Search.IndexName))
            {
                if (!_commandOptions.Value.Replace)
                {
                    _logger.LogError(
                        "Azure Search index {IndexName} already exists",
                        _options.Value.Search.IndexName);
                    return;
                }

                _logger.LogInformation(
                    "Deleting Azure Search index {IndexName}...",
                    _options.Value.Search.IndexName);

                await _searchClient.Indexes.DeleteAsync(_options.Value.Search.IndexName);
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
