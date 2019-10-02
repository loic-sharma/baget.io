using System;
using System.CommandLine;
using Microsoft.Extensions.Options;

namespace BaGet
{
    public class ConfigureCommandOptions<TOptions> : IConfigureOptions<TOptions>
        where TOptions : class
    {
        private readonly ParseResult _parseResult;
        private readonly Action<ParseResult, TOptions> _configureOptions;

        public ConfigureCommandOptions(ParseResult parseResult, Action<ParseResult, TOptions> configureOptions)
        {
            _parseResult = parseResult;
            _configureOptions = configureOptions;
        }

        public void Configure(TOptions options)
        {
            _configureOptions(_parseResult, options);
        }
    }
}
