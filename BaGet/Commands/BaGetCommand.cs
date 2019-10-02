using System.CommandLine;
using System.CommandLine.Builder;
using System.Linq;
using NuGet.Versioning;

namespace BaGet
{
    public static class BaGetCommand
    {
        public static HostedCommandLineBuilder Create()
        {
            var builder = new HostedCommandLineBuilder();

            builder.Command.Handler = builder.BuildHelpHandler(builder.Command);
            builder.Command.Add(PackagesCommand());
            builder.Command.Add(V3Command());
            builder.Command.Add(AzureSearchCommand());

            return builder;

            Command PackagesCommand()
            {
                var command = new Command("packages", "Manage packages.");

                command.Handler = builder.BuildHelpHandler(command);
                command.Add(AddPackagesCommand());
                command.Add(UpdatePackagesCommand());

                return command;

                Command AddPackagesCommand()
                {
                    var addCommand = new Command("add", "Add packages.");

                    addCommand
                        .AddOptionalArgument<string>("packageId", argument =>
                        {
                            argument.Description = "The package's ID. If not specified, adds all packages "
                                + "from the NuGet package source.";
                        })
                        .AddOptionalArgument<NuGetVersion>("packageVersion", argument =>
                        {
                            argument.Description = "The package's version. If not specified, adds all versions "
                                + "of the package ID.";
                        })
                        //.AddOption("--source", option =>
                        //{
                        //    option.AddAlias("-s");
                        //    option.Description = "The NuGet package source.";
                        //})
                        .AddOption("--enqueue", option =>
                        {
                            option.AddAlias("-q");
                            option.Description = "Add work to queue instead of processing directly.";
                        });

                    addCommand.Handler = builder
                        .Configure<AddPackagesOptions>(ConfigureAddPackages)
                        .BuildHandler<AddPackagesCommand>();

                    return addCommand;

                    void ConfigureAddPackages(ParseResult parseResult, AddPackagesOptions options)
                    {
                        var packageIdArg = addCommand.Arguments.First(a => a.Name == "packageId");
                        var packageVersionArg = addCommand.Arguments.First(a => a.Name == "packageVersion");

                        options.PackageId = parseResult.FindResultFor(packageIdArg).GetValueOrDefault<string>();
                        options.PackageVersion = parseResult.FindResultFor(packageVersionArg).GetValueOrDefault<NuGetVersion>();
                        options.Enqueue = parseResult.HasOption("enqueue");
                        //options.Source = parseResult.HasOption("source")
                        //    ? parseResult.ValueForOption<string>("source")
                        //    : "https://api.nuget.org/v3/index.json";
                    }
                }

                Command UpdatePackagesCommand()
                {
                    var updateCommand = new Command("update", "Update packages using metadata from nuget.org.");

                    updateCommand
                        .AddOptionalArgument<string>("packageId", argument =>
                        {
                            argument.Description = "The package's ID. If not specified, updates all packages "
                                + "from the NuGet package source.";
                        })
                        .AddOptionalArgument<NuGetVersion>("packageVersion", argument =>
                        {
                            argument.Description = "The package's version. If not specified, updates all versions "
                                + "of the package ID.";
                        })
                        .AddOption("--downloads", option =>
                        {
                            option.AddAlias("-d");
                            option.Description = "Update package downloads.";
                        })
                        .AddOption("--owners", option =>
                        {
                            option.AddAlias("-o");
                            option.Description = "Update package owners.";
                        });

                    updateCommand.Handler = builder
                        .Configure<UpdatePackagesOptions>(ConfigureUpdatePackages)
                        .BuildHandler<UpdatePackagesCommand>();

                    return updateCommand;

                    void ConfigureUpdatePackages(ParseResult parseResult, UpdatePackagesOptions options)
                    {
                        var packageIdArg = updateCommand.Arguments.First(a => a.Name == "packageId");
                        var packageVersionArg = updateCommand.Arguments.First(a => a.Name == "packageVersion");

                        options.PackageId = parseResult.FindResultFor(packageIdArg).GetValueOrDefault<string>();
                        options.PackageVersion = parseResult.FindResultFor(packageVersionArg).GetValueOrDefault<NuGetVersion>();
                        options.UpdateDownloads = parseResult.HasOption("downloads");
                        options.UpdateOwners = parseResult.HasOption("owners");
                    }
                }
            }

            Command V3Command()
            {
                var command = new Command("v3", "Manage the static V3 resources.");

                command.Handler = builder.BuildHelpHandler(command);
                command.Add(RebuildV3Command());

                return command;

                Command RebuildV3Command()
                {
                    var rebuildCommand = new Command("rebuild", "Rebuild the static V3 resources.");

                    rebuildCommand.AddOption("--enqueue", option =>
                    {
                        option.AddAlias("-q");
                        option.Description = "Add work to queue instead of processing directly.";
                    });

                    rebuildCommand.Handler = builder
                        .Configure<RebuildV3Options>(ConfigureRebuildV3)
                        .BuildHandler<RebuildV3Command>();

                    return rebuildCommand;

                    void ConfigureRebuildV3(ParseResult parseResult, RebuildV3Options options)
                    {
                        options.Enqueue = parseResult.HasOption("enqueue");
                    }
                }
            }

            Command AzureSearchCommand()
            {
                var command = new Command("azure-search", "Manage the Azure Search resource.");

                command.Handler = builder.BuildHelpHandler(command);
                command.AddAlias("azs");
                command.Add(CreateSearchCommand());
                command.Add(RebuildSearchCommand());

                return command;

                Command CreateSearchCommand()
                {
                    var createCommand = new Command("create", "Create the Azure Search index.");

                    createCommand.AddOption(new Option("--replace", "Delete an Azure Search index if it exists already."));
                    createCommand.Handler = builder
                        .Configure<CreateSearchOptions>(ConfigureCreateSearch)
                        .BuildHandler<CreateSearchCommand>();

                    return createCommand;

                    void ConfigureCreateSearch(ParseResult parseResult, CreateSearchOptions options)
                    {
                        options.Replace = parseResult.HasOption("replace");
                    }
                }

                Command RebuildSearchCommand()
                {
                    return new Command("rebuild", "Add the latest data to an empty Azure Search index.")
                    {
                        Handler = builder.BuildHandler<RebuildSearchCommand>()
                    };
                }
            }
        }
    }
}
