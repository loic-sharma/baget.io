namespace BaGet
{
    public class Configuration
    {
        public BlobStorageConfiguration BlobStorage { get; set; }

        public ConnectionStringConfiguration ServiceBus { get; set; }
        public ConnectionStringConfiguration TableStorage { get; set; }

        public SearchConfiguration Search { get; set; }

        /// <summary>
        /// The API's public root URL.
        /// </summary>
        public string RootUrl { get; set; }
    }

    public class BlobStorageConfiguration
    {
        public string ConnectionString { get; set; }
        public string ContainerName { get; set; }
    }

    public class ConnectionStringConfiguration
    {
        public string ConnectionString { get; set; }
    }

    public class SearchConfiguration
    {
        public string ServiceName { get; set; }
        public string IndexName { get; set; }
        public string ApiKey { get; set; }
    }
}
