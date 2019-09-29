namespace BaGet
{
    public class Configuration
    {
        public string BlobStorageConnectionString { get; set; }
        public string BlobContainerName { get; set; }

        public string ServiceBusConnectionString { get; set; }

        public string TableStorageConnectionString { get; set; }

        /// <summary>
        /// The API's public root URL.
        /// </summary>
        public string RootUrl { get; set; }
    }
}
