namespace BaGet
{
    public class AppConfiguration
    {
        public string BlobStorageConnectionString { get; set; }
        public string BlobContainerName { get; set; }

        public string StorageQueueConnectionString { get; set; }
        public string StorageQueueName { get; set; }

        public string TableStorageConnectionString { get; set; }
    }
}
