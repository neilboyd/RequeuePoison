using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace RequeuePoison
{
    /// <summary>
    /// Requeue messages from poison queue back to original queue.
    /// Requeue messages from the last 24 hours, delete older messages.
    /// </summary>
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var queueName = args.Length > 0 ? args[0] : throw new ArgumentException("Specify queue name as parameter");
            var poisonQueueName = $"{queueName}-poison";

            // read the configuration
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.{environmentName}.json", true)
                .AddUserSecrets<Program>();
            var configuration = builder.Build();

            // get storage references
            var connectionString = configuration.GetConnectionString("AzureWebJobsStorage");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(queueName);
            var poisonQueue = queueClient.GetQueueReference(poisonQueueName);

            var requeued = 0;
            var deleted = 0;
            var lastDay = DateTime.UtcNow.AddDays(-100);
            while (true)
            {
                var message = await poisonQueue.GetMessageAsync();
                if (message == null)
                {
                    break;
                }
                var isLastDay = message.InsertionTime > lastDay;
                if (isLastDay)
                {
                    await queue.AddMessageAsync(new CloudQueueMessage(message.AsString));
                    ++requeued;
                }
                else
                {
                    ++deleted;
                }
                await poisonQueue.DeleteMessageAsync(message);
            }

            Console.WriteLine($"Requeued {requeued} messages to {queueName}, deleted {deleted}");
        }
    }
}
