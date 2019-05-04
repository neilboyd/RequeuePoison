﻿using System;
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
            // read the configuration
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true)
                .AddJsonFile($"appsettings.{environmentName}.json", true)
                .AddCommandLine(args)
                .AddUserSecrets<Program>();
            var configuration = builder.Build();

            // get queue name
            var queueName = configuration["Queue"] ?? throw new ArgumentException("Specify queue name");
            var poisonQueueName = $"{queueName}-poison";

            // get storage references
            var connectionString = configuration["ConnectionString"] ?? throw new ArgumentException("Specify connection string");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(queueName);
            var poisonQueue = queueClient.GetQueueReference(poisonQueueName);

            // get max age property
            var maxAgeString = configuration["MaxAge"];
            if (!int.TryParse(maxAgeString, out var maxAge))
            {
                maxAge = 1;
            }

            // get interval property
            var intervalString = configuration["Interval"];
            if (!int.TryParse(intervalString, out var intervalInt))
            {
                intervalInt = 0;
            }
            var interval = TimeSpan.FromMilliseconds(intervalInt);

            var requeued = 0;
            var deleted = 0;
            var lastDay = DateTime.UtcNow.AddDays(-maxAge);
            var delay = TimeSpan.Zero;
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
                    await queue.AddMessageAsync(new CloudQueueMessage(message.AsString), null, delay, null, null);
                    ++requeued;
                    delay += interval;
                }
                else
                {
                    ++deleted;
                }
                await poisonQueue.DeleteMessageAsync(message);
            }

            Console.WriteLine($"Requeued {requeued} messages to {queueName}, deleted {deleted}");
            Console.WriteLine("Enter ENTER to continue");
            Console.ReadLine();
        }
    }
}
