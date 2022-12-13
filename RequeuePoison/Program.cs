using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

// read the configuration
var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
var builder = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", true)
    .AddJsonFile($"appsettings.{environmentName}.json", true)
    .AddUserSecrets<Program>()
    .AddCommandLine(args);
var configuration = builder.Build();

// get queue name
var queueName = configuration["Queue"] ?? throw new ArgumentException("Specify queue name");
var poisonQueueName = $"{queueName}-poison";

// get storage references
var connectionString = configuration["ConnectionString"] ?? throw new ArgumentException("Specify connection string");
var queue = new QueueClient(connectionString, queueName);
var poisonQueue = new QueueClient(connectionString, poisonQueueName);

// get max property
var maxString = configuration["Max"];
if (!int.TryParse(maxString, out var max))
{
    max = int.MaxValue;
}

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
var ageLimit = DateTime.UtcNow.AddDays(-maxAge);
var delay = TimeSpan.Zero;
while (true)
{
    QueueMessage message = await poisonQueue.ReceiveMessageAsync();
    if (message == null)
    {
        break;
    }
    var isOld = message.InsertedOn < ageLimit;
    if (isOld)
    {
        ++deleted;
    }
    else
    {
        var body = message.Body.ToString();
        await queue.SendMessageAsync(body, visibilityTimeout: delay);
        ++requeued;
        delay += interval;
    }
    await poisonQueue.DeleteMessageAsync(message.MessageId, message.PopReceipt);
    if (requeued == max)
    {
        break;
    }
}

Console.WriteLine($"Requeued {requeued} messages to {queueName}, deleted {deleted}");
if (delay < TimeSpan.FromMinutes(3))
{
    Console.WriteLine($"Total delay: {delay.TotalSeconds} seconds");
}
else if (delay < TimeSpan.FromHours(3))
{
    Console.WriteLine($"Total delay: {delay.TotalSeconds / 60:F1} minutes");
}
else
{
    Console.WriteLine($"Total delay: {delay.TotalSeconds / 3600:F1} hours");
}
if (!Debugger.IsAttached)
{
    Console.WriteLine("Enter ENTER to continue");
    Console.ReadLine();
}
