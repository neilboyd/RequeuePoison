using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

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
var storageAccount = CloudStorageAccount.Parse(connectionString);
var queueClient = storageAccount.CreateCloudQueueClient();
var queue = queueClient.GetQueueReference(queueName);
var poisonQueue = queueClient.GetQueueReference(poisonQueueName);

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
    var message = await poisonQueue.GetMessageAsync();
    if (message == null)
    {
        break;
    }
    var isOld = message.InsertionTime < ageLimit;
    if (isOld)
    {
        ++deleted;
    }
    else
    {
        await queue.AddMessageAsync(new CloudQueueMessage(message.AsString), null, delay, null, null);
        ++requeued;
        delay += interval;
    }
    await poisonQueue.DeleteMessageAsync(message);
    if (requeued == max)
    {
        break;
    }
}

Console.WriteLine($"Requeued {requeued} messages to {queueName}, deleted {deleted}");
if(delay < TimeSpan.FromMinutes(3))
{
    Console.WriteLine($"Total delay: {delay.TotalSeconds} seconds");
}
else if(delay < TimeSpan.FromHours(3))
{
    Console.WriteLine($"Total delay: {(delay.TotalSeconds / 60):F1} minutes");
}
else
{
    Console.WriteLine($"Total delay: {(delay.TotalSeconds / 3600):F1} hours");
}
Console.WriteLine("Enter ENTER to continue");
Console.ReadLine();
