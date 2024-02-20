using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Text;
using System.ComponentModel;
using System.Collections.Concurrent;
using System.Diagnostics;
using StackExchange.Redis;

namespace TestSampleAPIV2
{
    internal class Program
    {
        public enum DbType
        {
            PgSql,
            MySql,
            InMemory,
            Redis
        }

        private class Person
        {
            public string? FirstName { get; set; }
            public string? LastName { get; set; }
            public string? Email { get; set; }
        }

        public class Request
        {
            public required string TestName { get; init; }
            public required string Url { get; init; }

            public DateTime? StartTime { get; set; }
            public DateTime? EndTime { get; set; }
        }

        private const string UrlInMemory = "http://localhost:5041/api/v2/Person/in-memory/add-person";
        private const string UrlPgSql = "http://localhost:5041/api/v2/Person/pg-sql/add-person";
        private const string UrlMySql = "http://localhost:5041/api/v2/Person/my-sql/add-person";
        private const string UrlRedis = "http://localhost:5041/api/v2/Person/redis/add-person";

        private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        public static readonly RandomNumberGenerator Rng = RandomNumberGenerator.Create();
        private static readonly Random Rnd = new();

        private const int MaxDataSet = 5000;
        private const int MaxNumberOfRequest = 10000;

        private static Person[]? _persons;

        private static volatile int _totalFailed = 0;
        private static volatile int _totalResponse = 0;

        private static readonly ConcurrentDictionary<string, Person> RequestList = new ();

        private const string RedisConnectionString = "127.0.0.1:6379";

        private const string AddPersonChannelSuccess = "add_person_success";

        private static readonly ConnectionMultiplexer Redis = ConnectionMultiplexer.Connect(RedisConnectionString);

        // Create HttpClient instance
        static readonly HttpClient Client = new();

        static readonly SemaphoreSlim Semaphore = new SemaphoreSlim(0, 1);

        public static string ShuffleString(string stringToShuffle)
        {
            string shuffled;

            do
            {
                shuffled = new string(
                    stringToShuffle
                        .OrderBy(character => Guid.NewGuid())
                        .ToArray()
                );
            } while (shuffled == stringToShuffle);

            return shuffled;
        }

        private static void GenerateData()
        {
            _persons = new Person[MaxDataSet];
            for (var i = 0; i < _persons.Length; i++)
            {
                _persons[i] = new Person
                {
                    FirstName = GetNextRandomString(35),
                    LastName = GetNextRandomString(35),
                    Email = Guid.NewGuid().ToString() //GetNextRandomString(70)
                };
            }
        }

        private static void Print(Request request)
        {
            Console.WriteLine("*************************************************");
            Console.WriteLine("");

            Console.WriteLine($"Test Name: {request.TestName}");
            Console.WriteLine($"URL: {request.Url}");
            Console.WriteLine($"Total Response Received: {_totalResponse}");
            Console.WriteLine($"Total Failed: {_totalFailed}");

            // Calculate the time difference
            if (request is { EndTime: not null, StartTime: not null })
            {
                //var timeDifference = (TimeSpan)(request.EndTime - request.StartTime)!;
                var timeDifference = request.EndTime.Value.Subtract(request.StartTime.Value);
                var differenceInSeconds = (int)timeDifference.TotalSeconds;
                Console.WriteLine($"Time taken in seconds: {differenceInSeconds}");
            }

            Console.WriteLine("");
            Console.WriteLine("*************************************************");
        }

        private static int GetNextRandomInt(int upperLimit)
        {
            //var data = new byte[1];
            //Rng.GetBytes(data);
            //return data[0] % upperLimit;
            return Rnd.Next(upperLimit);
        }

        private static string GetNextRandomString(int length)
        {
            StringBuilder result = new();

            for (var i = 0; i < length; i++)
            {
                result.Append(Chars[GetNextRandomInt(Chars.Length)]);
            }
            return result.ToString();
        }

        private static async Task SendRequest(Request request)
        {
            // Prepare the request body
            if (_persons != null)
            {
                // pick a random person
                var person = _persons[GetNextRandomInt(_persons.Length)];
                var requestBody = JsonSerializer.Serialize(person);

                // Create the request content with JSON data
                var content = new StringContent(requestBody, Encoding.UTF8, "application/json");

                try
                {
                    // Send the POST request
                    var response = await Client.PostAsync(request.Url, content);

                    // Check if the request was successful
                    if (response.IsSuccessStatusCode)
                    {
                        // Read and display the response body
                        try
                        {
                            //RequestList.TryAdd(person.Email!, person);
                            var responseContent = await response.Content.ReadAsStringAsync();
                            var jsonResponse = JsonNode.Parse(responseContent)!;
                            var resTrackingId = jsonResponse["trackingId"]!;
                            var resEmail = jsonResponse["email"]!;
                            
                            var trackingId = resTrackingId.GetValue<string>();
                            var email = resEmail.GetValue<string>();

                            if (!RequestList.TryAdd(trackingId, person))
                            {
                                Console.WriteLine("Request already exists");
                            }

                            if (email != person.Email)
                            {
                                Console.WriteLine("Response did not match with input");
                                Interlocked.Increment(ref _totalFailed);
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Failed to parse response. Error: " + ex.Message);
                            Interlocked.Increment(ref _totalFailed);
                        }
                    }
                    else
                    {
                        Console.WriteLine("Error: " + response);
                        Interlocked.Increment(ref _totalFailed);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e.Message);
                    Interlocked.Increment(ref _totalFailed);
                }
            }
        }


        private static async Task ExecuteTestAsync(Request request, int num)
        {
            var tasks = new Task[num];

            for (var i = 0; i < num; i++)
            {
                tasks[i] = Task.Run(() => SendRequest(request));
            }
            Console.WriteLine("All requests have been sent successfully.");
            Console.WriteLine("Waiting to get all responses.");
            await Task.WhenAll(tasks);
        }

        private static async Task Execute(Request request)
        {
            _totalFailed = 0;
            _totalResponse = 0;

            await ExecuteTestAsync(request, MaxNumberOfRequest);
        }


        public static Request GetRequest(DbType dbType)
        {
            var request = dbType switch
            {
                DbType.PgSql => new Request { TestName = "PgSql Test:", Url = UrlPgSql, },
                DbType.MySql => new Request { TestName = "MySql Test:", Url = UrlMySql, },
                DbType.InMemory => new Request { TestName = "In Memory Test:", Url = UrlInMemory },
                DbType.Redis => new Request { TestName = "Redis Test:", Url = UrlRedis },
                _ => throw new InvalidEnumArgumentException("Invalid data type")
            };
            return request;
        }

        private static void HandleNewCommentNotification(string message)
        {
            if (!string.IsNullOrEmpty(message))
            {
                Interlocked.Increment(ref _totalResponse);
                
                var jsonResponse = JsonNode.Parse(message)!;
                var resTrackingId = jsonResponse["TrackingId"]!;
                var resEmail = jsonResponse["Person"]!["Email"]!;

                var trackingId = resTrackingId.GetValue<string>();
                var email = resEmail.GetValue<string>();

                if (!RequestList.TryGetValue(trackingId, out Person? person) || person.Email != email)
                {
                    Console.WriteLine("Output did not match with input");
                    Interlocked.Increment(ref _totalFailed);
                }
            }

            if (_totalResponse >= MaxNumberOfRequest)
            {
                Semaphore.Release();
            }
        }

        static async Task Main(string[] args)
        {
            //_redis = ConnectionMultiplexer.Connect(RedisConnectionString);
            var sub = Redis.GetSubscriber();
            await sub.SubscribeAsync(AddPersonChannelSuccess, (channel, message) =>
            {
                Debug.Assert(message != RedisValue.Null, nameof(message) + " != null");
                HandleNewCommentNotification(message!);
            });
            
            Client.Timeout = TimeSpan.FromSeconds(30000); // 30 seconds
            Console.WriteLine("*************************************************");
            Console.WriteLine("");
            Console.WriteLine("Generating test data set of size: " + MaxDataSet);
            Console.WriteLine("");

            GenerateData();

            const DbType dbType = DbType.InMemory;

            Console.WriteLine("*************************************************");
            Console.WriteLine("");
            Console.WriteLine("Data Generated");
            Console.WriteLine("");
            Console.WriteLine("*************************************************");
            Console.WriteLine("");
            Console.WriteLine("Starting execution of " + MaxNumberOfRequest + " requests: " + dbType);
            Console.WriteLine("");

            Request request = GetRequest(dbType);
            request.StartTime = DateTime.Now;
            await Execute(request);

            Console.WriteLine("Waiting to receive all notifications.");
            
            await Semaphore.WaitAsync(TimeSpan.FromMinutes(2)); // wait maximum 2 minutes to get all responses
            request.EndTime = DateTime.Now;
            
            Print(request);
        }
    }
}
