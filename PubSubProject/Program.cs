using PubSubProject.Dto;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

namespace PubSubProject
{
    class Program
    {
        private const string RedisConnectionString = "localhost";
        private const int Port = 6379;

        private static  ConfigurationOptions options = new ConfigurationOptions
        {
            EndPoints = { { RedisConnectionString, Port } },
            DefaultDatabase = 6
        };

        private static ConnectionMultiplexer conn = ConnectionMultiplexer.Connect(options);
        private static IDatabase database = conn.GetDatabase();
        private static ISubscriber pubSub = conn.GetSubscriber();
        private const string channelName = "TokenChannel";
        private const string accessToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwNWFmYjdmNS1kN2IzLTQzMzMtOGM4NS1hYWUyMGZmOTI5NmUiLCJleHAiOjE1OTg0Mjc0NjQsImlzcyI6Imh0dHA6Ly9ncGF5LmNvbS50ciIsImF1ZCI6Imh0dHA6Ly9ncGF5LmNvbS50ciJ9.175vjZ0LY-0Vln4Vvo4WV_XJ8OfAirlb3sOfNh7pv48";

        static void Main(string[] args)
        {
            Console.WriteLine("-------------------");

            pubSub.Subscribe(channelName, (channel, message) =>
            {
                try
                {
                    WriteToRedis(message);
                }
                catch (Exception)
                {
                    Console.WriteLine("Error occured !");
                } 
            });

            RunAllAtTheSameTime().Wait();
            Console.WriteLine("-------------------");
            Console.ReadLine();
        }

        static void WriteToRedis(string message)
        {
            ModelDto model = JsonSerializer.Deserialize<ModelDto>(message);
            bool isExecuted = database.StringSet(model.Token, model.Message, TimeSpan.FromMilliseconds(1000), When.NotExists);
            if (!isExecuted)
            {
                throw new Exception("Token is in use already.");
            }

            Console.WriteLine($"Success. Message is {model.Message}");
        }

        private static async Task RunAllAtTheSameTime()
        {
            Task task1 = SendMessages("Message 1",1);
            Task task2 = SendMessages("Message 2",2);
            Task task3 = SendMessages("Message 3",3);
            Task task4 = SendMessages("Message 4",4);

            List<Task> taskList = new List<Task>() { task1, task2, task3, task4 };
            Task allTasks = Task.WhenAll(taskList);

            try
            {
                await allTasks;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.Message);
            }
        }

        private static async Task SendMessages(string message,int index)
        {

            ModelDto modelDto = new ModelDto
            {
                Token = accessToken,
                Message = message,
                Index = index
            };

            await pubSub.PublishAsync(channelName, JsonSerializer.Serialize(modelDto));
        }
    }
}
