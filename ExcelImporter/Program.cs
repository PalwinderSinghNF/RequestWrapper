using System;
using System.IO;
using System.Linq;
using System.Text;
using Azure.Messaging.ServiceBus;
using GremlinServiceBusWrapper;

namespace ExcelImporter
{
    class Program
    {

        static string connectionString = "Endpoint=sb://jiw-boost-gremlin-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YqM46tLfZ0DUcvf5hfm53JkuHSW8FWeT9qX6e3iXXOs=";
        static string queueName = "boost-gremlin-queue";
        static ServiceBusClient client;
        static ServiceBusSender sender;
        static ServiceBusProcessor processor;

        //private void InitializeQueue()
        //{
        //    client = new ServiceBusClient(connectionString);
        //    sender = client.CreateSender(queueName);
        //    processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());            
        //}

        static void Main(string[] args)
        {
            Console.WriteLine(" Please enter an excel path with name: ");
           
            var path = Console.ReadLine(); 
            if (File.Exists(path))
            {
                byte[] byteArray = Encoding.UTF8.GetBytes(path);
                MemoryStream stream = new MemoryStream(byteArray);

                StreamReader sr = new StreamReader(stream);

                String filecontent = sr.ReadToEnd();

                ServiceBusWrapper ServiceBusWrapper = new ServiceBusWrapper();
                ServiceBusWrapper.AddFileToQueue(filecontent);

            }

        }
    }
}
