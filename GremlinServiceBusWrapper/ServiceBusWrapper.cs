using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using DataAccessLayer;
using ExcelLibrary.CompoundDocumentFormat;
using ExcelLibrary.SpreadSheet;

namespace GremlinServiceBusWrapper
{
    public class ServiceBusWrapper
    {
        static string connectionString = "Endpoint=sb://jiw-boost-gremlin-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YqM46tLfZ0DUcvf5hfm53JkuHSW8FWeT9qX6e3iXXOs=";
        static string queueName = "boost-gremlin-queue";
        static ServiceBusClient client;
        static ServiceBusSender sender;
        static ServiceBusProcessor processor;

        public ServiceBusWrapper()
        {
            InitializeQueue();
        }

        private async Task InitializeQueue()
        {
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(queueName);
            processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());

            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;
            await processor.StartProcessingAsync();

            //ClearQueue();
        }

        /*public async Task AddToQueue(string filepath)
        {
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            int i =messageBatch.Count;

            messageBatch.TryAddMessage(new ServiceBusMessage(filepath));
            await sender.SendMessagesAsync(messageBatch);
        }*/

        public async Task AddFileToQueue(string filecontent)
        {
            try
            {
                using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                int i = messageBatch.Count;

                messageBatch.TryAddMessage(new ServiceBusMessage(filecontent));
                await sender.SendMessagesAsync(messageBatch);
            }
            catch(Exception ex)
            { }
        }

        private async Task ClearQueue()
        {
            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            while ((await receiver.PeekMessageAsync()) != null)
            {
                // receive in batches of 100 messages.
                await receiver.ReceiveMessagesAsync(100);
            }
        }


        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            Dictionary<string, string> gremlinQueries = CreateGreminQueries(body);

            GremlinDAL Dal = new GremlinDAL();
            Dal.PopulateGraph(gremlinQueries);

            // complete the message. message is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        private static Dictionary<string, string> CreateGreminQueries(string filecontent)
        {
            Dictionary<string, string> gremlinQueries = new Dictionary<string, string>();

            Stream stream = new MemoryStream();            
            byte[] byteArray = Encoding.UTF8.GetBytes(filecontent);
            stream.Write(byteArray, 0, byteArray.Length);
            stream.Position = 0;

            Workbook book = Workbook.Load(stream);

            gremlinQueries.Add("Cleanup", "g.V().drop()");

            //MLBProfile
            Worksheet sheet = book.Worksheets[0];
            for (int rowIndex = sheet.Cells.FirstRowIndex+1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                gremlinQueries.Add("AddMLBProfile" + rowIndex, "g.addV('mlbprofile" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('email', '" + row.GetCell(2) + "').property('pk', 'pk')");
            }

            //FPSummary
            sheet = book.Worksheets[1];
            for (int rowIndex = sheet.Cells.FirstRowIndex+1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                gremlinQueries.Add("AddFPSummary" + rowIndex, "g.addV('fpsummary" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('email', '" + row.GetCell(2) + "').property('FloatingPointTag', '" + row.GetCell(3) + "').property('pk', 'pk')");
            }

            //Product
            sheet = book.Worksheets[2];
            for (int rowIndex = sheet.Cells.FirstRowIndex+1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                //string s = "g.addV('product" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('name', '" + row.GetCell(2) + "').property('price', '" + row.GetCell(3) + "').property('pk', 'pk')";
                gremlinQueries.Add("AddProduct" + rowIndex, "g.addV('product" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('price', '" + row.GetCell(2) + "').property('pk', 'pk')");
            }

            int edgecount = 0;
            //mlbprofile to fp summary
            sheet = book.Worksheets[3];
            for (int rowIndex = sheet.Cells.FirstRowIndex+1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                edgecount++;
                gremlinQueries.Add("AddEdge" + edgecount, "g.V('" + row.GetCell(0) + "').addE('hasfp').to(g.V('" + row.GetCell(1) + "'))");
            }

            //fpSummary to product
            sheet = book.Worksheets[4];
            for (int rowIndex = sheet.Cells.FirstRowIndex+1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                edgecount++;
                gremlinQueries.Add("AddEdge" + edgecount, "g.V('" + row.GetCell(0) + "').addE('fphasproduct').to(g.V('" + row.GetCell(1) + "'))");
            }

            return gremlinQueries;
        }

        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        private async Task StopProcessingQueue()
        {
            try
            {
                await processor.StopProcessingAsync();
            }
            finally
            {
                await processor.DisposeAsync();
                await client.DisposeAsync();
            }
        }
    }
}
