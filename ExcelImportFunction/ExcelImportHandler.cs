using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using ExcelLibrary.CompoundDocumentFormat;
using ExcelLibrary.SpreadSheet;
using DataAccessLayer;

namespace ExcelImportFunction
{
    public static class ExcelImportHandler
    {
        [FunctionName("ExcelImportHandler")]
        public static void Run([QueueTrigger("boost-gremlin-queue", Connection = "Endpoint=sb://jiw-boost-gremlin-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YqM46tLfZ0DUcvf5hfm53JkuHSW8FWeT9qX6e3iXXOs=")]string myQueueItem, ILogger log)
        {

            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");

            Dictionary<string, string> gremlinQueries = CreateGreminQueries(myQueueItem);

            GremlinDAL Dal = new GremlinDAL();
            Dal.PopulateGraph(gremlinQueries);
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
            for (int rowIndex = sheet.Cells.FirstRowIndex + 1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                gremlinQueries.Add("AddMLBProfile" + rowIndex, "g.addV('mlbprofile" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('email', '" + row.GetCell(2) + "').property('pk', 'pk')");
            }

            //FPSummary
            sheet = book.Worksheets[1];
            for (int rowIndex = sheet.Cells.FirstRowIndex + 1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                gremlinQueries.Add("AddFPSummary" + rowIndex, "g.addV('fpsummary" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('email', '" + row.GetCell(2) + "').property('FloatingPointTag', '" + row.GetCell(3) + "').property('pk', 'pk')");
            }

            //Product
            sheet = book.Worksheets[2];
            for (int rowIndex = sheet.Cells.FirstRowIndex + 1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                //string s = "g.addV('product" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('name', '" + row.GetCell(2) + "').property('price', '" + row.GetCell(3) + "').property('pk', 'pk')";
                gremlinQueries.Add("AddProduct" + rowIndex, "g.addV('product" + rowIndex + "').property('id', '" + row.GetCell(0) + "').property('name', '" + row.GetCell(1) + "').property('price', '" + row.GetCell(2) + "').property('pk', 'pk')");
            }

            int edgecount = 0;
            //mlbprofile to fp summary
            sheet = book.Worksheets[3];
            for (int rowIndex = sheet.Cells.FirstRowIndex + 1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                edgecount++;
                gremlinQueries.Add("AddEdge" + edgecount, "g.V('" + row.GetCell(0) + "').addE('hasfp').to(g.V('" + row.GetCell(1) + "'))");
            }

            //fpSummary to product
            sheet = book.Worksheets[4];
            for (int rowIndex = sheet.Cells.FirstRowIndex + 1; rowIndex <= sheet.Cells.LastRowIndex; rowIndex++)
            {
                Row row = sheet.Cells.GetRow(rowIndex);
                edgecount++;
                gremlinQueries.Add("AddEdge" + edgecount, "g.V('" + row.GetCell(0) + "').addE('fphasproduct').to(g.V('" + row.GetCell(1) + "'))");
            }

            return gremlinQueries;
        }
    }
}
