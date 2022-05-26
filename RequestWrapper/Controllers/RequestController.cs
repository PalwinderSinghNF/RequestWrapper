using DataAccessLayer;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GremlinServiceBusWrapper;

namespace RequestWrapper.Controllers
{
    [Route("MLB")]
    public class RequestController : Controller
    {
        private GremlinDAL Dal = new GremlinDAL();

        [HttpGet("v1/PopulateGraph")]
        public void PopulateGraph()
        {
            Dal.PopulateGraph();
        }

        [HttpGet("v1/GetTotalProductCount")]
        public int GetTotalProductCount()
        {
            return Dal.GetTotalProductCount();
        }

        [HttpGet("v1/GetProductCountByFPId/{id}")]
        public int GetProductCountByFPId(string id)
        {
            return Dal.GetProductCountByFPId(id);
        }

        [HttpGet("v1/GetTackingIDForTotalProductCount")]
        public string GetTackingIDForTotalProductCount()
        {
            return Dal.GetTackingIDForTotalProductCount();
        }

        [HttpGet("v1/GetTackingIDForProductCountByFPId/{id}")]
        public string GetTackingIDForProductCountByFPId(string id)
        {
            return Dal.GetTackingIDForProductCountByFPId(id);
        }

        [HttpGet("v1/GetDataByTrackingId/{id}")]
        public object GetDataByTrackingId(string id)
        {
            return Dal.GetDataByTrackingId(id);
        }

        [HttpGet("v1/PopulateGraphByExcelImport")]
        public void PopulateGraphByExcelImport()
        {
            string excelpath = @"C:\GremlinExcel\DemoData.xls";

            //string excelpath = @"C:\Users\Palwinder Singh\Desktop\New folder\DemoData - Copy.xls"

            ServiceBusWrapper ServiceBusWrapper = new ServiceBusWrapper();
            ServiceBusWrapper.AddToQueue(excelpath);
        }
    }
}
