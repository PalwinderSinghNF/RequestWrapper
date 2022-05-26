using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Threading.Tasks;

namespace DataAccessLayer
{
    public class GremlinDAL
    {
        private static Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
        {
            { "Cleanup",        "g.V().drop()" },
            { "AddMLBProfile 1",    "g.addV('mlbprofile').property('id', '105').property('name', 'vivaan').property('email', 'support@mhpolymers.com').property('pk', 'pk')" },
            { "AddMLBProfile 2",    "g.addV('mlbprofile').property('id', '211').property('name', 'weddingdestination').property('email', 'akshayabcvijayawada@gmail.com').property('pk', 'pk')" },
            { "AddMLBProfile 3",    "g.addV('mlbprofile').property('id', '101').property('name', 'indusindbank').property('email', 'vishal.shah.8878@gmail.com').property('pk', 'pk')" },
           
            { "AddFPSummary 1",    "g.addV('fpsummary').property('id', '56fbd9de9ec66b0f2cdc8a3a').property('name', 'The Vivaan Hotel &  Resorts').property('email', 'info@thevivaan.com').property('FloatingPointTag', 'VIVAANHOTELDELHI').property('pk', 'pk')" },
            { "AddFPSummary 2",    "g.addV('fpsummary').property('id', '56fbd9789ec66a10dcb60f90').property('name', 'The Vivaan Hotel & Restaurants').property('email', 'info@thevivaan.com').property('FloatingPointTag', 'VIVAANHOTELCHANDIGARH').property('pk', 'pk')" },
            { "AddFPSummary 3",    "g.addV('fpsummary').property('id', '56fbd83e9ec66a10dcb60f44').property('name', 'The Vivaan Hotel &  Business Rooms').property('email', 'info@thevivaan.com').property('FloatingPointTag', 'VIVAANHOTELLUDHIANA').property('pk', 'pk')" },
            
            { "AddFPSummary 4",    "g.addV('fpsummary').property('id', '57c6aa819ec6690ab0eb50e4').property('name', 'The Wedding Destination').property('email', 'Sales@narayaniheights.com').property('FloatingPointTag', 'WEDDINGDESTINATION').property('pk', 'pk')" },
            { "AddFPSummary 5",    "g.addV('fpsummary').property('id', '5810353f9ec66b06ec8ec4d0').property('name', 'The Wedding Destination').property('email', 'Sales@narayaniheights.com').property('FloatingPointTag', 'WEDDINGDESTINATIONUSA').property('pk', 'pk')" },
            { "AddFPSummary 6",    "g.addV('fpsummary').property('id', '581035969ec66b06ec8ec4e5').property('name', 'The Wedding Destination').property('email', 'Sales@narayaniheights.com').property('FloatingPointTag', 'WEDDINGDESTINATIONUK').property('pk', 'pk')" },
            
            { "AddFPSummary 7",    "g.addV('fpsummary').property('id', '56e7ca469ec6680090cd4d6f').property('name', 'IndusInd Bank -TT Nagar,Bhopal').property('email', 'reachus@indusind.com').property('FloatingPointTag', 'INDUSINDBANKTTNAGAR').property('pk', 'pk')" },
            { "AddFPSummary 8",    "g.addV('fpsummary').property('id', '56e7ca419ec668081864f925').property('name', 'IndusInd Bank -9th Ln Arundelpet,Guntur').property('email', 'reachus@indusind.com').property('FloatingPointTag', 'INDUSINDBANKARUNDELPET').property('pk', 'pk')" },
            { "AddFPSummary 9",    "g.addV('fpsummary').property('id', '56e7ca409ec6680090cd4d6b').property('name', 'IndusInd Bank -Station Rd,Umargam').property('email', 'reachus@indusind.com').property('FloatingPointTag', 'INDUSINDBANKSTATIONRD').property('pk', 'pk')" },


            { "AddProduct 1",    "g.addV('product1').property('id', '5b6af68b702ce1054a2523c6').property('name', 'Product A').property('price', 'INR 10').property('pk', 'pk')" },
            { "AddProduct 2",    "g.addV('product2').property('id', '5b6af68b702ce1054a2523c7').property('name', 'Product B').property('price', 'INR 102').property('pk', 'pk')" },
            { "AddProduct 3",    "g.addV('product3').property('id', '5b6af68b702ce164a52523c6').property('name', 'Product C').property('price', 'INR 103').property('pk', 'pk')" },
            { "AddProduct 4",    "g.addV('product4').property('id', '5b6af68b702ce10534a2523c').property('name', 'Product D').property('price', 'INR 104').property('pk', 'pk')" },
            { "AddProduct 5",    "g.addV('product5').property('id', '5b6af68b702ce1054a2523c8').property('name', 'Product E').property('price', 'INR 105').property('pk', 'pk')" },
            { "AddProduct 6",    "g.addV('product6').property('id', '5b6af68b702ce1054a2523c9').property('name', 'Product F').property('price', 'INR 106').property('pk', 'pk')" },
            { "AddProduct 7",    "g.addV('product7').property('id', '6b6af68b702ce1054a2523c6').property('name', 'Product G').property('price', 'INR 107').property('pk', 'pk')" },
            { "AddProduct 8",    "g.addV('product8').property('id', '7b6af68b702ce1054a2523c6').property('name', 'Product H').property('price', 'INR 108').property('pk', 'pk')" },
            { "AddProduct 9",    "g.addV('product9').property('id', '8b6af68b702ce1054a2523c6').property('name', 'Product I').property('price', 'INR 109').property('pk', 'pk')" },
            { "AddProduct 10",    "g.addV('product10').property('id', '9b6af68b702ce1054a2523c6').property('name', 'Product J').property('price', 'INR 110').property('pk', 'pk')" },
            { "AddProduct 11",    "g.addV('product11').property('id', '0b6af68b702ce1054a2523c5').property('name', 'Product K').property('price', 'INR 1110').property('pk', 'pk')" },
            { "AddProduct 12",    "g.addV('product12').property('id', '6b6af68b702ce1054a2523c7').property('name', 'Product L').property('price', 'INR 1120').property('pk', 'pk')" },

            //mlbprofile to fp summary
            { "AddEdge 1",      "g.V('105').addE('hasfp').to(g.V('56fbd9de9ec66b0f2cdc8a3a'))" },
            { "AddEdge 2",      "g.V('105').addE('hasfp').to(g.V('56fbd9789ec66a10dcb60f90'))" },
            { "AddEdge 3",      "g.V('105').addE('hasfp').to(g.V('56fbd83e9ec66a10dcb60f44'))" },
            { "AddEdge 4",      "g.V('211').addE('hasfp').to(g.V('57c6aa819ec6690ab0eb50e4'))" },
            { "AddEdge 5",      "g.V('211').addE('hasfp').to(g.V('5810353f9ec66b06ec8ec4d0'))" },
            { "AddEdge 6",      "g.V('211').addE('hasfp').to(g.V('581035969ec66b06ec8ec4e5'))" },
            { "AddEdge 7",      "g.V('101').addE('hasfp').to(g.V('56e7ca469ec6680090cd4d6f'))" },
            { "AddEdge 8",      "g.V('101').addE('hasfp').to(g.V('56e7ca419ec668081864f925'))" },
            { "AddEdge 9",      "g.V('101').addE('hasfp').to(g.V('56e7ca409ec6680090cd4d6b'))" },

            //fpSummary to product
            { "AddEdge 10",      "g.V('56fbd9de9ec66b0f2cdc8a3a').addE('fphasproduct').to(g.V('5b6af68b702ce1054a2523c6'))" },
            { "AddEdge 11",      "g.V('56fbd9789ec66a10dcb60f90').addE('fphasproduct').to(g.V('5b6af68b702ce1054a2523c7'))" },
            { "AddEdge 12",      "g.V('56fbd83e9ec66a10dcb60f44').addE('fphasproduct').to(g.V('5b6af68b702ce164a52523c6'))" },
            { "AddEdge 13",      "g.V('57c6aa819ec6690ab0eb50e4').addE('fphasproduct').to(g.V('5b6af68b702ce10534a2523c'))" },
            { "AddEdge 14",      "g.V('5810353f9ec66b06ec8ec4d0').addE('fphasproduct').to(g.V('5b6af68b702ce1054a2523c8'))" },
            { "AddEdge 15",      "g.V('581035969ec66b06ec8ec4e5').addE('fphasproduct').to(g.V('5b6af68b702ce1054a2523c9'))" },
            { "AddEdge 16",      "g.V('56e7ca469ec6680090cd4d6f').addE('fphasproduct').to(g.V('6b6af68b702ce1054a2523c6'))" },
            { "AddEdge 17",      "g.V('56e7ca419ec668081864f925').addE('fphasproduct').to(g.V('7b6af68b702ce1054a2523c6'))" },
            { "AddEdge 18",      "g.V('56e7ca409ec6680090cd4d6b').addE('fphasproduct').to(g.V('9b6af68b702ce1054a2523c6'))" },
            { "AddEdge 19",      "g.V('57c6aa819ec6690ab0eb50e4').addE('fphasproduct').to(g.V('7b6af68b702ce1054a2523c6'))" },
            { "AddEdge 20",      "g.V('5810353f9ec66b06ec8ec4d0').addE('fphasproduct').to(g.V('5b6af68b702ce1054a2523c7'))" },
            { "AddEdge 21",      "g.V('56fbd9789ec66a10dcb60f90').addE('fphasproduct').to(g.V('5b6af68b702ce10534a2523c'))" },  
            
        };       

        private static string Host = "jiwboostgremlinaccount.gremlin.cosmos.azure.com";
        private static string PrimaryKey = "6vjVt02uzEqkPu17nySf7SdcByDP2nOPpDbYtEalcr6OoqSrAoiUW2iKORxsFcJlxq7bWQrDRSguMkRePnSJ7Q==";
        private static string Database = "Boost360MLB";
        private static string Container = "MLBGraph";
        private static bool EnableSSL = true;
        private static int Port = 443;

        private GremlinServer gremlinServer;
        private ConnectionPoolSettings connectionPoolSettings;
        private Action<ClientWebSocketOptions> webSocketConfiguration;

        //Tracking ID, Object, Timestamp
        private Dictionary<string, object> cache = new Dictionary<string, object>();
        //Tracking ID, Timestamp
        private Dictionary<string, DateTime> cacheTime = new Dictionary<string, DateTime>();

        private static DALCache<Task<ResultSet<dynamic>>> gremlinCache = new DALCache<Task<ResultSet<dynamic>>>();

        private static DALCache<Task<ResultSet<dynamic>>> gremlinTrackingCache = new DALCache<Task<ResultSet<dynamic>>>();

        private string GenerateTrackingId()
        {
            return Guid.NewGuid().ToString().Replace("-", "");
        }

        private void InitilizeConnection()
        {
            string containerLink = "/dbs/" + Database + "/colls/" + Container;
            Console.WriteLine($"Connecting to: host: {Host}, port: {Port}, container: {containerLink}, ssl: {EnableSSL}");
            gremlinServer = new GremlinServer(Host, Port, enableSsl: EnableSSL,
                                                    username: containerLink,
                                                    password: PrimaryKey);

            connectionPoolSettings = new ConnectionPoolSettings()
            {
                MaxInProcessPerConnection = 10,
                PoolSize = 30//,
                //ReconnectionAttempts = 3,
                //ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
            };

            webSocketConfiguration =
                new Action<ClientWebSocketOptions>(options =>
                {
                    options.KeepAliveInterval = TimeSpan.FromSeconds(10);
                });            
        }

        private static Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, KeyValuePair<string, string> query)
        {
            try
            {
                var resultSet = gremlinCache.GetOrCreate(query, () => gremlinClient.SubmitAsync<dynamic>(query.Value));
                return resultSet;
                //return gremlinClient.SubmitAsync<dynamic>(query.Value);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        private static Task<ResultSet<dynamic>> SubmitSingleRequest(GremlinClient gremlinClient, string query)
        {
            try
            {
                var resultSet = gremlinCache.GetOrCreate(query, () => gremlinClient.SubmitAsync<dynamic>(query));
                return resultSet;
                //return gremlinClient.SubmitAsync<dynamic>(query);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public void PopulateGraph()
        {
            InitilizeConnection();

            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType,
                connectionPoolSettings,
                webSocketConfiguration))
            {
                foreach (var query in gremlinQueries)
                {
                    // Create async task to execute the Gremlin query.
                    var resultSet = SubmitRequest(gremlinClient, query).Result;
                    if (resultSet.Count > 0)
                    {
                        foreach (var result in resultSet)
                        {
                            string output = JsonConvert.SerializeObject(result);                            
                        }                        
                    }                    
                }                
            }
        }

        public void PopulateGraph(Dictionary<string, string> gQueries)
        {
            InitilizeConnection();

            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType,
                connectionPoolSettings,
                webSocketConfiguration))
            {
                foreach (var query in gQueries)
                {
                    // Create async task to execute the Gremlin query.
                    var resultSet = SubmitRequest(gremlinClient, query).Result;
                    if (resultSet.Count > 0)
                    {
                        foreach (var result in resultSet)
                        {
                            string output = JsonConvert.SerializeObject(result);
                        }
                    }
                }
            }
        }

        public int GetTotalProductCount()
        {
            InitilizeConnection();

            //string query = "g.V().out('hasfp').out('fphasproduct').count()";
            string query = "g.V().out('hasfp').out('fphasproduct').count()";

            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType,
                connectionPoolSettings,
                webSocketConfiguration))
            {
                // Create async task to execute the Gremlin query.
                var resultSet = SubmitSingleRequest(gremlinClient, query).Result;
                if (resultSet.Count > 0)
                {
                    foreach (var result in resultSet)
                    {
                       string output = JsonConvert.SerializeObject(result);
                        return Convert.ToInt32(output);
                    }
                }
            }
            return 0;
        }

        public int GetProductCountByFPId(string id)
        {
            InitilizeConnection();

            //string query = "g.V().haslabel('fpsummary').has('id', '57c6aa819ec6690ab0eb50e4').out('fphasproduct').count()";
            string query = "g.V().haslabel('fpsummary').has('id', '"+id+"').out('fphasproduct').count()";

            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType,
                connectionPoolSettings,
                webSocketConfiguration))
            {
                // Create async task to execute the Gremlin query.
                var resultSet = SubmitSingleRequest(gremlinClient, query).Result;
                if (resultSet.Count > 0)
                {
                    foreach (var result in resultSet)
                    {
                        string output = JsonConvert.SerializeObject(result);
                        return Convert.ToInt32(output);
                    }
                }
            }
            
            return 0;
        }

        public string GetTackingIDForTotalProductCount()
        {
            string id = GenerateTrackingId();

            //add id on gremlinTrackingCache

            //create execution task TPL and return tracking ID

            // Oncompletion update tracking ID on gremlinTrackingCache
            return "";


            //Tracking ID, Object, Timestamp
        //private Dictionary<string, object> cache = new Dictionary<string, object>();
        //Tracking ID, Timestamp
        //private Dictionary<string, DateTime> cacheTime = new Dictionary<string, DateTime>();

    }

        public string GetTackingIDForProductCountByFPId(string id)
        {
            //gremlinTrackingCache

            //add id on gremlinTrackingCache

            //create execution task TPL and return tracking ID

            // Oncompletion update tracking ID on gremlinTrackingCache
            return "";
        }

        public object GetDataByTrackingId(string id)
        {
            InitilizeConnection();

            string query = "g.V().out('hasfp').out('fphasproduct').count()";

            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType,
                connectionPoolSettings,
                webSocketConfiguration))
            {
                // Create async task to execute the Gremlin query.
                var resultSet = SubmitSingleRequest(gremlinClient, query).Result;
                if (resultSet.Count > 0)
                {
                    foreach (var result in resultSet)
                    {
                        string output = JsonConvert.SerializeObject(result);
                        return Convert.ToInt32(output);
                    }
                }
            }
            
            return 0;
        }

    }

}
