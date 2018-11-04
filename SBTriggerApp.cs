using System;
using System.IO;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Company.Function
{
    public static class SBTriggerApp
    {
        private const string EndpointUri = "https://mandartestcosmosdb.documents.azure.com:443/";
        private const string PrimaryKey = "H4csrUWWfcDjQwwrXl8CtKXvuYYRotvTHu4sZQoBsvlyAz1XtOHWBmXbJPOqyjV0xNUYprUpjhIBafw7IPtyFA==";
        private static DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey);
        private static string databaseName = "testdb";
        private static string collectionName = "test";

        [FunctionName("SBTriggerApp")]
        public static void Run([ServiceBusTrigger("myqueue", AccessRights.Listen, Connection = "mandartestsb_SERVICEBUS")]BrokeredMessage message, TraceWriter log)
        {
            try
            {
                Stream stream = message.GetBody<Stream>();
                StreamReader reader = new StreamReader(stream);
                string s = reader.ReadToEnd();
                log.Info($"message: {s}");
                dynamic jsonObject = new JObject();
                jsonObject.message = s;
                if (s.Contains("3"))
                {
                    log.Warning(" DeliveryCount:----> " + message.DeliveryCount.ToString());
                    message.AbandonAsync().Wait();                    
                }
                else    
                {
                    message.CompleteAsync().Wait();
                }

                client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), jsonObject).Wait();
            }
            catch (System.Exception ex)
            {

                throw ex;
            }

        }
    }
}
