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
    public static class SBDeadLetterQueue
    {
        private const string EndpointUri = "https://mandartestcosmosdb.documents.azure.com:443/";
        private const string PrimaryKey = "H4csrUWWfcDjQwwrXl8CtKXvuYYRotvTHu4sZQoBsvlyAz1XtOHWBmXbJPOqyjV0xNUYprUpjhIBafw7IPtyFA==";
        private static DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey);
        private static string databaseName = "testdb";
        private static string collectionName = "error";
        [FunctionName("SBDeadLetterQueue")]
        public static void Run([ServiceBusTrigger("myqueue/$DeadLetterQueue", AccessRights.Listen, Connection = "mandartestsb_SERVICEBUS")]BrokeredMessage message, TraceWriter log)
        {
            try
            {

                Stream stream = message.GetBody<Stream>();
                StreamReader reader = new StreamReader(stream);
                string s = reader.ReadToEnd();
                log.Error($"DeadLetter Queue message: {s}");
                dynamic jsonObject = new JObject();
                jsonObject.message = s;
                client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), jsonObject).Wait();
                message.Complete();
            }
            catch (System.Exception ex)
            {

                log.Error($"exception: {ex}");
            }
        }
    }
}
