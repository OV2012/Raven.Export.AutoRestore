using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Smuggler;

namespace RavenMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("So. are you sure you want to start? Yes/No");
            using (var store = new DocumentStore
            {
                ConnectionStringName = "RavenDB"
            }.Initialize())
            {
                if (Console.ReadLine() == "Yes")
                {
                    Console.WriteLine("create master database");
                    var fileMaster  = Directory.GetFiles(@".\Master").First();

                    CreateDatabase(store, GetDatabaseName(fileMaster));
                    CreateData(store, GetDatabaseName(fileMaster), fileMaster);
                    
                    var backupFiles = Directory.GetFiles(@".\Tenants");
                    foreach (var file in backupFiles)
                    {
                        var tenantId = GetDatabaseName(file);
                        if (!store.DatabaseExists(tenantId))
                        {
                            CreateDatabase(store, tenantId);

                            CreateReplication(store, tenantId);

                            CreateData(store, tenantId, file);

                            CreateIndexes(store, tenantId);
                        }
                    }
                }

            }
        }

        private static void CreateIndexes(IDocumentStore store, string tenantId)
        {
            //?????
        }

        private static void CreateData(IDocumentStore store, string tenantId, string file)
        {
            Console.WriteLine("import data: {0}", tenantId);
            var connectionStringOptions = new RavenConnectionStringOptions
            {
                DefaultDatabase = tenantId,
                Url = ConfigurationManager.AppSettings["app:master"]
            };
            var smugglerApi = new SmugglerApi(new SmugglerOptions { }, connectionStringOptions);
            smugglerApi.ImportData(new SmugglerOptions
            {
                OperateOnTypes = ItemType.Documents,
                BackupPath = file,
                TransformScript = TransformScript(tenantId)
            }).Wait(TimeSpan.FromSeconds(15));

            WaitForNonStaleIndexes(store.DatabaseCommands.ForDatabase(tenantId));
        }

        private static string TransformScript(string tenantId)
        {
            if (tenantId.Contains("Global"))
            {
                return @"function(doc) { 
                        var type = doc['@metadata']['Raven-Entity-Name']; 
                        if(type === 'Songs' || type === 'Users')
                            return doc;
                        return null;
                    }";
            }
            return @"function(doc) { 
                        var type = doc['@metadata']['Raven-Entity-Name']; 
                        if(type === 'Songs' || type === 'Users')
                            return null;
                        return doc;
                    }";
        }

        private static void CreateReplication(IDocumentStore store, string tenantId)
        {
            Console.WriteLine("Setup replication: {0}", tenantId);
            using (var session = store.OpenSession("MusicMind.Global"))
            {
                var replicationDocument = session.Load<ReplicationDocument>("Raven/Replication/Destinations") ?? new ReplicationDocument();
                replicationDocument.Destinations = new List<ReplicationDestination>(replicationDocument.Destinations)
                {
                    new ReplicationDestination
                    {
                        Url = ConfigurationManager.AppSettings["app:master"],
                        Database = tenantId
                    }
                };
                session.Store(replicationDocument);
                session.SaveChanges();
            }

            WaitForNonStaleIndexes(store.DatabaseCommands.ForDatabase(tenantId));
            
        }

        private static void CreateDatabase(IDocumentStore store, string tenantId)
        {
            Console.WriteLine("Create database: {0}", tenantId);

            store.CreateDataBase(tenantId);

            WaitForNonStaleIndexes(store.DatabaseCommands.ForDatabase(tenantId));
        }

        private static void WaitForNonStaleIndexes(IDatabaseCommands databaseCommands)
        {
            while (databaseCommands.GetStatistics().StaleIndexes.Any())
            {
                Thread.Sleep(25);
            }
        }

        private static string GetDatabaseName(string file)
        {
            return Path.GetFileNameWithoutExtension(file).Replace(".ravendump", "");
        }
    }
}
