using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Extensions;

namespace RavenMigration
{
    public static class Extensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string databaseName)
        {
            var key = "Raven/Databases/" + databaseName;
            return documentStore.DatabaseCommands.ForSystemDatabase().DocumentExists(key);
        }


        public static bool DocumentExists(this IDatabaseCommands databaseCommands, string key)
        {
            var metadata = databaseCommands.Head(key);
            return metadata != null;
        }

        public static void CreateDataBase(this IDocumentStore store, string databaseName)
        {
            store.DatabaseCommands.CreateDatabase(new DatabaseDocument
            {
                Id = databaseName,
                Settings =
                {
                    {"Raven/ActiveBundles","PeriodicBackup;ScriptedIndexResults;Replication" }, 
                    {"Raven/DataDir","~\\Databases\\" + databaseName}
                }
            });
        }
    }
}