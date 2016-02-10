using System;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;


namespace DTE
{
    class Program
    {
        static void Main()
        {
            //This is the way we initialize a Cloud Storage Account with a plain stirng connection
            var account = CloudStorageAccount.Parse(
            $"DefaultEndpointsProtocol=https;AccountName=tucuenta;AccountKey=" +
            $"kjhkjhfsdfsdOpUUpWTZnwXe89kxANYoDAMEegL7G7wlCZhZ60UYNltbj6bZiH9x3eLHNSD1TEIQ+jw==");

            //We're gonna need a Table Client for table operations
            var tableClient = account.CreateCloudTableClient();
            //And this will be the table we are gonna work with
            var tblInventory = tableClient.GetTableReference("Inventory");


            //These are simple filters represented by plain strings, but tools as GenerateFilterCondition and CombineFilters
            //make the process less error prone
            var categoryFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.Equal,
                "Compute");

            var subcategoryFilter = TableQuery.GenerateFilterCondition(
                "SubCategory",
                QueryComparisons.Equal,
                "Laptop");

            //This will produce: (PartitionKey eq 'Compute') and (SubCategory eq 'Laptop')
            var combinedFilters = TableQuery.CombineFilters(
                categoryFilter,
                TableOperators.And,
                subcategoryFilter);

            //With the filter already assembled, we proceed to create a query that will include the filter we made
            var query = new TableQuery().Where(combinedFilters);
            //Now the table has to lazy-execute the query
            var virtualResults = 
                tblInventory.ExecuteQuery(query);
            //And here we iterate over the virtual results in order to get them in a working list
            var laptops = virtualResults.ToList();

            //Just show the results on the console. Observe how we can dynamically access the table properties
            foreach(var laptop in laptops)
            {
                Console.WriteLine(
                    string.Concat(
                        laptop.RowKey,
                        "\t",
                        laptop["StockAmount"].Int32Value));
            }

            Console.ReadLine();


            //From here on, we are going to make an insert/update

            //This is how we check for the existence of a unique element:
            var retrieve = TableOperation.Retrieve("Compute", "X220");
            var retrievedLaptop = tblInventory.Execute(retrieve);

            //We need to cast the resulting object to DynamicTableEntity, because we don't have mapped entities            
            var dynaLaptop = (DynamicTableEntity)retrievedLaptop.Result;

            //If null, then the register doesn't exist, so we have to create a new one
            if(dynaLaptop==null)
            {
                //The native properties are available out of the box
                dynaLaptop = new DynamicTableEntity()
                {
                    PartitionKey = "Compute",
                    RowKey = "X220"
                };

                //Other properties must be added like this:
                dynaLaptop.Properties.Add("StockAmount", EntityProperty.GeneratePropertyForInt(100));
                dynaLaptop.Properties.Add("SubCategory", EntityProperty.GeneratePropertyForString("Laptop"));
                //It is not necessary to strictly map all the properties of the cloud table.
                
                //You can even add properties that are NOT on the cloud yet!
                dynaLaptop.Properties.Add("HDD", EntityProperty.GeneratePropertyForString("300GB"));
            }
            else //If the entity exists, we just update it
            {
                //Observe how easy is access the dynamic property, and modify it
                dynaLaptop["StockAmount"].Int32Value += 100;
                //You can even add properties that are NOT on the cloud yet on an existing entity!
                dynaLaptop.Properties.Add("RAM", EntityProperty.GeneratePropertyForString("32GB"));
            }
            //The updated/new entity is ready. We just have to upload it:
            var updateOperation = TableOperation.InsertOrReplace(dynaLaptop);
            tblInventory.Execute(updateOperation);
        }
    }
}
