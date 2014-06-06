using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DTE
{
    class Program
    {
        static void Main(string[] args)
        {
            //This is the way we initialize a Cloud Storage Account with a plain stirng connection
            CloudStorageAccount _account = CloudStorageAccount.Parse(
            String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", "tucuenta",
            "kjhkjhfsdfsdOpUUpWTZnwXe89kxANYoDAMEegL7G7wlCZhZ60UYNltbj6bZiH9x3eLHNSD1TEIQ+jw=="));

            //We're gonna need a Table Client for table operations
            CloudTableClient _tableClient = _account.CreateCloudTableClient();
            //And this will be the table we are gonna work with
            CloudTable _tblInventory = _tableClient.GetTableReference("Inventory");


            //These are simple filters represented by plain strings, but tools as GenerateFilterCondition and CombineFilters
            //make the process less error prone
            string categoryFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.Equal,
                "Compute");

            string subcategoryFilter = TableQuery.GenerateFilterCondition(
                "SubCategory",
                QueryComparisons.Equal,
                "Laptop");

            //This will produce: (PartitionKey eq 'Compute') and (SubCategory eq 'Laptop')
            string combinedFilters = TableQuery.CombineFilters(
                categoryFilter,
                TableOperators.And,
                subcategoryFilter);

            //With the filter already assembled, we proceed to create a query that will include the filter we made
            TableQuery query = new TableQuery().Where(combinedFilters);
            //Now the table has to lazy-execute the query
            IEnumerable<DynamicTableEntity> virtualResults = 
                _tblInventory.ExecuteQuery(query);
            //And here we iterate over the virtual results in order to get them in a working list
            List<DynamicTableEntity> laptops = virtualResults.ToList();

            //Just shwo the results on the console. Observe how we can dynamically access the table properties
            foreach(var laptop in laptops)
            {
                Console.WriteLine(
                    String.Concat(
                        laptop.RowKey,
                        "\t",
                        laptop["StockAmount"].Int32Value));
            }

            Console.ReadLine();


            //From here on, we are going to make aninsert/update

            //This is how we check for the existence of a unique element:
            TableOperation retrieve = TableOperation.Retrieve("Compute", "X220");
            TableResult retrievedLaptop = _tblInventory.Execute(retrieve);

            //We need to cast the resulting object to DynamicTableEntity, because we don't have mapped entities            
            DynamicTableEntity dynaLaptop = (DynamicTableEntity)retrievedLaptop.Result;

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
            TableOperation updateOperation = TableOperation.InsertOrReplace(dynaLaptop);
            _tblInventory.Execute(updateOperation);
        }
    }
}
