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
            CloudStorageAccount _account = CloudStorageAccount.Parse(
            String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", "tucuenta",
            "kjhkjhfsdfsdOpUUpWTZnwXe89kxANYoDAMEegL7G7wlCZhZ60UYNltbj6bZiH9x3eLHNSD1TEIQ+jw=="));

            CloudTableClient _tableClient = _account.CreateCloudTableClient();
            CloudTable _tblInventory = _tableClient.GetTableReference("Inventory");

            string categoryFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.Equal,
                "Compute");

            string subcategoryFilter = TableQuery.GenerateFilterCondition(
                "SubCategory",
                QueryComparisons.Equal,
                "Laptop");

            string combinedFilters = TableQuery.CombineFilters(
                categoryFilter,
                TableOperators.And,
                subcategoryFilter);

            TableQuery query = new TableQuery().Where(combinedFilters);
            IEnumerable<DynamicTableEntity> virtualResults = 
                _tblInventory.ExecuteQuery(query);
            List<DynamicTableEntity> laptops = virtualResults.ToList();

            foreach(var laptop in laptops)
            {
                Console.WriteLine(
                    String.Concat(
                        laptop.RowKey,
                        "\t",
                        laptop["StockAmount"].Int32Value));
            }

            Console.ReadLine();

            TableOperation retrieve = TableOperation.Retrieve("Compute", "X220");
            TableResult retrievedLaptop = _tblInventory.Execute(retrieve);
            //Tenemos que hacer un castig a DynamicEntity para trabajar ágilmente
            //El resultado originalmente viene en un object
            DynamicTableEntity dynaLaptop = (DynamicTableEntity)retrievedLaptop.Result;

            if(dynaLaptop==null)
            {
                dynaLaptop = new DynamicTableEntity()
                {
                    PartitionKey = "Compute",
                    RowKey = "X220"
                };
                dynaLaptop.Properties.Add("StockAmount", EntityProperty.GeneratePropertyForInt(100));
                dynaLaptop.Properties.Add("SubCategory", EntityProperty.GeneratePropertyForString("Laptop"));
                //No es neecsario crear propiedades para todos los campos existentes en la tabla.
                //Aquí intencionalmente dejé por fuera ModelName y UnitPrice.
                //Además dada la flexibilidad del Azure Storage, podemos agregar propiedades que 
                //no estaban antes en la tabla:
                dynaLaptop.Properties.Add("HDD", EntityProperty.GeneratePropertyForString("300GB"));
            }
            else
            {
                dynaLaptop["StockAmount"].Int32Value += 100;                
                //Aquí también podemos aregar nuevas propiedades
                dynaLaptop.Properties.Add("RAM", EntityProperty.GeneratePropertyForString("32GB"));
            }
            TableOperation updateOperation = TableOperation.InsertOrReplace(dynaLaptop);
            _tblInventory.Execute(updateOperation);
        }
    }
}
