
using Dapper;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using MPI;

namespace ConsoleApp4
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Repository repository = new Repository();

            parallelUnion(repository, args);
            
        }



        static void parallelUnion(Repository userRepository, string[] args) { 
           MPI.Environment.Run(ref args, comm =>
                {
                    var myRank = MPI.Communicator.world.Rank;
                    var size = MPI.Communicator.world.Size;

                  

                    var array = new List<Contact>();

                    DateTime start = DateTime.Now;
                    var queries = new List<string>();

                    int arraySize = 80;
                    if(!Int32.TryParse(args[0], out arraySize))
                    {
                        arraySize = 8;
                    }
                    

                    if (myRank == 0)
                    {
                        for (int i=0; i<arraySize; i++)
                        {
                            queries.Add("SELECT companyname, phone FROM Sales.Customers");
                            //queries.Add("SELECT companyname, phone FROM Sales.Customers  ");
                        }
                        if (size > 1)
                        {
                            for (int i=1; i<size; i++)
                            {
                                Communicator.world.Send(queries, i, myRank);
                            }
                        }
                        
                    }

                    if (myRank > 0)
                    {
                        queries = Communicator.world.Receive<List<string>>(0, 0);

                    }
                    //var arraySize = queries.Count;
                    var query = "";

//                  for (var i = myRank * (arraySize / size); i < (myRank + 1) * (arraySize / size); i++)
//                  {
//                   query = query + queries[i] + "UNION ALL ";
//                     }

                    
                    //  array = userRepository.GetContact(query.Substring(0, query.Length-10));
                    
                    if (myRank > 0)
                    {
                        
                        Communicator.world.Send(array, 0, myRank);
                        
                    }
                    

                    if (myRank == 0)
                    {
                        for (int i=1; i<size; i++)
                        {
                            var list = Communicator.world.Receive<List<Contact>>(i, i);
                            array.AddRange(list);
                        }
                        DateTime end = DateTime.Now;
                        TimeSpan ts = (end - start);
                        Console.WriteLine("Elapsed Time is {0} ms", ts.TotalMilliseconds);
                    }
                });
            }
        
    }

    

    [Serializable]
    public class Contact
    {
        public string companyname { get; set; }
        public string phone { get; set; }

        public Contact(string companyname, string phone)
        {
            this.companyname = companyname;
            this.phone = phone;
        }
    }

   


    
    public class Repository 
    {
        string connectionString = "Server=.\\SQLEXPRESS;Initial Catalog=TSQL2012; Encrypt=false; TrustServerCertificate=true; Integrated Security=True";
        public Repository()
        {
            
        }
        


        public List<Contact> GetContact(string query)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                //return db.Query<Contact>("SELECT companyname, phone FROM Sales.Customers UNION SELECT companyname, phone FROM Sales.Customers").ToList();
                return db.Query<Contact>(query).ToList();
            }
        }

       

        // public void Create(string query)
        // {
        //    using (IDbConnection db = new SqlConnection(connectionString))
        //    {
        //        //var sqlQuery = "INSERT INTO Sales.Shippers(companyname, phone) VALUES(@companyname, @phone)";
        //        db.Execute(query);
        //    }
        //}

        

        

    }
}