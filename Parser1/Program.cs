using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Parser1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int counter = 0;

            int totalCountSqlEx = 0;

            int totalRowInserted = 0;

            var timePassLocal = Stopwatch.StartNew();

            var timePassTotal = Stopwatch.StartNew();

            try
            {
                string connectionString = "Server=.\\SQLEXPRESS;Database=LesegaisParsed;Trusted_Connection=True;";
                
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    List<Content> data = new List<Content>();

                    sqlConnection.Open();

                    while (true)
                    {
                        //if (counter == 7) break;

                        data.Clear();

                        try
                        {
                            MyPostRequestToLesegais postRequest = new MyPostRequestToLesegais();

                            data = postRequest.GetResponseObject(50000, counter);

                            if (data.Count == 0) break;

                            Console.WriteLine("Data reading is done.");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Incorrect response from far source: " + ex.ToString() + ex.Message);

                            break;
                        }

                        DataTable dataTable = MyToDataTableConverter.ToDataTable<Content>(data);
                        
                        var sqlCommand = new SqlCommand("Proc_MyData", sqlConnection);
                        
                        sqlCommand.CommandType = CommandType.StoredProcedure;
                        
                        var sqlParameter = sqlCommand.Parameters.AddWithValue("@MyData_table", dataTable);
                        
                        sqlParameter.SqlDbType = SqlDbType.Structured;

                        var sqlParameterReturnValue = sqlCommand.Parameters.Add("@RowCount", SqlDbType.Int);

                        sqlParameterReturnValue.Direction = ParameterDirection.ReturnValue;
                        
                        sqlCommand.ExecuteNonQuery();
                        
                        counter++; totalRowInserted += (int)sqlParameterReturnValue.Value;

                        Console.WriteLine("TimePassLocal: " + timePassLocal.Elapsed + " | Rows: " + sqlParameterReturnValue.Value.ToString());

                        timePassLocal.Restart();
                    }

                    sqlConnection.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Main process is stopped: " + ex.ToString() + ex.Message);
            }

            Console.WriteLine();

            Console.WriteLine("Iterations: " + counter + " | TotalRowsInserted: " + totalRowInserted + " | TotalTimePassed: " + timePassTotal.Elapsed);

            Console.ReadLine();
        }


    }
}
