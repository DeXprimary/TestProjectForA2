using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Data.SqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;

namespace Parser1
{

    internal class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "Server=.\\SQLEXPRESS;Database=LesegaisParsed;Trusted_Connection=True;MultipleActiveResultSets=True";

            var sqlConnection = new SqlConnection(connectionString);

            try
            {
                sqlConnection.Open();

                Console.WriteLine("Connected to DB.");

                var timePassLocal = Stopwatch.StartNew();

                var timePassTotal = Stopwatch.StartNew();

                int totalRowInserted = 0;

                int counter = 0;

                List<Content> tableData = new List<Content>();

                bool isNeedStop = false;

                while (!isNeedStop)
                {
                    //if (counter == 1) break;

                    tableData.Clear();

                    try
                    {
                        //Thread.Sleep(1000);

                        MyPostRequestToLesegais postRequest = new MyPostRequestToLesegais();

                        tableData = postRequest.GetResponseObject(10000, counter);

                        if (tableData.Count == 0)
                        {
                            isNeedStop = true;

                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Have a trouble. Not have correct response. With message: " + ex.ToString() + ex.Message);

                        break;
                    }

                    //Console.WriteLine("Count data rows: " + tableData.Count);



                    int rowsInserted = 0;

                    foreach (var k in tableData)
                    {
                        if (TryInsertNote(sqlConnection, k))

                            rowsInserted++;

                        //Console.WriteLine("Строчка из текущей выборки: " + rowsInserted);
                    }

                    totalRowInserted += rowsInserted;

                    Console.WriteLine("Rows inserted: " + rowsInserted);

                    /*
                    try
                    {
                        sqlConnection.Open();

                        Console.WriteLine("Connected to DB.");                      
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Have a trouble. Operation not complete. With message: " + ex.Message);
                    }
                    finally
                    {
                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            sqlConnection.Close();

                            Console.WriteLine("Connection to DB closed.");
                        }
                    }
                    */

                    Console.WriteLine("TimePassedForIter: " + timePassLocal.Elapsed);

                    timePassLocal.Restart();

                    counter++;
                }

                Console.WriteLine();

                Console.WriteLine("Iteration: " + counter + " | TotalRowInserted: " + totalRowInserted + " | TotalTimePassed: " + timePassTotal.Elapsed);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Have a trouble. Operation not complete. With message: " + ex.Message);
            }
            finally
            {
                if (sqlConnection.State == ConnectionState.Open)
                {
                    sqlConnection.Close();

                    Console.WriteLine("Connection to DB closed.");
                }
            }            

            Console.ReadLine();
        }



        private static bool TryInsertNote(SqlConnection sqlConnection, Content note)
        {
            //if (note.buyerName != null) note.buyerName = note.buyerName.Replace("'", "''");

            //if (note.sellerName != null) note.sellerName = note.sellerName.Replace("'", "''");

            bool isInserted = false;

            string sqlCommandString =
                $"SELECT * " +
                $"FROM Agents " +
                $"WHERE AgentINN = '{note.buyerInn}' OR AgentINN = '{note.sellerInn}'";

            SqlCommand sqlCommandReader = new SqlCommand(sqlCommandString, sqlConnection);

            int? BuyerAgentId = null;

            int? SellerAgentId = null;

            using (SqlDataReader reader = sqlCommandReader.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    reader.Read();

                    string name = "", inn = "";

                    if ((string)reader["AgentINN"] == note.buyerInn)
                    {
                        BuyerAgentId = (int)reader["AgentId"];

                        name = note.sellerName; 

                        inn = note.sellerInn;
                    }
                    else
                    {
                        SellerAgentId = (int)reader["AgentId"];

                        name = note.buyerName;

                        inn = note.buyerInn;
                    } 
                                        
                    if (reader.Read())
                    {
                        if (BuyerAgentId.HasValue) SellerAgentId = (int)reader["AgentId"];

                        else BuyerAgentId = (int)reader["AgentId"];
                    }
                    else
                    {
                        sqlCommandString =
                            $"INSERT INTO Agents (AgentName, AgentINN) " +
                            $"VALUES ('{name}', '{inn}') " +
                            $"SELECT SCOPE_IDENTITY()";

                        SqlCommand sqlCommandQuery = new SqlCommand(sqlCommandString, sqlConnection);


                        int id = int.Parse(sqlCommandQuery.ExecuteScalar().ToString());

                        if (BuyerAgentId.HasValue) SellerAgentId = id;

                        else BuyerAgentId = id;
                    }
                }
                else
                {
                    sqlCommandString =
                        $"INSERT INTO Agents (AgentName, AgentINN) " +
                        $"VALUES ('{note.buyerName}', '{note.buyerInn}') " +
                        $"SELECT SCOPE_IDENTITY()";

                    SqlCommand sqlCommandQuery = new SqlCommand(sqlCommandString, sqlConnection);
                    
                    BuyerAgentId = int.Parse(sqlCommandQuery.ExecuteScalar().ToString());

                    sqlCommandString =
                        $"INSERT INTO Agents (AgentName, AgentINN) " +
                        $"VALUES ('{note.sellerName}', '{note.sellerInn}') " +
                        $"SELECT SCOPE_IDENTITY()";

                    sqlCommandQuery = new SqlCommand(sqlCommandString, sqlConnection);
                    
                    SellerAgentId = int.Parse(sqlCommandQuery.ExecuteScalar().ToString());
                }
            }

            /*           
            var BuyerAgentId = CheckDuplicateOrInsertAgent(sqlConnection, note.buyerName, note.buyerInn);

            var SellerAgentId = CheckDuplicateOrInsertAgent(sqlConnection, note.sellerName, note.sellerInn);
            */
            if (BuyerAgentId.HasValue && SellerAgentId.HasValue)
            {
                if (!CheckDuplicateOrInsertDeal(sqlConnection, note, BuyerAgentId.Value, SellerAgentId.Value).HasValue)

                    isInserted = true;
            }
           
            return isInserted;
        }

        private static int? CheckDuplicateOrInsertAgent(SqlConnection sqlConnection, string name, string inn)
        {
            int? noteId = null;

            //if (name.Contains("'")) { name = name.Replace("''","\""); name = name.Replace("'", "\'\'"); }

            string sqlCommandString =
                $"SELECT * FROM Agents " +
                $"WHERE AgentINN = '{inn}'"; //AgentName = '{name}' AND 

            SqlCommand sqlCommand = new SqlCommand(sqlCommandString, sqlConnection);
            
            using (SqlDataReader reader = sqlCommand.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    reader.Read();

                    noteId = (int)reader["AgentId"];

                    return noteId;
                }
            }

            sqlCommandString =
                $"INSERT INTO Agents (AgentName, AgentINN) " +
                $"VALUES ('{name}', '{inn}')";

            SqlCommand sqlCommandInsert = new SqlCommand(sqlCommandString, sqlConnection);

            sqlCommandInsert.ExecuteNonQuery();

            return CheckDuplicateOrInsertAgent(sqlConnection, name, inn);
        }

        private static int? CheckDuplicateOrInsertDeal(SqlConnection sqlConnection, Content note, int BuyerAgentId, int SellerAgentId)
        {
            int? noteId = null;

            string sqlCommandString =
                    $"SELECT * FROM Deals " +
                    $"WHERE DealNumber = '{note.dealNumber}' AND BuyerAgentId = {BuyerAgentId} AND SellerAgentId = {SellerAgentId}";

            SqlCommand sqlCommand = new SqlCommand(sqlCommandString, sqlConnection);
            
            using (SqlDataReader reader = sqlCommand.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    reader.Read();

                    noteId = (int)reader["DealId"];

                    return noteId;
                }
            }
            
            sqlCommandString =
                $"INSERT INTO Deals (DealNumber, DealDate, BuyerAgentId, SellerAgentId, BuyerWoodVolume, SellerWoodVolume) " +
                $"VALUES ('{note.dealNumber}', '{note.dealDate}', {BuyerAgentId}, {SellerAgentId}, " +
                $"{note.woodVolumeBuyer.ToString(System.Globalization.CultureInfo.GetCultureInfo("en-US"))}, " +
                $"{note.woodVolumeSeller.ToString(System.Globalization.CultureInfo.GetCultureInfo("en-US"))}) ";
            
            SqlCommand sqlCommandInsert = new SqlCommand(sqlCommandString, sqlConnection);

            sqlCommandInsert.ExecuteNonQuery();

            return noteId;
        }

    }
}
