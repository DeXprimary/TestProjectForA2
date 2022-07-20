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
            var timePassLocal = Stopwatch.StartNew();

            var timePassTotal = Stopwatch.StartNew();

            int totalRowInserted = 0;

            int counter = 0;

            List<Content> tableData = new List<Content>();

            bool isNeedStop = false;

            while (!isNeedStop)
            {
                if (counter == 10) break;

                tableData.Clear();

                try
                {
                    //Thread.Sleep(1000);

                    MyPostRequestToLesegais postRequest = new MyPostRequestToLesegais();

                    tableData = postRequest.GetResponseObject(1000, counter);                                     

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

                string connectionString = "Server=.\\SQLEXPRESS;Database=LesegaisParsed;Trusted_Connection=True;";

                var sqlConnection = new SqlConnection(connectionString);

                try
                {
                    sqlConnection.Open();

                    Console.WriteLine("Connected to DB.");

                    int rowsInserted = 0;

                    foreach (var k in tableData)
                    {
                        if (TryInsertNote(sqlConnection, k))

                            rowsInserted++;
                    }

                    totalRowInserted += rowsInserted;

                    Console.WriteLine("Rows inserted: " + rowsInserted);

                    /*
                    bool inserted = TryInsertNote(sqlConnection, new Content
                    {
                        buyerName = "'TESTbuyer'",
                        buyerInn = "'TESTbuyerINN'",
                        sellerName = "'TESTseller'",
                        sellerInn = "'TESTsellerINN'",
                        dealNumber = "'78624537864533'",
                        dealDate = "'20071201'",
                        woodVolumeBuyer = 7f,
                        woodVolumeSeller = 7f
                    });

                    Console.WriteLine("Insert result: " + inserted);
                    */
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

                Console.WriteLine("TimePassedForIter: " + timePassLocal.Elapsed);

                timePassLocal.Restart();

                counter++;
            }                                             

            Console.WriteLine();

            Console.WriteLine("Iteration: " + counter + " | TotalRowInserted: " + totalRowInserted + " | TotalTimePassed: " + timePassTotal.Elapsed);

            Console.ReadLine();
        }



        private static bool TryInsertNote(SqlConnection sqlConnection, Content note)
        {
            string sqlCommandString =
                $"SELECT * " +
                $"FROM Agents " +
                $"WHERE AgentINN = '{note.buyerInn}' OR AgentINN = '{note.sellerInn}'";

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

            /*
            bool isInserted = false;

            var BuyerAgentId = CheckDuplicateOrInsertAgent(sqlConnection, note.buyerName, note.buyerInn);

            var SellerAgentId = CheckDuplicateOrInsertAgent(sqlConnection, note.sellerName, note.sellerInn);

            if (BuyerAgentId.HasValue && SellerAgentId.HasValue)
            {
                if (!CheckDuplicateOrInsertDeal(sqlConnection, note, BuyerAgentId.Value, SellerAgentId.Value).HasValue)

                    isInserted = true;
            }
           */
            return false;
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
