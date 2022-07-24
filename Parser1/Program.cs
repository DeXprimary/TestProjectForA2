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
        static int globalIterations = 0;

        static async Task Main(string[] args)
        {
            // Запускаем цикл выполнения программы с интервалом в 10 мин.
            while (true)
            {
                var timer = Task.Run(() => Thread.Sleep(TimeSpan.FromMinutes(10)));

                DoWork();

                await timer;
            }
        }

        static void DoWork()
        {
            int totalCounter = 0;

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

                    // Запускаем цикл парсинга пока не поступит инструкция выхода из цикла в связи с пустым списком данных
                    while (true)
                    {
                        //if (counter == 7) break;

                        data.Clear();

                        // Собираем в список десериализованные JSON данные из ответа на наш POST-запрос
                        try
                        {                            
                            MyPostRequestToLesegais postRequest = new MyPostRequestToLesegais();

                            data = postRequest.GetResponseObject(50000, totalCounter);

                            if (data.Count == 0) break;

                            Console.WriteLine("Data reading is done.");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Incorrect response from far source: " + ex.ToString() + ex.Message);

                            break;
                        }

                        // Преобразуем наш список объектов в DataTable 
                        DataTable dataTable = MyToDataTableConverter.ToDataTable<Content>(data);

                        var sqlCommand = new SqlCommand("Proc_MyData", sqlConnection);

                        sqlCommand.CommandType = CommandType.StoredProcedure;

                        // Отправляем наш DataTable на SQL сервер в виде параметра SQL-команды
                        var sqlParameter = sqlCommand.Parameters.AddWithValue("@MyData_table", dataTable);

                        sqlParameter.SqlDbType = SqlDbType.Structured;

                        // Добавим возвращаемый параметр для получения числа добавленных записей
                        var sqlParameterReturnValue = sqlCommand.Parameters.Add("@RowCount", SqlDbType.Int);

                        sqlParameterReturnValue.Direction = ParameterDirection.ReturnValue;

                        // Выполняем SQL-запрос
                        sqlCommand.ExecuteNonQuery();

                        totalCounter++; totalRowInserted += (int)sqlParameterReturnValue.Value;

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

            globalIterations++;

            Console.WriteLine();

            Console.WriteLine("Iteration done: " + globalIterations + " | TotalRowsInserted: " + totalRowInserted + " | TotalTimePassed: " + timePassTotal.Elapsed);

            Console.WriteLine();

            Console.WriteLine("Next iteration will be run in " + (600 - timePassTotal.ElapsedMilliseconds / 1000) + " sec.");
            
            Console.WriteLine();
        }
    }
}
