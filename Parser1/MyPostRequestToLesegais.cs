using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using Newtonsoft.Json;
using System.Data;

namespace Parser1
{
    // Класс реализует POST запрос, обработку ответа и десериализацию полученных объектов. 
    internal class MyPostRequestToLesegais
    {
        //public int CountRowsToRequest { get; set; }

        //public int CurrentAreaSelection { get; set; }

        /*
        private string _contentRequest = "{\"query\":\"query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, " +
                "$orders: [Order!]) {\n searchReportWoodDeal(filter: $filter, pageable: { number: $number, size: $size}, " +
                "orders: $orders) {\n content {\n sellerName\n sellerInn\n buyerName\n buyerInn\n woodVolumeBuyer\n " +
                "woodVolumeSeller\n dealDate\n dealNumber\n __typename\n    }\n __typename\n  }\n}\n\",\"variables\":{\"" +
                "size\":20,\"number\":1,\"filter\":null,\"orders\":null},\"operationName\":\"SearchReportWoodDeal\"}";
        */
        

        private HttpWebRequest _request;

        public MyPostRequestToLesegais()
        {          
            _request = WebRequest.CreateHttp("https://www.lesegais.ru/open-area/graphql");
                       
            _request.Method = "POST";                        

            _request.ContentType = "application/json";

            _request.Accept = "*/*";

            _request.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.62";

            _request.Referer = "https://www.lesegais.ru/open-area/deal";

            _request.KeepAlive = true;

            //request.Connection = "keep-alive";

            _request.Host = "www.lesegais.ru";

            _request.AutomaticDecompression = DecompressionMethods.GZip;

            _request.Headers.Add("Accept-Encoding", "gzip, deflate, br");

            _request.Headers.Add("Accept-Language", "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7");

            _request.Headers.Add("Origin", "https://www.lesegais.ru");

            _request.Headers.Add("sec-ch-ua", "\".Not/A)Brand\";v=\"99\", \"Microsoft Edge\";v=\"103\", \"Chromium\";v=\"103\"");

            _request.Headers.Add("sec-ch-ua-mobile", "?0");

            _request.Headers.Add("sec-ch-ua-platform", "\"Windows\"");

            _request.Headers.Add("Sec-Fetch-Dest", "empty");

            _request.Headers.Add("Sec-Fetch-Mode", "cors");

            _request.Headers.Add("Sec-Fetch-Site", "same-origin");
        }

        public List<Content> GetResponseObject(int size, int num)
        {
            //_request.KeepAlive

            string contentRequest = "{\"query\":\"query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, " +
                "$orders: [Order!]) {\\n searchReportWoodDeal(filter: $filter, pageable: { number: $number, size: $size}, " +
                "orders: $orders) {\\n content {\\n sellerName\\n sellerInn\\n buyerName\\n buyerInn\\n woodVolumeBuyer\\n " +
                "woodVolumeSeller\\n dealDate\\n dealNumber\\n __typename\\n    }\\n __typename\\n  }\\n}\\n\",\"variables\":{\"" +
                "size\":" + size + ",\"number\":" + num + ",\"filter\":null,\"orders\":null},\"operationName\":\"SearchReportWoodDeal\"}";

            _request.ContentLength = contentRequest.Length;

            byte[] dataArrayContentRequest = Encoding.UTF8.GetBytes(contentRequest);

            Console.WriteLine("Send request with length: " + contentRequest.Length);

            using (Stream dataStream = _request.GetRequestStream())
            {
                dataStream.Write(dataArrayContentRequest, 0, dataArrayContentRequest.Length);
            }

            //WebResponse response = await request.GetResponseAsync();

            HttpWebResponse response = (HttpWebResponse)_request.GetResponse();

            MyResponseJsonModel deserializedObject;

            using (Stream stream = response.GetResponseStream())
            {             
                using (StreamReader reader = new StreamReader(stream))
                {
                    string tmp = reader.ReadToEnd();
                    
                    deserializedObject = JsonConvert.DeserializeObject<MyResponseJsonModel>(tmp);
                }

                Console.WriteLine("Get count objects with response: " + deserializedObject.data.searchReportWoodDeal.content.Length);
            }
            
            response.Close();

            // Кладём набор десериализованных объектов в список и возвращаем из метода
            List<Content> notes = new List<Content>();

            foreach (var note in deserializedObject.data.searchReportWoodDeal.content)
            {
                notes.Add(note);
            }
            
            return notes;
        }

    }
}
