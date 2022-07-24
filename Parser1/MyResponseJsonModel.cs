using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parser1
{
    // Плоский класс представляющий получаемый JSON объект
    internal class MyResponseJsonModel
    {
        public Data data { get; set; }
    }

    internal class Data
    {
        public Searchreportwooddeal searchReportWoodDeal { get; set; }
    }

    internal class Searchreportwooddeal
    {
        public Content[] content { get; set; }
        public string __typename { get; set; }
    }

    internal class Content
    {
        private string _sellerName;
        public string sellerName 
        {
            get
            {
                if (_sellerName != null)
                {
                    string tmp = _sellerName.Replace("'", "''").Replace("`", "''");

                    if (tmp.Length > 250) return tmp.Substring(0, 250);

                    else return tmp;
                }

                return _sellerName = "";
            }

            set { _sellerName = value; }
        }

        private string _sellerInn;
        public string sellerInn 
        { 
            get
            {
                if (_sellerInn != null)
                {
                    if (_sellerInn.Length > 19) return _sellerInn.Substring(0, 19);

                    else return _sellerInn;
                }

                return _sellerInn = "";
            }

            set { _sellerInn = value; } 
        }

        private string _buyerName;
        public string buyerName
        {
            get
            {
                if (_buyerName != null)
                {
                    string tmp = _buyerName.Replace("'", "''").Replace("`", "''");

                    if (tmp.Length > 250) return tmp.Substring(0, 250);

                    else return tmp;
                }

                return _buyerName = "";
            }

            set { _buyerName = value; }
        }

        private string _buyerInn;
        public string buyerInn
        {
            get
            {
                if (_buyerInn != null)
                {
                    if (_buyerInn.Length > 19) return _buyerInn.Substring(0, 19);

                    else return _buyerInn;
                }

                return _buyerInn = "";
            }

            set { _buyerInn = value; }
        }
        public float woodVolumeBuyer { get; set; }
        public float woodVolumeSeller { get; set; }

        public string _dealDate;
        public string dealDate 
        { 
            get
            {
                if (_dealDate != null) return _dealDate;

                else return "20010101";
            } 

            set { _dealDate = value; } 
        }

        private string _dealNumber;
        public string dealNumber
        {
            get
            {
                if (_dealNumber?.Length > 29) return _dealNumber.Substring(0, 29);

                else return _dealNumber;
            }

            set { _dealNumber = value; }
        }
        public string __typename { get; set; }
    }
}
