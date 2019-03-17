using System;
using System.Linq;
using System.Data;
using HtmlAgilityPack;
using System.Net;

namespace SchemaMapper.Converters
{

    public class HtmlImport: BaseImport, IDisposable
    {

        public HtmlImport(string htmlpath)
        {

            FilePath = htmlpath;

        }
        
        public DataSet GetDataSet()
        {


            string html = System.IO.File.ReadAllText(FilePath);

            html = WebUtility.HtmlDecode(html);
            HtmlAgilityPack.HtmlDocument htmldoc = new HtmlAgilityPack.HtmlDocument();

            htmldoc.LoadHtml(html);

            var tables = htmldoc.DocumentNode.SelectNodes("//table//tr")
                         .GroupBy(x => x.Ancestors("table").First()).ToList();

            for (int i = 0; i == tables.Count - 1; i++)
            {

                var rows = tables[i].ToList();
                Maindataset.Tables.Add(String.Format("Table {0}", i.ToString()));

                var headers = rows[0].Elements("th").Union(rows[0].Elements("td")).Select(x => new headerClass()
                {

                    Name = x.InnerText.Trim(),
                    Count = x.Attributes["colspan"] == null ? 1 : Convert.ToInt32(x.Attributes["colspan"].Value)

                }).ToList();


                if (headers.Count > 0)
                {

                    foreach (headerClass hr in headers)
                    {
                        for (int idx = 1; idx == hr.Count; idx++)
                        {

                            string postfix = hr.Count > 1 ? idx.ToString() : "";
                            Maindataset.Tables[i].Columns.Add(hr.Name + postfix);

                        }

                    }

                    for (int j = 1; j <= rows.Count - 1; j++)
                    {
                        var row = rows[j];
                        var dr = row.Elements("td").Select(x => x.InnerText + GetLink(x)).ToArray();
                        Maindataset.Tables[i].Rows.Add(dr);


                    }




                }
            }

            return Maindataset;
        }

        public string GetLink(HtmlNode html)
        {

            string strResult = "";

            if (html.Attributes["href"] != null)
            {
                strResult = " (" + html.Attributes["href"].Value + ")";

            }

            foreach (HtmlNode nd in html.ChildNodes)
            {

                if (nd.Attributes["href"] != null) { 
                if (strResult == "")
                    strResult = " (" + nd.Attributes["href"].Value + ")";
                else
                    strResult = strResult + Environment.NewLine + " (" + nd.Attributes["href"].Value + ")";
                }
            }

            return strResult;

        }

        public void Dispose()
        {
            Maindataset.Dispose();
        }
    }

    public class headerClass
    {


        public string Name { get; set; }
        public int Count { get; set; }

    }

}
