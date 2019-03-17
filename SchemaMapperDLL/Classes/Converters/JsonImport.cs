using System;
using System.Linq;
using System.Data;
using Microsoft.VisualBasic;
using Newtonsoft.Json;

namespace SchemaMapper.Converters
{
    public class JsonImport: BaseImport, IDisposable
    {

        private int[]  m_intEncodings = { 1256, 1200, 1201, 12000, 12001, 65000, 65001 };
        private char[] chrsNumeric = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
        private char[] chrsUnwanted = { '\\', '-', '.', ' ', '`', '\'', Strings.Chr(34), '/', '+', '=', ',', '_', '|', '\t' };
        private char[] chrsEnglishAlphabetic = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
        private char[] chrsArabicAlphabetic = { 'ذ', 'ض', 'ص', 'ث', 'ق', 'ف', 'غ', 'ع', 'ه', 'خ', 'ح', 'ج', 'د', 'ش', 'س', 'ي', 'ب', 'ل', 'ا', 'ت', 'ن', 'م', 'ك', 'ط', 'ئ', 'ء', 'ؤ', 'ر', 'ى', 'ة', 'و', 'ز', 'ظ', 'إ', 'أ', 'آ' };

        public JsonImport(string JsonPath)
        {

            FilePath = JsonPath;
        }

        public DataSet ReadDataSetFromJson()
        {

           string strJson;
           using (System.IO.StreamReader srJson = new System.IO.StreamReader(FilePath, DetectEncoding(FilePath)) ) {

           strJson= srJson.ReadToEnd();
           srJson.Close();
    
           }

           Maindataset = JsonConvert.DeserializeObject<DataSet>(strJson);

           return Maindataset;

        }

        public System.Text.Encoding DetectEncoding(string strFilePath)
        {
            foreach (int encoding in m_intEncodings)
            {
                using (System.IO.StreamReader txtReader = new System.IO.StreamReader(strFilePath, System.Text.Encoding.GetEncoding(encoding)))
                {
                    string strFirstLine = txtReader.ReadLine();

                    while (string.IsNullOrEmpty(strFirstLine.Trim()) && !txtReader.EndOfStream)

                        strFirstLine = txtReader.ReadLine();

                    txtReader.Close();

                    if (!strFirstLine.Contains("�") && !string.IsNullOrEmpty(strFirstLine.Trim()) && !strFirstLine.Contains(""))
                    {
                        if (strFirstLine.ToCharArray().Where(x => chrsArabicAlphabetic.Contains(x) | chrsEnglishAlphabetic.Contains(x) | chrsNumeric.Contains(x)).Count() > 0)

                            return System.Text.Encoding.GetEncoding(encoding);

                        // this.strFirstLine = strFirstLine;
                    }
                }
            }

            return System.Text.Encoding.Unicode;
        }

        public void Dispose()
        {
            Maindataset.Dispose();
        }

    }
}
