using System;
using System.Linq;
using Microsoft.VisualBasic;
using System.Data;
using Microsoft.VisualBasic.FileIO;

namespace SchemaMapper.Converters
{
    public class FlatFileImportTools : IDisposable
    {

        #region declaration

        private int m_intRowsToSkip = 0;
        private DataTable m_DataTable;
        private string m_strFilePath = string.Empty;
        private string m_strDelimiter = "";
        private bool m_boolContainsHeader = false;
        private int m_intNumberOfColumns = 0;
        private bool m_FieldEnclosedInQuotes = false;
        private TextFieldParser tfpTxtParser;
        private string[] strFirstLine;
        private string[] m_strDelimiters = { ":", "|", ",", ";", "\t", "\r","\r\n", "\n" };
        private int[] m_intEncodings =  { 1256, 1200, 1201, 12000, 12001, 65000, 65001 };
        private string[] m_strQualifiers =  { "\"", "'" };
        private  string m_strDefaultDelimiter = ";";
        private string[] m_strFirstLines =  { "", "", "", "", "", "", "", "" };
        private string m_strTextQualifier = string.Empty;
        private char[] chrsNumeric = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
        private char[] chrsUnwanted = { '\\', '-', '.', ' ', '`', '\'', Strings.Chr(34), '/', '+', '=', ',', '_', '|', '\t' };
        private char[] chrsEnglishAlphabetic =  { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
        private char[] chrsArabicAlphabetic =  { 'ذ', 'ض', 'ص', 'ث', 'ق', 'ف', 'غ', 'ع', 'ه', 'خ', 'ح', 'ج', 'د', 'ش', 'س', 'ي', 'ب', 'ل', 'ا', 'ت', 'ن', 'م', 'ك', 'ط', 'ئ', 'ء', 'ؤ', 'ر', 'ى', 'ة', 'و', 'ز', 'ظ', 'إ', 'أ', 'آ' };

        #endregion

        #region properties

        public string TextQualifier
        {
            get
            {
                return m_strTextQualifier;
            }
        }

        public bool FieldEnclosedInQuotes
        {
            get
            {
                return m_FieldEnclosedInQuotes;
            }
        }

        public int NumberOfColumns
        {
            get
            {
                return m_intNumberOfColumns;
            }
        }

        public string FilePath
        {
            get
            {
                return m_strFilePath;
            }
        }

        public string Delimiter
        {
            get
            {
                return m_strDelimiter;
            }
        }

        public bool FirstRowIsHeader
        {
            get
            {
                return m_boolContainsHeader;
            }
        }

        public DataTable DataTable
        {
            get
            {
                return m_DataTable;
            }
        }

        #endregion

        #region constructors

        public FlatFileImportTools(string strFilePath, bool ContainsHeader = false, int v_intRowsToSkip = 0)
        {
            m_DataTable = new DataTable();

            m_intRowsToSkip = v_intRowsToSkip;
            m_strFilePath = strFilePath;
            m_boolContainsHeader = ContainsHeader;

            m_strDelimiter = DetectDelimiter(strFilePath);

            m_strTextQualifier = DetectTextQualifier(m_strDelimiter);

            m_FieldEnclosedInQuotes = string.IsNullOrEmpty(m_strTextQualifier) ? false : true;


            tfpTxtParser = new TextFieldParser(this.FilePath, DetectEncoding(this.FilePath));
            tfpTxtParser.TextFieldType = FieldType.Delimited;
            tfpTxtParser.SetDelimiters(this.Delimiter.ToString());
            tfpTxtParser.HasFieldsEnclosedInQuotes = this.FieldEnclosedInQuotes;

            if (m_intRowsToSkip > 0)
            {
                for (var i = 1; i <= m_intRowsToSkip; i++)

                    tfpTxtParser.ReadLine();
            }
        }






        #endregion

        #region "Methods"

        public void BuildDataTableStructure()
        {
            strFirstLine = tfpTxtParser.ReadFields();
            m_intNumberOfColumns = GetColumnsCount();

            for (var intCounter = 0; intCounter <= m_intNumberOfColumns - 1; intCounter++)
            {
                DataColumn dcCol = new DataColumn();

                if (this.FirstRowIsHeader && strFirstLine.Length > intCounter)
                {
                    dcCol.ColumnName = strFirstLine[intCounter].Trim();
                    dcCol.Caption = strFirstLine[intCounter].Trim();
                }
                else
                {
                    dcCol.ColumnName = "F" + intCounter.ToString();
                    dcCol.Caption = "F" + intCounter.ToString();
                }

                m_DataTable.Columns.Add(dcCol);
            }
        }

        public void InsertLineIntoTable(string[] strLine)
        {
            int intColumns = 0;
            DataRow drRow;
            drRow = m_DataTable.NewRow();

            if (m_intNumberOfColumns > strLine.Length)
                intColumns = strLine.Length;
            else
                intColumns = m_intNumberOfColumns;

            for (var intCounter = 0; intCounter <= intColumns - 1; intCounter++)
                drRow[intCounter] = strLine[intCounter];

            m_DataTable.Rows.Add(drRow);
        }

        public DataTable FillDataTable()
        {
            if (!this.FirstRowIsHeader)
                InsertLineIntoTable(strFirstLine);

            while (tfpTxtParser.EndOfData == false)
            {
                string[] strCurrentLine;
                strCurrentLine = tfpTxtParser.ReadFields();
                InsertLineIntoTable(strCurrentLine);
            }

            return DataTable;
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

        public void ExportDataTableToXml(string xmlPath)
        {
            m_DataTable.WriteXml(xmlPath);
        }

        public string DetectDelimiter(string strFilePath)
        {
            string strDelimiter = "";
            int intMax = 0;


            using (System.IO.StreamReader txtReader = new System.IO.StreamReader(strFilePath))
            {
                for (var intCount = m_intRowsToSkip; intCount <= m_intRowsToSkip + 7; intCount++)
                {
                    if (txtReader.Peek() != -1)
                        m_strFirstLines[intCount - m_intRowsToSkip] = txtReader.ReadLine();
                    else
                        break;
                }

                txtReader.Close();
            }

            foreach (string Str in m_strDelimiters)
            {
                string[] strArray = m_strFirstLines[0].Split(new string[] {Str},StringSplitOptions.None);

                if (strArray.Length > intMax)
                {
                    strDelimiter = Str;
                    intMax = strArray.Length;
                }
            }

            if (intMax == 1)
                return m_strDefaultDelimiter;
            else
                return strDelimiter;
        }

        public string DetectTextQualifier(string strDelimiter)
        {
            string r_strQualifier = string.Empty;

            foreach (string strQualifier in m_strQualifiers)
            {
                bool IsQualifier = true;

                foreach (string str in m_strFirstLines[0].Split(new string[] { Delimiter }, StringSplitOptions.None))
                {
                    if (!str.StartsWith(strQualifier) || !str.EndsWith(strQualifier))
                    {
                        IsQualifier = false;

                        break;
                    }
                }

                if (IsQualifier == true)
                {
                    r_strQualifier = strQualifier;
                    break;
                }
            }




            return r_strQualifier;
        }

        public int GetColumnsCount()
        {
            int r_intResult = 0;

            foreach (string Str in m_strFirstLines)
            {
                if (Str.Split(new string[]{Delimiter},StringSplitOptions.None).Length > r_intResult)
                    r_intResult = Str.Split(new string[] { Delimiter }, StringSplitOptions.None).Length;
            }

            return r_intResult;
        }

        #endregion

                private bool disposedValue = false;        // To detect redundant calls

        // IDisposable
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    if (m_DataTable != null)
                        m_DataTable.Dispose();

                    m_strFilePath = null;
                    m_boolContainsHeader = default(bool);
                    m_intNumberOfColumns = default(int);
                    m_FieldEnclosedInQuotes = default(bool);
                    tfpTxtParser.Dispose();
                    strFirstLine = null;
                }
            }
            this.disposedValue = true;
        }

        // This code added by Visual Basic to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code.  Put cleanup code in Dispose(ByVal disposing As Boolean) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
