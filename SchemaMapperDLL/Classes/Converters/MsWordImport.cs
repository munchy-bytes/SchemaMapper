using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.IO;
using Microsoft.VisualBasic;
using System.Data;
using Microsoft.Office.Interop.Word;
using System.Management;

namespace SchemaMapper.Converters
{
    public class MsWordImportTools : IDisposable
    {

        #region decalrations

        private string m_computername = string.Empty;
        private string m_Username = string.Empty;

        private int m_intRowsToSkip = 0;
        private string m_strWordPath = string.Empty;
        private DataSet m_Maindataset = new DataSet();
        // Private xmlConverter As New TableConverter
        private Application appWord = null;
        private Documents docsWord = null;
        private Document newDoc = null;
        private List<string> m_lstTablesStrings = new List<string>();
        private bool m_HasHeader = false;




        #endregion

        #region properties

       public string WordDocumentPath
        {
            get
            {
                return m_strWordPath;
            }
        }

        public bool FirstRowContainsHeader
        {
            get
            {
                return m_HasHeader;
            }
        }

        public DataSet MainDataset
        {
            get
            {
                return m_Maindataset;
            }
        }
        #endregion
        
        #region constructors

        public MsWordImportTools(string DocumentPath, bool firstRowContainsHeader = false, int v_intRowsToSkip = 0)
        {
            m_strWordPath = DocumentPath;
            m_HasHeader = firstRowContainsHeader;
            m_computername = Environment.MachineName;
            m_Username = Environment.UserName;
            m_intRowsToSkip = v_intRowsToSkip;
        }

        #endregion

        #region Methods

        public void ExportDatasetToXml(string xmlPath)
        {
            m_Maindataset.WriteXml(xmlPath);
        }

        public void ImportWordTablesIntoListUsingConvertToText(char delimiter)
        {
             Range rngTable;
            string strTable = string.Empty;

            appWord = new Application();
            appWord.Visible = false;
    
            docsWord = appWord.Documents;
            newDoc = docsWord.Open(this.WordDocumentPath);


            foreach (Table wTable in newDoc.Tables)
            {
                rngTable = wTable.ConvertToText(Separator: delimiter, NestedTables: true);
                // For Each wRow As Word.Row In wTable.Rows

                strTable = rngTable.Text;
                newDoc.Undo();

                m_lstTablesStrings.Add(strTable);
            }

            newDoc.Close(SaveChanges: false);
            // docsWord.Close(SaveChanges:=False)
            appWord.Quit(SaveChanges: false);


            System.Runtime.InteropServices.Marshal.ReleaseComObject(newDoc);
            System.Runtime.InteropServices.Marshal.ReleaseComObject(appWord);



       
        }

        public DataSet ConvertListToTables(string v_delimiter)
        {
            foreach (var strTable in m_lstTablesStrings)
            {
                int intColumnsCount = GetColumnsCount(strTable, v_delimiter);
                string[] strRows = strTable.Split(new string[]{Environment.NewLine},StringSplitOptions.None).Skip(m_intRowsToSkip).ToArray();
                int intRowsCounter = 0;
                System.Data.DataTable dtTable = new System.Data.DataTable();

                foreach (string drRow in strRows)
                {
                    if (string.IsNullOrEmpty(drRow.Trim()))
                        continue;

                    string[] strCells = drRow.Split(new string[] { v_delimiter}, StringSplitOptions.None);



                    if (m_HasHeader && intRowsCounter == 0)
                    {
                        for (var intCount = 0; intCount <= intColumnsCount - 1; intCount++)
                        {
                            DataColumn dtCol = new DataColumn();

                            if (strCells.Length <= intCount || string.IsNullOrEmpty(strCells[intCount]))
                            {
                                dtCol.ColumnName = "F" + intCount;
                                dtCol.Caption = "F" + intCount;
                            }
                            else
                            {
                                dtCol.ColumnName = strCells[intCount].Trim();
                                // .Replace(" ", "_")

                                dtCol.Caption = strCells[intCount].Trim();
                            }

                            dtTable.Columns.Add(dtCol);
                        }

                        intRowsCounter += 1;
                        continue;
                    }
                    else if (intRowsCounter == 0)
                    {
                        for (int intCount = 0; intCount <= intColumnsCount - 1; intCount++)
                        {
                            DataColumn dtCol = new DataColumn();

                            dtCol.ColumnName = "F" + intCount;
                            dtCol.Caption = "F" + intCount;

                            dtTable.Columns.Add(dtCol);
                        }
                    }
                    // Try


                    InsertArrayofStringIntoDataRow(strCells, ref dtTable);
                    intRowsCounter += 1;
                }

                m_Maindataset.Tables.Add(dtTable);
            }

            return m_Maindataset;
        }

        public int GetColumnsCount(string strTable, string delimiter)
        {
            int r_intResult = 0;
            string[] strRows = strTable.Split(new string[] {Environment.NewLine},StringSplitOptions.None);

            for (var i = 0; i <= 8; i++)
            {
                if (strRows.Length > i)
                {
                    string[] strCells = strRows[i].Split(new string[] { delimiter }, StringSplitOptions.None);

                    if (strCells.Length > r_intResult)
                        r_intResult = strCells.Length;
                }
                else
                    break;
            }

            return r_intResult;
        }

        public void ImportWordTablesIntoList(string delimiter)
        {
            
            // Dim rngTable As Word.Range
            string strTable = string.Empty;

            appWord = new Application();
            appWord.Visible = false;

            docsWord = appWord.Documents;
            newDoc = docsWord.Open(this.WordDocumentPath);


            foreach (Table wTable in newDoc.Tables)
            {
                string strText = string.Empty;
                var intRowCounter = 1;
                int intColumnIndex = 1;
                foreach (Cell wCell in wTable.Range.Cells)
                {
                    if (wCell.RowIndex == intRowCounter)
                    {
                        for (var intCount = 1; intCount <= (wCell.ColumnIndex - intColumnIndex); intCount++)
                            strText += delimiter;

                        strText += CleaningCellText(wCell.Range.Text, delimiter);

                        intColumnIndex = wCell.ColumnIndex;
                    }
                    else
                    {
                        strText += Environment.NewLine + CleaningCellText(wCell.Range.Text, delimiter);

                        intColumnIndex = wCell.ColumnIndex;
                        intRowCounter += 1;
                    }
                }

                m_lstTablesStrings.Add(strText);
            }

            newDoc.Close(SaveChanges: false);
            // docsWord.Close(SaveChanges:=False)
            appWord.Quit(SaveChanges: false);

            System.Runtime.InteropServices.Marshal.ReleaseComObject(newDoc);
            System.Runtime.InteropServices.Marshal.ReleaseComObject(appWord);

        }

        public string CleaningCellText(string v_strText, string delimiter)
        {
            string strText = v_strText.Trim();

            strText = strText.Replace(Environment.NewLine, " ");
            strText = strText.Replace(Constants.vbCr, " ");
            strText = strText.Replace(Constants.vbLf, " ");
            strText = strText.Replace("", " ");
            strText = strText.Replace(delimiter, " ");
            strText = strText.Replace("  ", " ").Replace("  ", " ");

            return strText;
        }

        public void InsertArrayofStringIntoDataRow(string[] arrayStr, ref System.Data.DataTable dtTable)
        {
            DataRow dtRow;
            dtRow = dtTable.NewRow();
            int intColCounter = 0;

            foreach (string strCell in arrayStr)
            {
                dtRow[intColCounter] = strCell;
                intColCounter += 1;
            }

            dtTable.Rows.Add(dtRow);
        }

        public void WriteToTxtFile(string strFilePath, System.Text.Encoding encoding)
        {
            StreamWriter fsWriteFile = new StreamWriter(strFilePath, true, encoding);

            int intCounter = 0;
            foreach (string Str in m_lstTablesStrings)
            {
                fsWriteFile.Write(Str);

                if (intCounter < m_lstTablesStrings.Count - 1)
                {
                    fsWriteFile.Write(Environment.NewLine);
                    fsWriteFile.Write("------------------------------");
                    fsWriteFile.Write(Environment.NewLine);
                }


                intCounter += 1;
            }

            fsWriteFile.Close();
        }

        public void WriteToTxtFile(string strFilePath, int codepage)
        {
            StreamWriter fsWriteFile = new StreamWriter(strFilePath, true, System.Text.Encoding.GetEncoding(codepage));

            int intCounter = 0;
            foreach (string Str in m_lstTablesStrings)
            {
                fsWriteFile.Write(Str);

                if (intCounter < m_lstTablesStrings.Count - 1)
                {
                    fsWriteFile.Write(Environment.NewLine);
                    fsWriteFile.Write("------------------------------");
                    fsWriteFile.Write(Environment.NewLine);
                }


                intCounter += 1;
            }

            fsWriteFile.Close();
        }

        public void WriteToTxtFile(string strFilePath)
        {
            StreamWriter fsWriteFile = new StreamWriter(strFilePath, true);

            int intCounter = 0;
            foreach (string Str in m_lstTablesStrings)
            {
                fsWriteFile.Write(Str);

                if (intCounter < m_lstTablesStrings.Count - 1)
                {
                    fsWriteFile.Write(Environment.NewLine);
                    fsWriteFile.Write("------------------------------");
                    fsWriteFile.Write(Environment.NewLine);
                }


                intCounter += 1;
            }

            fsWriteFile.Close();
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
                    newDoc = null;
                    docsWord = null;
                    appWord = null;

                    if (m_Maindataset != null)
                        m_Maindataset.Dispose();

                    m_lstTablesStrings = null;
                    // xmlConverter = Nothing
                    m_strWordPath = null;
                    m_HasHeader = default(Boolean);
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

