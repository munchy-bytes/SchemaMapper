using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using Microsoft.VisualBasic;

namespace SchemaMapperDLL.Classes.Converters
{
    public class MsExcelImport : BaseImportusingOLEDB, IDisposable
    {

        #region Declarations

        private char[] m_chrSymbols = { '$', '.', '-', '+', '~', '*', '/', '(', ')', '@', '!', '?', '#', '%', '^', '&', ',', ':', ';', '\'', '\"' };
        private List<string> m_SheetsSpecifications = new List<string>();
        
        #endregion

        #region override methods

        public override DataSet FillAllTables(string Tablename) {

            if (SchemaTable == null) { getSchemaTable(); }

            string strSheetname = string.Empty;
            BuildConnectionString();

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                foreach (DataRow schRow in SchemaTable.Rows)
                {
                    strSheetname = schRow["TABLE_NAME"].ToString().Trim('\'');

                    if (!strSheetname.EndsWith("_") && strSheetname.EndsWith("$"))
                    {

                        // Filter
                        try
                        {

                            string strcommand = string.Empty;

                            if (m_SheetsSpecifications.Where(x => x.StartsWith(strSheetname)).Count() > 0)
                                strcommand = "SELECT * , \"" + strSheetname + "\" AS [Sys_Sheetname] FROM [" + m_SheetsSpecifications.Where(x => x.StartsWith(strSheetname)).FirstOrDefault() + "]";
                            else
                                strcommand = "SELECT * , \"" + strSheetname + "\" AS [Sys_Sheetname] FROM [" + strSheetname + "]";

                            using (OleDbCommand cmd = new OleDbCommand(strcommand, OleDBCon))
                            {
                                DataTable dtTable = new DataTable(RemoveUnusedChars(strSheetname));


                                cmd.CommandType = CommandType.Text;

                                using (OleDbDataAdapter daGetDataFromSheet = new OleDbDataAdapter(cmd))
                                {
                                    try
                                    {
                                        daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                                        foreach (DataColumn dCol in dtTable.Columns)
                                        {
                                            if (dCol.DataType != typeof(System.String))
                                                dCol.DataType = typeof(System.String);
                                        }

                                        daGetDataFromSheet.Fill(dtTable);

                                    }
                                    catch (Exception ex)
                                    {
                                        if (ex.Message == "Too many fields defined.")
                                        {
                                            try
                                            {
                                                cmd.CommandText = "SELECT * FROM [" + strSheetname + "A1:IU65536]";

                                                daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                                                foreach (DataColumn dCol in dtTable.Columns)
                                                {
                                                    if (dCol.DataType != typeof(System.String))
                                                        dCol.DataType = typeof(System.String);
                                                }

                                                daGetDataFromSheet.Fill(dtTable);

                                                if (dtTable.Rows.Count <= RowsToSkip)
                                                    continue;
                                            }
                                            catch (Exception exc)
                                            {
                                                continue;
                                            }
                                        }
                                        else
                                            // Deleted Sheet
                                            continue;
                                    }
                                    //Read header
                                    if (dtTable.Rows.Count > 0)
                                    {
                                        int intCount = 0;
                                        // Dim intSheetnameidx As Integer = dtTable.Columns.IndexOf("Sys_Sheetname")

                                        // If intSheetnameidx < 0 Then intSheetnameidx = 0

                                        foreach (DataColumn dCol in dtTable.Columns)
                                        {
                                            if (!dCol.ColumnName.StartsWith("Sys_"))
                                            {
                                                string strColumnname = "";

                                                // If intCount <= intSheetnameidx Then

                                                strColumnname = dtTable.Rows[0][intCount].ToString() == "" ? "F" + (intCount) : GetColumnName(dtTable.Rows[0][intCount].ToString().Trim(), dtTable);

                                                // Else

                                                // strColumnname = If(dtTable.Rows(0).Item(intCount).ToString = "", _
                                                // "F" & (intCount + 2), _
                                                // GetColumnName(dtTable.Rows(0).Item(intCount).ToString.Trim, dtTable))

                                                // End If


                                                if (dtTable.Columns.IndexOf(strColumnname) > -1 && dtTable.Rows[0][intCount].ToString() != "")
                                                    dCol.ColumnName = strColumnname + "_" + intCount.ToString();
                                                else
                                                    dCol.ColumnName = strColumnname;
                                            }

                                            intCount += 1;
                                        }

                                        dtTable.Rows[0].Delete();
                                        dtTable.AcceptChanges();
                                    }

                                    foreach (DataColumn dCol in dtTable.Columns)
                                    {
                                        if (dCol.ColumnName.Contains("'"))
                                            dCol.ColumnName = dCol.ColumnName.Replace("'", "");
                                    }
                                }


                                if (Maindataset.Tables.Contains(dtTable.TableName))
                                {
                                    while (Maindataset.Tables.Contains(dtTable.TableName))
                                        dtTable.TableName = dtTable.TableName + "_";
                                }

                                if (dtTable.Rows.Count > 0)
                                    Maindataset.Tables.Add(dtTable);
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception(ex.Message + Constants.vbNewLine + string.Format("Sheet:{0}.File:F{1}", strSheetname, FilePath));
                        }
                    }
                }
            }

            return Maindataset;
        }
        public  DataTable GetTableByName(string Tablename,bool v_HasHeader, int v_RowsToSkip) {

            if (SchemaTable == null) { getSchemaTable(); }

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                //schRow["TABLE_NAME"].ToString().Trim('\'');

                      try
                        {

                            string strcommand = string.Empty;

                            if (m_SheetsSpecifications.Where(x => x.StartsWith(Tablename)).Count() > 0)
                                strcommand = "SELECT * , \"" + Tablename + "\" AS [Sys_Sheetname] FROM [" + m_SheetsSpecifications.Where(x => x.StartsWith(Tablename)).FirstOrDefault() + "]";
                            else
                                strcommand = "SELECT * , \"" + Tablename + "\" AS [Sys_Sheetname] FROM [" + Tablename + "]";

                            using (OleDbCommand cmd = new OleDbCommand(strcommand, OleDBCon))
                            {
                                DataTable dtTable = new DataTable(RemoveUnusedChars(Tablename));


                                cmd.CommandType = CommandType.Text;

                                using (OleDbDataAdapter daGetDataFromSheet = new OleDbDataAdapter(cmd))
                                {
                                    try
                                    {
                                        daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                                        foreach (DataColumn dCol in dtTable.Columns)
                                        {
                                            if (dCol.DataType != typeof(System.String))
                                                dCol.DataType = typeof(System.String);
                                        }

                                        daGetDataFromSheet.Fill(dtTable);

                                       if (dtTable.Rows.Count <= v_RowsToSkip)
                                        return null;
                                   }
                                    catch (Exception ex)
                                    {
                                        if (ex.Message == "Too many fields defined.")
                                        {
                                            try
                                            {
                                                cmd.CommandText = "SELECT * FROM [" + Tablename + "A1:IU65536]";

                                                daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                                                foreach (DataColumn dCol in dtTable.Columns)
                                                {
                                                    if (dCol.DataType != typeof(System.String))
                                                        dCol.DataType = typeof(System.String);
                                                }

                                                daGetDataFromSheet.Fill(dtTable);

                                                if (dtTable.Rows.Count <= RowsToSkip)
                                                    return null;
                                            }
                                            catch (Exception exc)
                                            {
                                        return null;
                                            }
                                        }
                                        else
                                    // Deleted Sheet
                                    return null;
                                    }

                                    if (RowsToSkip > 0)
                                    {
                                        List<DataRow> lstRows = dtTable.Select().Take(RowsToSkip).ToList();

                                        string strExtraFieldsString = string.Empty;

                                        foreach (DataRow drRow in lstRows)
                                        {
                                            if (string.IsNullOrEmpty(strExtraFieldsString))
                                                strExtraFieldsString += string.Join(Constants.vbNewLine, drRow.ItemArray.Where(x => x.ToString() != Tablename).ToArray());
                                            else
                                                strExtraFieldsString += Constants.vbNewLine + string.Join(Constants.vbNewLine, drRow.ItemArray.Where(x => x.ToString() != Tablename).ToArray());
                                        }

                                        if (!string.IsNullOrEmpty(strExtraFieldsString))
                                        {
                                            DataColumn dcCol = new DataColumn("Sys_ExtraFields");
                                            dcCol.MaxLength = 4000;
                                            dcCol.DefaultValue = strExtraFieldsString;
                                            dtTable.Columns.Add(dcCol);
                                        }


                                        dtTable = dtTable.Select().Skip(RowsToSkip).CopyToDataTable();
                                    }

                                    if (v_HasHeader && dtTable.Rows.Count > 0)
                                    {
                                        int intCount = 0;
                                        // Dim intSheetnameidx As Integer = dtTable.Columns.IndexOf("Sys_Sheetname")

                                        // If intSheetnameidx < 0 Then intSheetnameidx = 0

                                        foreach (DataColumn dCol in dtTable.Columns)
                                        {
                                            if (!dCol.ColumnName.StartsWith("Sys_"))
                                            {
                                                string strColumnname = "";

                                                // If intCount <= intSheetnameidx Then

                                                strColumnname = dtTable.Rows[0][intCount].ToString() == "" ? "F" + (intCount) : GetColumnName(dtTable.Rows[0][intCount].ToString().Trim(), dtTable);

                                                // Else

                                                // strColumnname = If(dtTable.Rows(0).Item(intCount).ToString = "", _
                                                // "F" & (intCount + 2), _
                                                // GetColumnName(dtTable.Rows(0).Item(intCount).ToString.Trim, dtTable))

                                                // End If


                                                if (dtTable.Columns.IndexOf(strColumnname) > -1 && dtTable.Rows[0][intCount].ToString() != "")
                                                    dCol.ColumnName = strColumnname + "_" + intCount.ToString();
                                                else
                                                    dCol.ColumnName = strColumnname;
                                            }

                                            intCount += 1;
                                        }

                                        dtTable.Rows[0].Delete();
                                        dtTable.AcceptChanges();
                                    }

                                    foreach (DataColumn dCol in dtTable.Columns)
                                    {
                                        if (dCol.ColumnName.Contains("'"))
                                            dCol.ColumnName = dCol.ColumnName.Replace("'", "");
                                    }
                                }


                        return dtTable;
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception(ex.Message + Constants.vbNewLine + string.Format("Sheet:{0}.File:F{1}",Tablename, FilePath));
                        }
                    }
                
            }
        public  DataTable GetTableByNamewithPaging(string Tablename, int v_PagingStartRecord, int v_PagingInterval, out int r_result, bool v_HasHeader = true, int v_RowsToSkip = 0) {

            if (SchemaTable == null) { getSchemaTable(); }

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                //schRow["TABLE_NAME"].ToString().Trim('\'');

                try
                {

                    string strcommand = string.Empty;

                    if (m_SheetsSpecifications.Where(x => x.StartsWith(Tablename)).Count() > 0)
                        strcommand = "SELECT * , \"" + Tablename + "\" AS [Sys_Sheetname] FROM [" + m_SheetsSpecifications.Where(x => x.StartsWith(Tablename)).FirstOrDefault() + "]";
                    else
                        strcommand = "SELECT * , \"" + Tablename + "\" AS [Sys_Sheetname] FROM [" + Tablename + "]";

                    using (OleDbCommand cmd = new OleDbCommand(strcommand, OleDBCon))
                    {
                        DataTable dtTable = new DataTable(RemoveUnusedChars(Tablename));


                        cmd.CommandType = CommandType.Text;

                        using (OleDbDataAdapter daGetDataFromSheet = new OleDbDataAdapter(cmd))
                        {
                            try
                            {
                                daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                                foreach (DataColumn dCol in dtTable.Columns)
                                {
                                    if (dCol.DataType != typeof(System.String))
                                        dCol.DataType = typeof(System.String);
                                }

                                r_result = daGetDataFromSheet.Fill( v_PagingStartRecord,v_PagingInterval, dtTable);

                                if (dtTable.Rows.Count <= v_RowsToSkip)
                                 return null; 
                            }
                            catch (Exception ex)
                            {
                                if (ex.Message == "Too many fields defined.")
                                {
                                    try
                                    {
                                        cmd.CommandText = "SELECT * FROM [" + Tablename + "A1:IU65536]";

                                        daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                                        foreach (DataColumn dCol in dtTable.Columns)
                                        {
                                            if (dCol.DataType != typeof(System.String))
                                                dCol.DataType = typeof(System.String);
                                        }

                                        r_result = daGetDataFromSheet.Fill(v_PagingStartRecord, v_PagingInterval, dtTable); ;

                                        if (dtTable.Rows.Count <= RowsToSkip)
                                        return null; 
                                    }
                                    catch (Exception exc)
                                    {
                                        { r_result = 0; return null; }
                                    }
                                }
                                else
                                // Deleted Sheet
                                { r_result = 0; return null; }
                            }

                            if (RowsToSkip > 0)
                            {
                                List<DataRow> lstRows = dtTable.Select().Take(RowsToSkip).ToList();

                                string strExtraFieldsString = string.Empty;

                                foreach (DataRow drRow in lstRows)
                                {
                                    if (string.IsNullOrEmpty(strExtraFieldsString))
                                        strExtraFieldsString += string.Join(Constants.vbNewLine, drRow.ItemArray.Where(x => x.ToString() != Tablename).ToArray());
                                    else
                                        strExtraFieldsString += Constants.vbNewLine + string.Join(Constants.vbNewLine, drRow.ItemArray.Where(x => x.ToString() != Tablename).ToArray());
                                }

                                if (!string.IsNullOrEmpty(strExtraFieldsString))
                                {
                                    DataColumn dcCol = new DataColumn("Sys_ExtraFields");
                                    dcCol.MaxLength = 4000;
                                    dcCol.DefaultValue = strExtraFieldsString;
                                    dtTable.Columns.Add(dcCol);
                                }


                                dtTable = dtTable.Select().Skip(RowsToSkip).CopyToDataTable();
                            }
                            //Get Header if it is starting from zero
                            if (v_HasHeader && dtTable.Rows.Count >0 )
                            {
                                if (v_PagingStartRecord == 0){
                                int intCount = 0;
                                // Dim intSheetnameidx As Integer = dtTable.Columns.IndexOf("Sys_Sheetname")

                                // If intSheetnameidx < 0 Then intSheetnameidx = 0

                                foreach (DataColumn dCol in dtTable.Columns)
                                {
                                    if (!dCol.ColumnName.StartsWith("Sys_"))
                                    {
                                        string strColumnname = "";

                                        // If intCount <= intSheetnameidx Then

                                        strColumnname = dtTable.Rows[0][intCount].ToString() == "" ? "F" + (intCount) : GetColumnName(dtTable.Rows[0][intCount].ToString().Trim(), dtTable);

                                        // Else

                                        // strColumnname = If(dtTable.Rows(0).Item(intCount).ToString = "", _
                                        // "F" & (intCount + 2), _
                                        // GetColumnName(dtTable.Rows(0).Item(intCount).ToString.Trim, dtTable))

                                        // End If


                                        if (dtTable.Columns.IndexOf(strColumnname) > -1 && dtTable.Rows[0][intCount].ToString() != "")
                                            dCol.ColumnName = strColumnname + "_" + intCount.ToString();
                                        else
                                            dCol.ColumnName = strColumnname;
                                    }

                                    intCount += 1;

                                    
                                }


                                dtTable.Rows[0].Delete();
                                dtTable.AcceptChanges();

                                SchemaTable.AsEnumerable().Where(x => x["TABLE_NAME"].ToString() == Tablename).First()["Header"] = String.Join(";", dtTable.Columns.Cast<DataColumn>().Select(x => x.ColumnName).ToArray<String>());

                            }else if(SchemaTable.AsEnumerable().Where(x => x["TABLE_NAME"].ToString() == Tablename).First()["Header"].ToString() != ""){

                                string strHeader = SchemaTable.AsEnumerable().Where(x => x["TABLE_NAME"].ToString() == Tablename).First()["Header"].ToString();


                                int idx = 0;
                                foreach (string strColumn in strHeader.Split(';'))
                                {

                                    dtTable.Columns[idx].ColumnName = strColumn;

                                    idx++;
                                }
                            
                            }
                            }
                            foreach (DataColumn dCol in dtTable.Columns)
                            {
                                if (dCol.ColumnName.Contains("'"))
                                    dCol.ColumnName = dCol.ColumnName.Replace("'", "");
                            }
                        }


                        return dtTable; 
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message + Constants.vbNewLine + string.Format("Sheet:{0}.File:F{1}", Tablename, FilePath));
                }
            }

        }
        public override void getSchemaTable() {

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                SchemaTable = OleDBCon.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                
                OleDBCon.Close();
            }

            DataColumn dcol = new DataColumn("Header",System.Type.GetType("System.String"));
            dcol.MaxLength = 4000;

            SchemaTable.Columns.Add(dcol);
        }
        public override void BuildConnectionString()
        {
            ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + FilePath + ";Excel 12.0;HDR=NO;IMEX=1";
            
        }

        #endregion

        #region constructors

        public MsExcelImport() : base() { }

        public MsExcelImport(string v_strFilePath, string v_RangeSpecifications = "") : base()
        {
            
            m_SheetsSpecifications.AddRange(v_RangeSpecifications.Split('|'));
            FilePath = v_strFilePath;
        }


        #endregion

        #region methods

        public string GetColumnName(string v_strname, DataTable v_table)
        {
            string strResult = string.Empty;


            if (v_table.Columns.Contains(v_strname))
            {
                int intCount = 1;

                while (v_table.Columns.Contains(v_strname + "_" + intCount.ToString()))

                    intCount += 1;

                strResult = v_strname + "_" + intCount.ToString();
            }
            else
                strResult = v_strname;


            return strResult;
        }

        public void ExportDatasetToXml(string xmlPath)
        {
            Maindataset.WriteXml(xmlPath);
        }

        public string RemoveUnusedChars(string strOriginal)
        {
            string strResult = string.Empty;
            strResult = strOriginal;
            foreach (char Chr in m_chrSymbols)
                strResult = strResult.Replace(Chr,'\0');

            return strResult;
        }

        public List<string> GetSheets()
        {
            List<string> lstResult = new List<string>();
             BuildConnectionString();

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                this.getSchemaTable();




                foreach (DataRow drRow in SchemaTable.Rows)
                {
                    if (!drRow["TABLE_NAME"].ToString().EndsWith("_") && drRow["TABLE_NAME"].ToString().Trim('\'').EndsWith("$"))
                        lstResult.Add(drRow["TABLE_NAME"].ToString());
                }
            }

            return lstResult;
        }

        #endregion


        public void Dispose()
        {
     
            SchemaTable.Dispose();

        }

    }
}
