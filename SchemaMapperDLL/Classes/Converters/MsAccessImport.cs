using System;
using System.Data;
using System.Data.OleDb;

namespace SchemaMapperDLL.Classes.Converters
{
    public class MsAccessImport: BaseImportusingOLEDB,IDisposable
    {

        #region constructors

        public MsAccessImport(string accesspath)
        {
            FilePath = accesspath;
        }

        #endregion

        #region override methods

        public override DataSet FillAllTables(string Tablename)
        {

            if (SchemaTable == null) { getSchemaTable(); }

            string strTablename = string.Empty;
            BuildConnectionString();

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                foreach (DataRow schRow in SchemaTable.Rows)
                {
                    strTablename = schRow["TABLE_NAME"].ToString().Trim('\'');

                    try
                    {

                        string strcommand = string.Empty;

                        DataTable dtTable = new DataTable(strTablename);

                        strcommand = "SELECT * FROM [" + strTablename + "]";

                        using (OleDbCommand cmd = new OleDbCommand(strcommand, OleDBCon))
                        {
                            cmd.CommandType = CommandType.Text;

                            using (OleDbDataAdapter daGetDataFromSheet = new OleDbDataAdapter(cmd))
                            {
                                    daGetDataFromSheet.Fill(dtTable);
                            }
                            dtTable.PrimaryKey = null;
                            Maindataset.Tables.Add(dtTable);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(ex.Message + Environment.NewLine + String.Format("Table: {0}", strTablename), ex);
                    }


                }

            }
            return Maindataset;
        }
        public DataTable GetTableByName(string Tablename)
        {

            if (SchemaTable == null) { getSchemaTable(); }

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                //schRow["TABLE_NAME"].ToString().Trim('\'');

                try
                {

                    string strcommand = string.Empty;

                    DataTable dtTable = new DataTable(Tablename);

                    strcommand = "SELECT * FROM [" + Tablename + "]";

                    using (OleDbCommand cmd = new OleDbCommand(strcommand, OleDBCon))
                    {
                        cmd.CommandType = CommandType.Text;

                        using (OleDbDataAdapter daGetDataFromSheet = new OleDbDataAdapter(cmd))
                        {

                            daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                            foreach (DataColumn dCol in dtTable.Columns)
                            {
                                if (dCol.DataType != typeof(System.String))
                                    dCol.DataType = typeof(System.String);
                            }

                            daGetDataFromSheet.Fill(dtTable);
                        }
                        dtTable.PrimaryKey = null;
                        return dtTable;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }


        }

        public DataTable GetTableByNamewithPaging(string Tablename, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {

            if (SchemaTable == null) { getSchemaTable(); }

            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                //schRow["TABLE_NAME"].ToString().Trim('\'');

                try
                {

                    string strcommand = string.Empty;

                    DataTable dtTable = new DataTable(Tablename);

                    strcommand = "SELECT * FROM [" + Tablename + "]";

                    using (OleDbCommand cmd = new OleDbCommand(strcommand, OleDBCon))
                    {
                        cmd.CommandType = CommandType.Text;

                        using (OleDbDataAdapter daGetDataFromSheet = new OleDbDataAdapter(cmd))
                        {

                            daGetDataFromSheet.FillSchema(dtTable, SchemaType.Source);

                            foreach (DataColumn dCol in dtTable.Columns)
                            {
                                if (dCol.DataType != typeof(System.String))
                                    dCol.DataType = typeof(System.String);
                                                               
                            }

                            r_result = daGetDataFromSheet.Fill(v_PagingStartRecord, v_PagingInterval, dtTable);
                        }

                        dtTable.PrimaryKey = null;
                        return dtTable;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }



        }
        public override void getSchemaTable()
        {
            using (OleDbConnection OleDBCon = new OleDbConnection(ConnectionString))
            {
                if (OleDBCon.State != ConnectionState.Open)
                    OleDBCon.Open();

                SchemaTable = OleDBCon.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });

                OleDBCon.Close();
            }

        }
        public override void BuildConnectionString()
        {
            ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + FilePath;
        }


        #endregion

        public void Dispose()
        {
            if (SchemaTable != null)
                SchemaTable.Dispose();

            if (Maindataset != null)
              Maindataset.Dispose();
        
        }

    }
 
}