using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SchemaMapperDLL.Classes.SchemaMapping;
using System.Data;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;

namespace SchemaMapperDLL.Classes.Exporters
{
    public class OracleExport : BaseDbExport, IDisposable
    {
        #region create destination table
        
        public override string BuildCreateTableQuery(SchemaMapper schmapper)
        {
            string strQuery = "create table \"" + schmapper.SchemaName + "\".\"" + schmapper.TableName + "\"(";


            foreach (var Col in schmapper.Columns)
            {

                switch (Col.DataType)
                {

                    case SchemaMapper_Column.ColumnDataType.Date:
                        strQuery += "\"" + Col.Name + "\" DATETIME NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Text:
                        strQuery += "\"" + Col.Name + "\" VARCHAR2(255) NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Number:
                        strQuery += "\"" + Col.Name + "\" BIGINT NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Memo:
                        strQuery += "\"" + Col.Name + "\" VARCHAR2(4000) NULL ,";
                        break;
                }


            }

            strQuery = strQuery.TrimEnd(',');

            strQuery += ")";


            return strQuery;
        }

        public override int CreateDestinationTable(SchemaMapper schmapper,string connection)
        {
            string cmd = BuildCreateTableQuery(schmapper);
            int result = 0;
            try
            {
                using (OracleConnection sqlcon = new OracleConnection(connection))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (OracleCommand cmdCreateTable = new OracleCommand(cmd))
                    {

                        cmdCreateTable.CommandTimeout = 0;
                        cmdCreateTable.Connection = sqlcon;
                        result = cmdCreateTable.ExecuteNonQuery();

                    }


                }

            }
            catch (Exception ex)
            {
                throw ex;

            }

            return result;
        }
        
        #endregion

        #region Insert to Db using OracleBulk

        public void InsertUsingOracleBulk(SchemaMapper schmapper, DataTable dt, string connectionstring)
        {


            try
            {
                using (var bulkCopy = new OracleBulkCopy(connectionstring, OracleBulkCopyOptions.Default))
                {

                    foreach (DataColumn col in dt.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    bulkCopy.BulkCopyTimeout = 600;
                    bulkCopy.DestinationTableName = "[" + schmapper.SchemaName + "].[" + schmapper.TableName + "]";
                    bulkCopy.WriteToServer(dt);
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Insert using SQL statement

        public override string BuildInsertStatement(SchemaMapper schmapper, DataTable dt, int startindex, int rowscount)
        {

            string strQuery = "INSERT INTO \"" + schmapper.SchemaName + "\".\"" + schmapper.TableName + "\" (";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "\"" + dc.ColumnName + "\",";

            }

            strQuery = strQuery.TrimEnd(',') + ")  VALUES ";

            int i = startindex;
            int lastrowindex = startindex + rowscount;

            for (i = startindex; i <= lastrowindex; i++)
            {
                strQuery = strQuery + "(";
                foreach (var Col in schmapper.Columns)
                {

                    switch (Col.DataType)
                    {

                        case SchemaMapper_Column.ColumnDataType.Date:
                            strQuery += "'" + ((DateTime)dt.Rows[i][Col.Name]).ToString("yyyy-MM-dd HH:mm:ss") + "',";
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            strQuery += "'" + dt.Rows[i][Col.Name].ToString() + "',";
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            strQuery += dt.Rows[i][Col.Name].ToString() + ",";
                            break;

                    }



                }

                strQuery = strQuery.TrimEnd(',') + "),";
            }

            strQuery = strQuery.TrimEnd(',');
            return strQuery;
        }

        public override void InsertIntoDb(SchemaMapper schmapper, DataTable dt, string connectionstring, int rowsperbatch = 10000)
        {

            try
            {
                using (OracleConnection sqlcon = new OracleConnection(connectionstring))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();


                    int totalcount = dt.Rows.Count;
                    int currentindex = 0;

                    while (currentindex < totalcount)
                    {

                        string strQuery = "";

                        if ((currentindex + rowsperbatch) >= totalcount)
                            rowsperbatch = totalcount - currentindex - 1;

                        strQuery = BuildInsertStatement(schmapper, dt, currentindex, rowsperbatch);

                        using (OracleCommand sqlcmd = new OracleCommand(strQuery, sqlcon))
                        {

                            sqlcmd.ExecuteNonQuery();
                            currentindex = currentindex + rowsperbatch;

                        }


                    }

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public override string BuildInsertStatementWithParameters(SchemaMapper schmapper, DataTable dt)
        {

            string strQuery = "INSERT INTO \"" + schmapper.SchemaName + "\".\"" + schmapper.TableName + "\" (";
            string strValues = "";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "\"" + dc.ColumnName + "\",";
                strValues += "@" + dc.ColumnName + ",";
            }

            strQuery = strQuery.TrimEnd(',') + ")  VALUES (" + strValues + ")";

            return strQuery;
        }

        public override void InsertIntoDbWithParameters(SchemaMapper schmapper, DataTable dt, string connectionstring)
        {

            try
            {

                using (OracleConnection sqlcon = new OracleConnection(connectionstring))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    string strQuery = BuildInsertStatementWithParameters(schmapper, dt);

                    using (OracleTransaction trans = sqlcon.BeginTransaction())
                    {
                        using (OracleCommand sqlcmd = new OracleCommand(strQuery, sqlcon))
                        {
                            sqlcmd.CommandType = CommandType.Text;


                            foreach (var Col in schmapper.Columns)
                            {


                                switch (Col.DataType)
                                {
                                    case SchemaMapper_Column.ColumnDataType.Date:
                                        sqlcmd.Parameters.Add("@" + Col.Name, OracleDbType.Date);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Text:
                                        sqlcmd.Parameters.Add("@" + Col.Name, OracleDbType.Varchar2);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Memo:
                                        sqlcmd.Parameters.Add("@" + Col.Name, OracleDbType.Varchar2, 4000);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Number:
                                        sqlcmd.Parameters.Add("@" + Col.Name, OracleDbType.Int64);
                                        break;

                                }

                            }


                            foreach (DataRow drrow in dt.Rows)
                            {

                                foreach (var Col in schmapper.Columns)
                                {

                                    sqlcmd.Parameters["@" + Col.Name].Value = drrow[Col.Name];

                                }

                                sqlcmd.ExecuteNonQuery();

                            }



                            trans.Commit();
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

          public void Dispose()
        {

        }
    }
}
