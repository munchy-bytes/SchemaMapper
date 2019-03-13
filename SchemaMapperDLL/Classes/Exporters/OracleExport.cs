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
        
        public override string BuildCreateTableQuery(SchemaMapper schmapper)
        {
            string strQuery = "if not exists(select * from information_schema.tables where table_name = '" + schmapper.TableName +
    "' and table_schema = '" + schmapper.SchemaName + "')" +
    "create table [" + schmapper.SchemaName + "].[" + schmapper.TableName + "](";


            foreach (var Col in schmapper.Columns)
            {

                switch (Col.DataType)
                {

                    case SchemaMapper_Column.ColumnDataType.Date:
                        strQuery += "[" + Col.Name + "] DATETIME NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Text:
                        strQuery += "[" + Col.Name + "] VARCHAR2(255) NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Number:
                        strQuery += "[" + Col.Name + "] BIGINT NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Memo:
                        strQuery += "[" + Col.Name + "] VARCHAR2(4000) NULL ,";
                        break;
                }


            }

            strQuery = strQuery.TrimEnd(',');

            strQuery += ") ON [PRIMARY]";


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

        public void Dispose()
        {

        }
    }
}
