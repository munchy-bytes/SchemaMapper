using System;
using System.Data.SqlClient;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SchemaMapper.SchemaMapping;

namespace SchemaMapper.Exporters
{
    public class SqlServerExport : BaseDbExport,IDisposable
    {

        #region create destination table

        public override string BuildCreateTableQuery(SchemaMapper.SchemaMapping.SchemaMapper schmapper)
        {

            string strQuery = "if not exists(select * from information_schema.tables where table_name = '" + schmapper.TableName +
                "' and table_schema = '" + schmapper.SchemaName + "')" +
                "create table [" + schmapper.SchemaName + "].[" + schmapper.TableName + "](";


            foreach (var Col in schmapper.Columns)
            {

                switch (Col.DataType)
                {
                    case SchemaMapper_Column.ColumnDataType.Boolean:
                        strQuery += "[" + Col.Name + "] BIT NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Date:
                        strQuery += "[" + Col.Name + "] DATETIME NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Text:
                        strQuery += "[" + Col.Name + "] [nvarchar](255) NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Number:
                        strQuery += "[" + Col.Name + "] BIGINT NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Memo:
                        strQuery += "[" + Col.Name + "] [nvarchar](4000) NULL ,";
                        break;
                }


            }

            strQuery = strQuery.TrimEnd(',');

            strQuery += ") ON [PRIMARY]";


            return strQuery;
        }

        public override int CreateDestinationTable(SchemaMapper.SchemaMapping.SchemaMapper schmapper,string connection)
        {

            string cmd = BuildCreateTableQuery(schmapper);
            int result = 0;
            try
            {
                using (SqlConnection sqlcon = new SqlConnection(connection))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCommand cmdCreateTable = new SqlCommand(cmd))
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

        #region Insert To Db Using Stored Procedure

        public string CreateTableTypeQuery(SchemaMapper.SchemaMapping.SchemaMapper schmapper)
        {
            string cmd = "CREATE TYPE [dbo].[MyTableType] AS TABLE(";

            foreach (var Col in schmapper.Columns)
            {

                //only supported data types are text (nvarchar(255)) and memo (nvarchar(4000))
                cmd += "[" + Col.Name + "] [" + (Col.DataType == SchemaMapper_Column.ColumnDataType.Text ? "nvarchar](255)" : "nvarchar](4000)") + " NULL ,";

            }

            cmd = cmd.TrimEnd(',');

            cmd += ")";


            return cmd;





        }
        public string CreateStoredProcedureQuery(SchemaMapper.SchemaMapping.SchemaMapper schmapper)
        {

            string cmd = "CREATE PROCEDURE [dbo].[InsertTable]" + Environment.NewLine +
                         " @myTableType MyTableType readonly" + Environment.NewLine +
                         " AS" + Environment.NewLine +
                         " BEGIN" + Environment.NewLine +
                         " insert into [" + schmapper.SchemaName + "].[" + schmapper.TableName + " ] select * from @myTableType" + Environment.NewLine +
                         " END";


            return cmd;

        }
        public void InsertToSQLUsingStoredProcedure(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring)
        {
            try
            {
                string cmdCreateTableType = CreateTableTypeQuery(schmapper);
                string cmdCreateStoredProcdure = CreateStoredProcedureQuery(schmapper);

                using (SqlConnection sqlcon = new SqlConnection(connectionstring))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();


                    //Create Table Type
                    using (SqlCommand sqlcmd = new SqlCommand(cmdCreateTableType, sqlcon))
                    {

                        sqlcmd.CommandTimeout = 300;
                        sqlcmd.ExecuteNonQuery();

                    }

                    //CreateDestinationTable stored procedure
                    using (SqlCommand sqlcmd = new SqlCommand(cmdCreateStoredProcdure, sqlcon))
                    {

                        sqlcmd.CommandTimeout = 300;
                        sqlcmd.ExecuteNonQuery();

                    }

                    //Execute Store procedure
                    using (SqlCommand sqlcmd = new SqlCommand("InsertTable"))
                    {
                        sqlcmd.Connection = sqlcon;
                        sqlcmd.CommandType = CommandType.StoredProcedure;
                        sqlcmd.Parameters.Add(new SqlParameter("@myTableType", dt));
                        sqlcmd.ExecuteNonQuery();
                    }

                    //Drop Table type and stored procedure if exists
                    DropObjects(sqlcon);


                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public void DropObjects(SqlConnection sqlcon)
        {
            try
            {

                //Drop stored procedure
                using (SqlCommand sqlcmd = new SqlCommand("DROP PROCEDURE [dbo].[InsertTable];", sqlcon))
                {

                    sqlcmd.CommandTimeout = 300;
                    sqlcmd.ExecuteNonQuery();

                }
                //Drop Table Type
                using (SqlCommand sqlcmd = new SqlCommand("DROP TYPE  [dbo].[MyTableType];", sqlcon))
                {

                    sqlcmd.CommandTimeout = 300;
                    sqlcmd.ExecuteNonQuery();

                }


            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Insert to Db using SQLBulk

        public void InsertUsingSQLBulk(SchemaMapper.SchemaMapping.SchemaMapper schmapper,DataTable dt, string connectionstring)
        {


            try
            {
                using (var bulkCopy = new SqlBulkCopy(connectionstring, SqlBulkCopyOptions.KeepIdentity))
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

        public override string BuildInsertStatement(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, int startindex, int rowscount)
        {

            string strQuery = "INSERT INTO [" + schmapper.SchemaName + "].[" + schmapper.TableName + "] (";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "[" + dc.ColumnName + "],";

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

        public override string BuildInsertStatementWithParameters(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt)
        {

            string strQuery = "INSERT INTO [" + schmapper.SchemaName + "].[" + schmapper.TableName + "] (";
            string strValues = "";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "[" + dc.ColumnName + "],";
                strValues += "@" + dc.ColumnName + ",";
            }

            strQuery = strQuery.TrimEnd(',') + ")  VALUES (" + strValues + ")" ;

            return strQuery;
        }

        public override void InsertIntoDb(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring, int rowsperbatch = 10000)
        {

            try
            {
                using (SqlConnection sqlcon = new SqlConnection(connectionstring))
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

                        using (SqlCommand sqlcmd = new SqlCommand(strQuery, sqlcon))
                        {

                            sqlcmd.ExecuteNonQuery();

                        }

                        currentindex = currentindex + rowsperbatch;
                    }

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public override void InsertIntoDbWithParameters(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring)
        {

            try
            {

                using (SqlConnection sqlcon = new SqlConnection(connectionstring))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    string strQuery = BuildInsertStatementWithParameters(schmapper, dt);

                    using (SqlTransaction trans = sqlcon.BeginTransaction())
                    {
                        using (SqlCommand sqlcmd = new SqlCommand(strQuery, sqlcon, trans))
                        {
                            sqlcmd.CommandType = CommandType.Text;


                            foreach (var Col in schmapper.Columns)
                            {

  
                                switch (Col.DataType)
                                {
                                    case SchemaMapper_Column.ColumnDataType.Date:
                                        sqlcmd.Parameters.Add("@" + Col.Name, SqlDbType.DateTime);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Text:
                                        sqlcmd.Parameters.Add("@" + Col.Name, SqlDbType.VarChar);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Memo:
                                        sqlcmd.Parameters.Add("@" + Col.Name, SqlDbType.VarChar,4000);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Number:
                                        sqlcmd.Parameters.Add("@" + Col.Name, SqlDbType.BigInt);
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
