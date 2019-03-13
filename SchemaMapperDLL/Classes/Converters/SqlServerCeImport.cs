using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlServerCe;
using System.Data.SqlClient;


namespace SchemaMapperDLL.Classes.Converters
{
    class SqlServerCeImport : BaseDbImport, IDisposable
    {
        
            #region constructors

          public SqlServerCeImport(string connectionstring)
             {

                ConnectionString = connectionstring;
                                
            }

            #endregion

            #region methods

        public override void getSchemaTable()
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter("Select * From Information_Schema.Tables", sqlcon))
                    {

                        da.Fill(SchemaTable);

                    }


                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public override DataTable GetDataTable(string schema, string tablename)
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
                    {

                        da.Fill(SQLTable);

                    }


                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SQLTable;
        }

        public DataTable GetQueryResult(SqlCeCommand sqlcmd)
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter(sqlcmd))
                    {

                        da.Fill(SQLTable);

                    }

                    SQLTable.PrimaryKey = null;

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SQLTable;
        }

        public override DataTable GetQueryResult(string sqlQuery)
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter(sqlQuery, sqlcon))
                    {

                        da.Fill(SQLTable);

                    }

                    SQLTable.PrimaryKey = null;

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SQLTable;
        }

        public override DataTable GetDataTableWithPaging(string schema, string tablename, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
                    {

                       r_result = da.Fill(v_PagingStartRecord, v_PagingInterval, SQLTable);

                    }

                    SQLTable.PrimaryKey = null;

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SQLTable;
        }

        public override DataTable GetDataTableWithPaging(string sqlQuery, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter(sqlQuery, sqlcon))
                    {

                        r_result = da.Fill(v_PagingStartRecord, v_PagingInterval, SQLTable);

                    }

                    SQLTable.PrimaryKey = null;

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SQLTable;
        }

        public DataTable GetDataTableWithPaging(SqlCeCommand sqlcmd, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {
                using (SqlCeConnection sqlcon = new SqlCeConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (SqlCeDataAdapter da = new SqlCeDataAdapter(sqlcmd))
                    {

                        r_result = da.Fill(v_PagingStartRecord, v_PagingInterval, SQLTable);

                    }

                    SQLTable.PrimaryKey = null;

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SQLTable;
        }

            #endregion

        public void Dispose()
        {
            
        }




    }
}
