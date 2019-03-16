using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapperDLL.Classes.Converters
{
    class SqlServerImport : BaseDbImport, IDisposable
    {
        
        #region constructors

            public SqlServerImport(string connectionstring){

                ConnectionString = connectionstring;
                                
            }

            #endregion
        
        #region methods
        
        public override void getSchemaTable()
        {
            try
            {
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    SchemaTable = sqlcon.GetSchema("Tables");


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
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlDataAdapter da = new SqlDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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

        public  DataTable GetQueryResult(SqlCommand sqlcmd)
        {
            try
            {
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (SqlDataAdapter da = new SqlDataAdapter(sqlcmd))
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
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlDataAdapter da = new SqlDataAdapter(sqlQuery, sqlcon))
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
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlDataAdapter da = new SqlDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlDataAdapter da = new SqlDataAdapter(sqlQuery, sqlcon))
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

        public DataTable GetDataTableWithPaging(SqlCommand sqlcmd, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {

                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (SqlDataAdapter da = new SqlDataAdapter(sqlcmd))
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
