using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace SchemaMapperDLL.Classes.Converters
{
    class MySQLImport : BaseDbImport, IDisposable
    {
        #region constructors

        public MySQLImport(string connectionstring)
        {

            ConnectionString = connectionstring;

        }

        #endregion

        #region methods

        public override void getSchemaTable()
        {
            try
            {
                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (MySqlDataAdapter da = new MySqlDataAdapter("Select * From Information_Schema.Tables", sqlcon))
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
                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (MySqlDataAdapter da = new MySqlDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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

        public DataTable GetQueryResult(MySqlCommand sqlcmd)
        {
            try
            {
                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (MySqlDataAdapter da = new MySqlDataAdapter(sqlcmd))
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
                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (MySqlDataAdapter da = new MySqlDataAdapter(sqlQuery, sqlcon))
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
                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (MySqlDataAdapter da = new MySqlDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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
                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (MySqlDataAdapter da = new MySqlDataAdapter(sqlQuery, sqlcon))
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

        public DataTable GetDataTableWithPaging(MySqlCommand sqlcmd, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {

                using (MySqlConnection sqlcon = new MySqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (MySqlDataAdapter da = new MySqlDataAdapter(sqlcmd))
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
