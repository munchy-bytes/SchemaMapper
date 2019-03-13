using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data.SQLite.Generic;

namespace SchemaMapperDLL.Classes.Converters
{
    class SQLiteImport : BaseDbImport, IDisposable
    {

        #region constructors

        public SQLiteImport(string connectionstring)
        {

            ConnectionString = connectionstring;

        }

        #endregion

        #region methods

        public override void getSchemaTable()
        {
            try
            {
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter("Select * From sqlite_master", sqlcon))
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
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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

        public DataTable GetQueryResult(SQLiteCommand sqlcmd)
        {
            try
            {
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter(sqlcmd))
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
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter(sqlQuery, sqlcon))
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
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter(sqlQuery, sqlcon))
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

        public DataTable GetDataTableWithPaging(SQLiteCommand sqlcmd, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {
                using (SQLiteConnection sqlcon = new SQLiteConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (SQLiteDataAdapter da = new SQLiteDataAdapter(sqlcmd))
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
