using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;


namespace SchemaMapperDLL.Classes.Converters
{
    class OracleImport: BaseDbImport, IDisposable
    {

        #region constructors

        public OracleImport(string connectionstring)
        {

            ConnectionString = connectionstring;

        }

        #endregion

        #region methods

        public override void getSchemaTable()
        {
            try
            {
                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (OracleDataAdapter da = new OracleDataAdapter("Select * From Information_Schema.Tables", sqlcon))
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
                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (OracleDataAdapter da = new OracleDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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

        public DataTable GetQueryResult(OracleCommand oracmd)
        {
            try
            {
                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    oracmd.Connection = sqlcon;

                    using (OracleDataAdapter da = new OracleDataAdapter(oracmd))
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
                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (OracleDataAdapter da = new OracleDataAdapter(sqlQuery, sqlcon))
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
                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (OracleDataAdapter da = new OracleDataAdapter("Select * From [" + schema + "].[" + tablename + "]", sqlcon))
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
                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (OracleDataAdapter da = new OracleDataAdapter(sqlQuery, sqlcon))
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

        public DataTable GetDataTableWithPaging(OracleCommand sqlcmd, int v_PagingStartRecord, int v_PagingInterval, out int r_result)
        {
            try
            {

                using (OracleConnection sqlcon = new OracleConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    sqlcmd.Connection = sqlcon;

                    using (OracleDataAdapter da = new OracleDataAdapter(sqlcmd))
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
