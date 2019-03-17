using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using SchemaMapper.SchemaMapping;

namespace SchemaMapper.Exporters
{
    public class MySQLExport: BaseDbExport, IDisposable
    {
        public void Dispose()
        {
            
        }

        #region create destination table

        public override string BuildCreateTableQuery(SchemaMapper.SchemaMapping.SchemaMapper schmapper)
        {

            string strQuery = "create table if not exists `" + schmapper.SchemaName + "`.`" + schmapper.TableName + "`(";


            foreach (var Col in schmapper.Columns)
            {

                switch (Col.DataType)
                {
                    case SchemaMapper_Column.ColumnDataType.Boolean:
                        strQuery += "\"" + Col.Name + "\" tinyint(1) NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Date:
                        strQuery += "`" + Col.Name + "` datetime NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Text:
                        strQuery += "`" + Col.Name + "` varchar(255) NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Number:
                        strQuery += "`" + Col.Name + "` bigint NULL ,";
                        break;
                    case SchemaMapper_Column.ColumnDataType.Memo:
                        strQuery += "`" + Col.Name + "` varchar(4000) NULL ,";
                        break;
                }


            }

            strQuery = strQuery.TrimEnd(',');

            strQuery += ")";


            return strQuery;
        }

        public override int CreateDestinationTable(SchemaMapper.SchemaMapping.SchemaMapper schmapper, string connection)
        {

            string cmd = BuildCreateTableQuery(schmapper);
            int result = 0;
            try
            {
                using (MySqlConnection sqlcon = new MySqlConnection(connection))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (MySqlCommand cmdCreateTable = new MySqlCommand(cmd))
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

        #region Insert using SQL statement

        public override string BuildInsertStatement(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, int startindex, int rowscount)
        {

            string strQuery = "INSERT INTO `" + schmapper.SchemaName + "`.`" + schmapper.TableName + "` (";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "`" + dc.ColumnName + "`,";
                
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
                            strQuery += "'" + MySqlHelper.EscapeString(((DateTime)dt.Rows[i][Col.Name]).ToString("yyyy-MM-dd HH:mm:ss")) + "',";
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            strQuery += "'" + MySqlHelper.EscapeString(dt.Rows[i][Col.Name].ToString()) + "',";
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            strQuery += MySqlHelper.EscapeString(dt.Rows[i][Col.Name].ToString()) + ",";
                            break;

                    }

                         

                }

                strQuery = strQuery.TrimEnd(',') + "),";  
            }

                strQuery = strQuery.TrimEnd(',');
                return strQuery;
        }

        public override void InsertIntoDb(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring, int rowsperbatch = 10000)
        {

            try
            {
                using (MySqlConnection sqlcon = new MySqlConnection(connectionstring))
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

                        using (MySqlCommand sqlcmd = new MySqlCommand(strQuery, sqlcon))
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

        public override string BuildInsertStatementWithParameters(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt)
        {

            string strQuery = "INSERT INTO `" + schmapper.SchemaName + "`.`" + schmapper.TableName + "` (";
            string strValues = "";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "`" + dc.ColumnName + "`,";
                strValues += "@" + dc.ColumnName + ",";
            }

            strQuery = strQuery.TrimEnd(',') + ")  VALUES (" + strValues + ")";

            return strQuery;
        }

        public override void InsertIntoDbWithParameters(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring)
        {

            try
            {

                using (MySqlConnection sqlcon = new MySqlConnection(connectionstring))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    string strQuery = BuildInsertStatementWithParameters(schmapper, dt);

                    using (MySqlTransaction trans = sqlcon.BeginTransaction())
                    {
                        using (MySqlCommand sqlcmd = new MySqlCommand(strQuery, sqlcon, trans))
                        {
                            sqlcmd.CommandType = CommandType.Text;


                            foreach (var Col in schmapper.Columns)
                            {


                                switch (Col.DataType)
                                {
                                    case SchemaMapper_Column.ColumnDataType.Date:
                                        sqlcmd.Parameters.Add("@" + Col.Name, MySqlDbType.DateTime);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Text:
                                        sqlcmd.Parameters.Add("@" + Col.Name, MySqlDbType.VarChar);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Memo:
                                        sqlcmd.Parameters.Add("@" + Col.Name, MySqlDbType.VarChar, 4000);
                                        break;
                                    case SchemaMapper_Column.ColumnDataType.Number:
                                        sqlcmd.Parameters.Add("@" + Col.Name, MySqlDbType.Int64);
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

    }
}
