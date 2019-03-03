using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;

namespace SchemaMapperDLL.Classes.SchemaMapping
{
    public class SchemaMapper : IDisposable
    {

        #region declarations

        #endregion

        #region properties
        string TableName { get; set; }
        string SchemaName { get; set; }

        List<SchemaMapper_Column> Columns;

        #endregion

        #region constructors

        public SchemaMapper(string DestinationSchema, string DestinationTable)
        {

            SchemaName = DestinationSchema;
            TableName = DestinationTable;


            //  MainDataSetConnectionString = SchemaMappingDbconnectionString;

            //    dsMain = new SchemaMapperDLL.Dataset.SchemaMapperDataSet();
            //    daDataTypes = new Dataset.SchemaMapperDataSetTableAdapters.DataTypesTableAdapter();
            //    daColumns = new Dataset.SchemaMapperDataSetTableAdapters.SchemaMapper_ColumnsTableAdapter();
            //    daSchemaMapper = new Dataset.SchemaMapperDataSetTableAdapters.SchemaMapperTableAdapter();
            //    daMapping = new Dataset.SchemaMapperDataSetTableAdapters.SchemaMapper_MappingTableAdapter();


            //    if (connectionType == DatabaseConnectionType.SQLServerCompactEdition)
            //    {
            //        using (SqlCeConnection sqlce = new SqlCeConnection(MainDataSetConnectionString))
            //        {

            //            if (sqlce.State != ConnectionState.Open)
            //                sqlce.Open();

            //            daDataTypes.Connection = sqlce;
            //            daColumns.Connection = sqlce;
            //            daMapping.Connection = sqlce;
            //            daSchemaMapper.Connection = sqlce;

            //            daDataTypes.Fill(dsMain.DataTypes);
            //            daSchemaMapper.Fill(dsMain.SchemaMapper);
            //            daColumns.Fill(dsMain.SchemaMapper_Columns);
            //            daMapping.Fill(dsMain.SchemaMapper_Mapping);

            //        }
            //    }else{
            //        throw new NotImplementedException();
            //    }
        }

        #endregion

        #region methods

        public bool ChangeTableStructure(ref DataTable dt, int SchemaMapperId)
        {
            try
            {
                //Check if all columns are mapped
                if (AreColumnsMapped(dt, SchemaMapperId) == false) return false;


                //Get columns Mapping for select Schema Mapper id
                var lstColsMap = Columns.SelectMany(x =>
                             x.MappedColumns.Select(y =>
                                 new
                                  {
                                      InputCol = y,
                                      OutputCol = x.Name,
                                      Type = x.DataType,
                                      DefaultValue = x.DefaultValue,
                                      IsExpression = x.IsExpression,
                                      Expression = x.Expression
                                  })).ToList();


                //Store current columns name inside a list and assign temporery names to columns
                List<TempColumn> lstTempColumns = new List<TempColumn>();
                int intCounter = 0;

                foreach (DataColumn dc in dt.Columns)
                {

                    TempColumn tempCol = new TempColumn("TempColumn_" + intCounter.ToString(), dc.ColumnName);
                    lstTempColumns.Add(tempCol);

                    dc.ColumnName = "TempColumn_" + intCounter.ToString();

                    intCounter++;
                }


                //Get Column Mapped to many columns

                var lstGroupByOutputCols = (from col in lstColsMap
                                            join tmp in lstTempColumns
                                            on col.InputCol equals tmp.originalCol
                                            group tmp by col.OutputCol into grp
                                            select new { Column = grp.Key, Inputs = grp.ToList() }).ToList();

                List<String> lstColumnsWithMultipleInput = (from grp in lstGroupByOutputCols
                                                            where grp.Inputs.Count() > 1
                                                            select grp.Column).ToList();

                //Get Columns for remove at the end

                List<String> lstColsToRemove = (from tmp in lstTempColumns
                                                where lstColsMap.Select(x => x.InputCol).ToList().Contains(tmp.originalCol) == false
                                                select tmp.originalCol).ToList();

                lstColsToRemove.AddRange(lstColumnsWithMultipleInput);


                //Assign expresion for multiple mapped columns

                foreach (var grp in lstGroupByOutputCols.Where(x => x.Inputs.Count > 1).ToList())
                {

                    DataColumn dc = new DataColumn();
                    

                    SchemaMapper_Column.ColumnDataType strType = lstColsMap.Where(x =>
                                                        x.OutputCol == grp.Column).Select(y => y.Type).First();

                    string expression = "";


                    switch (strType)
                    {

                        case SchemaMapper_Column.ColumnDataType.Date:
                            throw new Exception("Cannot map multiple columns into a column of type \"Date\"");
                            
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            expression = String.Join(" + ", grp.Inputs.Select(x => "IIF(ISNULL([" + x.tempCol + "],'') = '','',ISNULL([" + x.tempCol + "],'') + '|')").ToArray());
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            throw new Exception("Cannot map multiple columns into a column of type \"Number\"");
                            
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            expression = String.Join(" + ", grp.Inputs.Select(x => "IIF(ISNULL([" + x.tempCol + "],'') = '','','" + x.originalCol + ":' + ISNULL([" + x.tempCol + "],'')+ '" + Environment.NewLine + "')").ToArray());
                            break;
                    }

                    dc.Expression = expression;
                    dt.Columns.Add(dc);

                }


                //Assign Default Values as expression
                foreach (var col in lstColsMap.Where(x => x.DefaultValue != ""))
                {

                    DataColumn dc = new DataColumn();
                    dc.DataType = System.Type.GetType("System.String");

                    switch (col.Type)
                    {

                        case SchemaMapper_Column.ColumnDataType.Date:
                            dc.DataType = System.Type.GetType("System.DateTime");
                            dc.Expression = "CONVERT('" + col.DefaultValue + "',System.DateTime)";
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            dc.Expression = ("'" + col.DefaultValue + "'").Replace("''", "'");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            dc.DataType = System.Type.GetType("System.Int64");
                            dc.Expression = col.DefaultValue;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            dc.Expression = ("'" + col.DefaultValue + "'").Replace("''", "'");
                            break;
                    }
                    
                    

                    dt.Columns.Add(dc);


                }


                //Assign column Expression

                foreach (var col in lstColsMap.Where(x => x.IsExpression == true &&
                                                          x.Expression != ""))
                {

                    DataColumn dc = new DataColumn();
                    switch (col.Type)
                    {

                        case SchemaMapper_Column.ColumnDataType.Date:
                            dc.DataType = System.Type.GetType("System.DateTime");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            dc.DataType = System.Type.GetType("System.Int64");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            break;
                    }

                    string expression = col.Expression;

                    foreach (TempColumn tmp in lstTempColumns)
                    {
                        expression = expression.Replace("[" + tmp.originalCol + "]", "[" + tmp.tempCol + "]");
                    }


                    dc.Expression = expression;

                    dt.Columns.Add(dc);


                }


                //persist expressions

                DataTable dtNew = dt.Clone();

                foreach (DataColumn dtCol in dtNew.Columns)
                    dtCol.Expression = "";




                dtNew.Merge(dt);
                dt = dtNew;
                dtNew.Dispose();
                dtNew = null;

                //Remove unsused Columns

                List<String> lstColsToRemoveWithTemp = (from rmv in lstColsToRemove
                                                        join tmp in lstTempColumns
                                                        on rmv equals tmp.originalCol
                                                        select tmp.tempCol).ToList();

                foreach (string str in lstColsToRemoveWithTemp)
                {
                    dt.Columns.Remove(str);
                }


                //Rename remaining Columns

                var lstMapTempWithOutputCols = (from col in lstColsMap
                                                join tmp in lstTempColumns
                                                on col.InputCol equals tmp.originalCol
                                                where lstColsToRemoveWithTemp.Contains(tmp.tempCol) == false
                                                select new { tmp.tempCol, col.OutputCol }).ToList();

                foreach (var col in lstMapTempWithOutputCols)
                {
                    DataColumn dc = dt.Columns[col.tempCol];
                    dc.ColumnName = col.OutputCol;

                    SchemaMapper_Column smCol = Columns.Where(y => y.Name == col.OutputCol).First();

                    switch (smCol.DataType)
                    {
                        case SchemaMapper_Column.ColumnDataType.Date:
                            dc.DataType = System.Type.GetType("System.DateTime");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            dc.DataType = System.Type.GetType("System.Int64");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            break;

                    }

                }

                return true;
            }
            catch (Exception ex)
            {

                throw ex;

            }
        }
        public bool AreColumnsMapped(DataTable dt, int SchemaMapperId)
        {

            var lst = Columns.SelectMany(x => x.MappedColumns.Select(y => y)).ToList();

            int intCount = dt.Columns.Cast<DataColumn>().Where(x => lst.Contains(x.ColumnName) == false).Count();

            if (intCount > 0)
                return false;
            else
                return true;



        }
        public string BuildCreateTableQuery(int SchemaMapperId)
        {

            string strQuery = "if not exists(select * from information_schema.tables where table_name = '" + TableName +
                "' and table_schema = '" + SchemaName + "')" +
                "create table [" + SchemaName + "].[" + TableName + "](";


            foreach (var Col in Columns)
            {

                switch ( Col.DataType){

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
        public int CreateDestinationTable(string connection, int SchemaMapperId)
        {

            string cmd = BuildCreateTableQuery(SchemaMapperId);
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
                return -1;

            }

            return result;
        }

        #endregion

        #region Insert To Db Using Stored Procedure

        public string CreateTableTypeQuery()
        {
            string cmd = "CREATE TYPE [dbo].[MyTableType] AS TABLE(";

            foreach (var Col in Columns)
            {

                //only supported data types are text (nvarchar(255)) and memo (nvarchar(4000))
                cmd += "[" + Col.Name + "] [" + (Col.DataType == SchemaMapper_Column.ColumnDataType.Text ? "nvarchar](255)" : "nvarchar](4000)") + " NULL ,";

            }

            cmd = cmd.TrimEnd(',');

            cmd += ")";


            return cmd;





        }
        public string CreateStoredProcedureQuery()
        {

            string cmd = "CREATE PROCEDURE [dbo].[InsertTable]" + Environment.NewLine +
                         " @myTableType MyTableType readonly" + Environment.NewLine +
                         " AS" + Environment.NewLine +
                         " BEGIN" + Environment.NewLine +
                         " insert into [" + SchemaName + "].[" + TableName + " ] select * from @myTableType" + Environment.NewLine +
                         " END";


            return cmd;

        }
        public void InsertToSQLUsingStoredProcedure(DataTable dt, string connectionstring)
        {
            try
            {
                string cmdCreateTableType = CreateTableTypeQuery();
                string cmdCreateStoredProcdure = CreateStoredProcedureQuery();

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

        public void InsertToSQLUsingSQLBulk(DataTable dt, string connectionstring, int SchemaMapperId)
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
                    bulkCopy.DestinationTableName = "[" + SchemaName + "].[" + TableName + "]";
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
    public class TempColumn
    {

        public string tempCol { get; set; }
        public string originalCol { get; set; }

        public TempColumn(string temp, string original)
        {

            tempCol = temp;
            originalCol = original;

        }

    }

}
