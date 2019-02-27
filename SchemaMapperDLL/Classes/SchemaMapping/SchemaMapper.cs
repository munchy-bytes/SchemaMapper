using System;
using System.Data;
using System.Data.SqlServerCe;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace SchemaMapperDLL.Classes.SchemaMapping
{
    public class SchemaMapper: IDisposable
    {

        #region declarations

        SchemaMapperDLL.Dataset.SchemaMapperDataSetTableAdapters.DataTypesTableAdapter daDataTypes;
        SchemaMapperDLL.Dataset.SchemaMapperDataSetTableAdapters.SchemaMapper_ColumnsTableAdapter daColumns;
        SchemaMapperDLL.Dataset.SchemaMapperDataSetTableAdapters.SchemaMapperTableAdapter daSchemaMapper;
        SchemaMapperDLL.Dataset.SchemaMapperDataSetTableAdapters.SchemaMapper_MappingTableAdapter daMapping;

        public enum DatabaseConnectionType
        {
            SQLServerCompactEdition = 0
        }

        #endregion
        
        #region properties

        SchemaMapperDLL.Dataset.SchemaMapperDataSet dsMain { get; set; }
        string MainDataSetConnectionString { get; set; }
 
        #endregion

        #region constructors

        public SchemaMapper(string SchemaMappingDbconnectionString, DatabaseConnectionType connectionType = DatabaseConnectionType.SQLServerCompactEdition)
        {
            
            MainDataSetConnectionString = SchemaMappingDbconnectionString;

            dsMain = new SchemaMapperDLL.Dataset.SchemaMapperDataSet();
            daDataTypes = new Dataset.SchemaMapperDataSetTableAdapters.DataTypesTableAdapter();
            daColumns = new Dataset.SchemaMapperDataSetTableAdapters.SchemaMapper_ColumnsTableAdapter();
            daSchemaMapper = new Dataset.SchemaMapperDataSetTableAdapters.SchemaMapperTableAdapter();
            daMapping = new Dataset.SchemaMapperDataSetTableAdapters.SchemaMapper_MappingTableAdapter();


            if (connectionType == DatabaseConnectionType.SQLServerCompactEdition)
            {
                using (SqlCeConnection sqlce = new SqlCeConnection(MainDataSetConnectionString))
                {

                    if (sqlce.State != ConnectionState.Open)
                        sqlce.Open();

                    daDataTypes.Connection = sqlce;
                    daColumns.Connection = sqlce;
                    daMapping.Connection = sqlce;
                    daSchemaMapper.Connection = sqlce;

                    daDataTypes.Fill(dsMain.DataTypes);
                    daSchemaMapper.Fill(dsMain.SchemaMapper);
                    daColumns.Fill(dsMain.SchemaMapper_Columns);
                    daMapping.Fill(dsMain.SchemaMapper_Mapping);

                }
            }else{
                throw new NotImplementedException();
            }
        }

        #endregion

        #region methods

        public bool ChangeTableStructure(ref DataTable dt, int SchemaMapperId)
        {
            try { 
            //Check if all columns are mapped
            if (AreColumnsMapped(dt,SchemaMapperId) == false) return false;


            //Get columns Mapping for select Schema Mapper id
            var lstColsMap = (from map in dsMain.SchemaMapper_Mapping
                              join col in dsMain.SchemaMapper_Columns
                              on new {sm = map.SchemaMapper_Id,colid = (map["OutputColumn"] == DBNull.Value  ? 0 : map.OutputColumn )} equals new {sm = col.SchemaMapper_Id,colid = col.Id }
                              join type in dsMain.DataTypes
                              on col.DataType equals type.Id
                              where map.SchemaMapper_Id == SchemaMapperId
                              select new
                              {
                                  InputCol = map.InputColumn,
                                  OutputCol = col.Name,
                                  Type = type.Type,
                                  DefaultValue = col.DefaultValue,
                                  IsExpression = col.IsExpression,
                                  Expression = col.Expression
                              }).ToList();


            //Store current columns name inside a list and assign temporery names to columns
            List<TempColumn> lstTempColumns = new List<TempColumn>();
            int intCounter = 0;

            foreach (DataColumn dc in dt.Columns)
            {

                TempColumn tempCol = new TempColumn("TempColumn_" + intCounter.ToString(),dc.ColumnName);
                lstTempColumns.Add(tempCol);

                dc.ColumnName = "TempColumn_" + intCounter.ToString();
                
                intCounter++;
            }


            //Get Column Mapped to many columns

            //var lstTempJoinOutputCols = (from col in lstColsMap
            //                            join tmp in lstTempColumns
            //                             on col.InputCol equals tmp.originalCol
            //                             select new {OutputCol = col.OutputCol, TempColumn = tmp.tempCol, InputColumn = col.InputCol}).ToList();

            //  var lstGroupByOutputCols = (from to in lstTempJoinOutputCols
            //                             group to by to.OutputCol into grp
            //                             select new { Column = grp.Key, Inputs = grp.ToList()}).ToList();

              var lstGroupByOutputCols = (from col in lstColsMap
                                          join tmp in lstTempColumns
                                          on col.InputCol equals tmp.originalCol
                                          group tmp by col.OutputCol into grp
                                          select new { Column = grp.Key, Inputs = grp.ToList() }).ToList();

            List<String>  lstColumnsWithMultipleInput = (from grp in lstGroupByOutputCols
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
                dc.DataType = System.Type.GetType("System.String");

                string strType = lstColsMap.Where(x => x.OutputCol ==  grp.Column).Select(y => y.Type).First();

                string expression;

                if (strType == "Memo"){
                    dc.MaxLength = 4000;
                    expression = String.Join(" + ", grp.Inputs.Select(x => "IIF(ISNULL([" + x.tempCol + "],'') = '','','" + x.originalCol + ":' + ISNULL([" + x.tempCol + "],'')+ '" + Environment.NewLine + "')").ToArray());
                }
                else{
                    dc.MaxLength = 255;
                    expression = String.Join(" + ", grp.Inputs.Select(x => "IIF(ISNULL([" + x.tempCol + "],'') = '','',ISNULL([" + x.tempCol + "],'') + '|')").ToArray());
                }


                dc.Expression = expression;
                dt.Columns.Add(dc);

            }


            //Assign Default Values as expression
            foreach (var col in lstColsMap.Where(x => x.DefaultValue != ""))
            {

                DataColumn dc = new DataColumn();
                dc.DataType = System.Type.GetType("System.String");

                if (col.Type == "Memo")
                    dc.MaxLength = 4000;
                else
                    dc.MaxLength = 255;

                dc.Expression = ("'" + col.DefaultValue + "'").Replace("''", "'");

                dt.Columns.Add(dc);


            }
            

            //Assign column Expression

            foreach (var col in lstColsMap.Where(x => x.IsExpression == true && 
                                                      x.Expression != ""))
            {

                DataColumn dc = new DataColumn();
                dc.DataType = System.Type.GetType("System.String");

                if (col.Type == "Memo")
                    dc.MaxLength = 4000;
                else
                    dc.MaxLength = 255;

                string expression = col.Expression;

                foreach (TempColumn tmp in lstTempColumns)
                {
                    expression = expression.Replace("[" + tmp.originalCol + "]","[" + tmp.tempCol + "]");
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

                dt.Columns[col.tempCol].ColumnName = col.OutputCol;

            }

            return true;
        }
            catch(Exception ex) {

                throw ex;
            
            }
        }


        public bool AreColumnsMapped(DataTable dt, int SchemaMapperId)
        {

            var lst = (from map in dsMain.SchemaMapper_Mapping
                      where map.SchemaMapper_Id == SchemaMapperId
                      select map.InputColumn).ToList();

            int intCount = dt.Columns.Cast<DataColumn>().Where(x => lst.Contains(x.ColumnName) == false).Count();

            if (intCount > 0)
                return false;
            else
                return true;


            
        }

        public string BuildCreateTableQuery(int SchemaMapperId)
        {

            var lstCols = (from col in dsMain.SchemaMapper_Columns
                           join type in dsMain.DataTypes
                          on col.DataType equals type.Id
                           where col.SchemaMapper_Id == SchemaMapperId
                           select new { OutputCol = col.Name, Type = type.Type }).ToList();

            var TableDef = (from sm in dsMain.SchemaMapper
                               where sm.Id == SchemaMapperId
                               select sm).First();

            string strQuery = "if not exists(select * from information_schema.tables where table_name = '" + TableDef.Table_Name +
                "' and table_schema = '" + TableDef.Table_Schema + "')" +
                "create table [" + TableDef.Table_Schema + "].[" + TableDef.Table_Name + "](";


            foreach (var Col in lstCols)
            {

                //only supported data types are text (nvarchar(255)) and memo (nvarchar(4000))
                strQuery += "[" + Col.OutputCol + "] [" + (Col.Type == "Text" ? "nvarchar](255)" : "nvarchar](4000)") + " NULL ,";

            }

            strQuery = strQuery.TrimEnd(',');
            
            strQuery += ") ON [PRIMARY]";


            return strQuery;
        }

        public int CreateDestinationTable(string connection, int SchemaMapperId)
        {

            string cmd = BuildCreateTableQuery(SchemaMapperId);
            int result = 0;
            try { 
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
            catch (Exception ex) {
                return -1;
            
            }

            return result;
        }



        #endregion

        #region Insert To Db Using Stored Procedure

        public string CreateTableTypeQuery(int SchemaMapperId)
        {
            string cmd = "CREATE TYPE [dbo].[MyTableType] AS TABLE(";

            var lstCols = (from col in dsMain.SchemaMapper_Columns
                           join type in dsMain.DataTypes
                          on col.DataType equals type.Id
                           where col.SchemaMapper_Id == SchemaMapperId
                           select new { OutputCol = col.Name, Type = type.Type }).ToList();


            foreach (var Col in lstCols)
            {

                //only supported data types are text (nvarchar(255)) and memo (nvarchar(4000))
                cmd += "[" + Col.OutputCol + "] [" + (Col.Type == "Text" ? "nvarchar](255)" : "nvarchar](4000)") + " NULL ,";

            }

            cmd = cmd.TrimEnd(',');

            cmd += ")";


            return cmd;





        }
         public string CreateStoredProcedureQuery(int SchemaMapperId)
        {
            var TableDef = (from sm in dsMain.SchemaMapper
                            where sm.Id == SchemaMapperId
                            select sm).First();


            string cmd = "CREATE PROCEDURE [dbo].[InsertTable]" + Environment.NewLine +
                         " @myTableType MyTableType readonly" + Environment.NewLine +
                         " AS" + Environment.NewLine +
                         " BEGIN" + Environment.NewLine +
                         " insert into [" + TableDef.Table_Schema + "].[" + TableDef.Table_Name + " ] select * from @myTableType" + Environment.NewLine +
                         " END";


            return cmd;

        }

        public void InsertToSQLUsingStoredProcedure(DataTable dt, string connectionstring, int SchemaMapperId)
         {
             try
             {
                 string cmdCreateTableType = CreateTableTypeQuery(SchemaMapperId);
                 string cmdCreateStoredProcdure = CreateStoredProcedureQuery(SchemaMapperId);

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
            try {

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
            catch (Exception ex) {
                throw ex;
            }
        }

        #endregion

        #region Insert to Db using SQLBulk

        public void InsertToSQLUsingSQLBulk(DataTable dt, string connectionstring, int SchemaMapperId) {

            var TableDef = (from sm in dsMain.SchemaMapper
                            where sm.Id == SchemaMapperId
                            select sm).First();

            
            try
            {
                using (var bulkCopy = new SqlBulkCopy(connectionstring, SqlBulkCopyOptions.KeepIdentity))
                {

                    foreach (DataColumn col in dt.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    bulkCopy.BulkCopyTimeout = 600;
                    bulkCopy.DestinationTableName = "[" + TableDef.Table_Schema + "].[" + TableDef.Table_Name + "]";
                    bulkCopy.WriteToServer(dt);
                }

            }catch (Exception ex)
            {
            throw ex;
            }
        }


        #endregion


        public void Dispose()
        {
            dsMain.Dispose();
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
