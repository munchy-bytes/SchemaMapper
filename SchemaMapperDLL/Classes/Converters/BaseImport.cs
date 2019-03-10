using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace SchemaMapperDLL.Classes.Converters
{
    public abstract class BaseImport
    {

        #region declaration
        public string FilePath { get; set; }
        public DataSet Maindataset { get; set; } 
        public  bool HasHeader { get; set; } 
        public  int RowsToSkip { get; set; } 

        #endregion

        #region abstract methods

        public BaseImport() { }

        #endregion

        #region methods

        #region export to Xml
             public void ExportDataTableAsXml(string v_ExportPath, int index)
        {
            Maindataset.Tables[index].WriteXml(v_ExportPath + "\\" + 
                Maindataset.Tables[index].TableName + ".xml", XmlWriteMode.WriteSchema, false);
        }

        public void ExportDataTableAsXml(string v_ExportPath,string tablename)
        {
         Maindataset.Tables[tablename].WriteXml(v_ExportPath + "\\" + 
             tablename + DateTime.Now.ToString("yyyyMMddHHmmsss") + ".xml", XmlWriteMode.WriteSchema, false);
        }

        public void ExportDataTablesAsXml(string v_ExportPath)
        {
            foreach (DataTable dtTable in Maindataset.Tables)
            {
                dtTable.WriteXml(v_ExportPath + "\\" + 
                    dtTable.TableName + DateTime.Now.ToString("yyyyMMddHHmmsss") + ".xml", XmlWriteMode.WriteSchema, false);
            }
        }

        public void ExportDataSetAsXml(string v_ExportPath)
        {
         Maindataset.WriteXml(v_ExportPath + "\\" + 
             DateTime.Now.ToString("yyyyMMddHHmmsss") + ".xml", XmlWriteMode.WriteSchema);
        }

        #endregion

        #region export to csv

        public void ExportDataTableAsCSV(DataTable dtDataTable, string strFilePath)
        {

            StringBuilder sb = new StringBuilder();

            IEnumerable<string> columnNames = dtDataTable.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName);
            sb.AppendLine(string.Join(",", columnNames));

            foreach (DataRow row in dtDataTable.Rows)
            {
                IEnumerable<string> fields = row.ItemArray.Select(field =>
  string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""));
                sb.AppendLine(string.Join(",", fields));
            }

            File.WriteAllText(strFilePath, sb.ToString());
        }

        public void ExportDataTableAsCSV(string v_ExportPath, int index)
        {
            ExportDataTableAsCSV(Maindataset.Tables[index],v_ExportPath + "\\" +
                Maindataset.Tables[index].TableName + ".csv");
        }

        public void ExportDataTableAsCSV(string v_ExportPath, string tablename)
        {
            ExportDataTableAsCSV(Maindataset.Tables[tablename],v_ExportPath + "\\" +
                tablename + DateTime.Now.ToString("yyyyMMddHHmmsss") + ".csv");
        }

        public void ExportDataTablesAsCSV(string v_ExportPath)
        {
            foreach (DataTable dtTable in Maindataset.Tables)
            {
                ExportDataTableAsCSV(dtTable,v_ExportPath + "\\" +
                    dtTable.TableName + DateTime.Now.ToString("yyyyMMddHHmmsss") + ".csv");
            }
        }



        #endregion





        #endregion

    }
}
