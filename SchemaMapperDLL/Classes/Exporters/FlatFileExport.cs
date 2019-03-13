using System;
using System.IO;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapperDLL.Classes.Exporters
{
    public class FlatFileExport : BaseFileExport, IDisposable
    {

        string Extension { get; set; }

        #region constructors

        public FlatFileExport(string ExportDir, string extension = ".csv")
        {

            ExportDirectory = ExportDir;
            Extension = extension;

        }

        #endregion


        #region methods

        public  void ExportDataTable(DataTable dtDataTable)
        {

            string FilePath = ExportDirectory.TrimEnd('\\') + "\\" + dtDataTable.TableName + Extension;

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

            File.WriteAllText(FilePath, sb.ToString());
        }
        public override  void ExportDataTable(DataSet dsMain, int index)
        {
            ExportDataTable(dsMain.Tables[index]);
        }
        public override  void ExportDataTable(DataSet dsMain, string tablename)
        {
            ExportDataTable(dsMain.Tables[tablename]);
        }
        public override  void ExportDataTables(DataSet dsMain)
        {
            foreach (DataTable dtTable in dsMain.Tables)
            {
                ExportDataTable(dtTable);
            }
        }

        #endregion


        public void Dispose()
        {
           
        }
    }
}
