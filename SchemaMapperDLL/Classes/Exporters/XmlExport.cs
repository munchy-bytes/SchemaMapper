using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapperDLL.Classes.Exporters
{
    public class XmlExport : BaseFileExport, IDisposable
    {


        public XmlWriteMode WriteMode { get; set; }

        #region constructors

        public XmlExport(string ExportDir, XmlWriteMode writemode = XmlWriteMode.WriteSchema)
        {

            WriteMode = writemode;
            ExportDirectory = ExportDir;
            
        }


        #endregion


        #region methods

        public override  void ExportDataTable(DataSet dsMain, int index)
        {
            dsMain.Tables[index].WriteXml(ExportDirectory + "\\" +
                dsMain.Tables[index].TableName + ".xml", WriteMode, false);
        }

        public override  void ExportDataTable(DataSet dsMain, string tablename)
        {
            dsMain.Tables[tablename].WriteXml(ExportDirectory + "\\" +
                tablename + DateTime.Now.ToString("yyyyMMddHHmmsss") + ".xml", WriteMode, false);
        }

        public override  void ExportDataTables(DataSet dsMain)
        {
            foreach (DataTable dtTable in dsMain.Tables)
            {
                dtTable.WriteXml(ExportDirectory + "\\" +
                    dtTable.TableName + DateTime.Now.ToString("yyyyMMddHHmmsss") + ".xml", WriteMode, false);
            }
        }

        public  void ExportDataSet(DataSet dsMain)
        {
            dsMain.WriteXml(ExportDirectory + "\\" +
                DateTime.Now.ToString("yyyyMMddHHmmsss") + ".xml", WriteMode);
        }

        #endregion



        public void Dispose()
        {

        }
    }
}
