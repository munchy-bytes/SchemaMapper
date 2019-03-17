using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapper.Exporters
{
    public abstract class BaseFileExport
    {

        public BaseFileExport() { }

        public string ExportDirectory { get; set; }
        
        public abstract void ExportDataTable(DataSet dsMain,  int index);
        public abstract void ExportDataTable(DataSet dsMain,  string tablename);
        public abstract void ExportDataTables(DataSet dsMain);
        

    }
}
