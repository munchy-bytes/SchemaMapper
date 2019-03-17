using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace SchemaMapper.Converters
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


    }
}
