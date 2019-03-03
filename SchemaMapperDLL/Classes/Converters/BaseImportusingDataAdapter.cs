using System;
using System.Data;
using System.Data.OleDb;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SchemaMapperDLL.Classes.Converters
{
    public abstract class BaseImportusingOLEDB : BaseImport
    {

        #region declarations

        public OleDbDataAdapter oledbAdapter;
        public DataTable SchemaTable { get; set; }
        public string ConnectionString { get; set; } 
                 
        #endregion

        #region constructors

                public BaseImportusingOLEDB() { }

        #endregion

        #region abstract functions
       
        public abstract DataSet FillAllTables(string Tablename);
        public abstract void getSchemaTable();
        public abstract void BuildConnectionString();

        #endregion


    }
}
