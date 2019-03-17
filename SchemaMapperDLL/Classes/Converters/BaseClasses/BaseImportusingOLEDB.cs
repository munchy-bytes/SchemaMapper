using System;
using System.Data;
using System.Data.OleDb;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SchemaMapper.Converters
{
    public abstract class BaseImportusingOLEDB : BaseImport
    {

        #region declarations

            public DataTable SchemaTable { get; set; }
            public string ConnectionString { get; set; } 
            
        #endregion

        #region constructors

                public BaseImportusingOLEDB() { }

        #endregion

        #region abstract functions
       
        public abstract DataSet FillAllTables();
        public abstract void getSchemaTable();
        public abstract void BuildConnectionString();

        #endregion


    }
}
