using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapper.Converters
{
    public abstract class BaseDbImport
    {

        #region declaration
        
        public string ConnectionString { get; set; }
        public DataTable SQLTable { get; set; }
        public DataTable SchemaTable { get; set; }

        #endregion

        #region constructors

        public BaseDbImport() {

            ConnectionString = "";
            SQLTable = new DataTable();
        
        }
        
        #endregion

        #region Methods

            public abstract void getSchemaTable();
            public abstract DataTable GetDataTable(string schema, string tablename);
            public abstract DataTable GetQueryResult(string sqlQuery);
            public abstract DataTable GetDataTableWithPaging(string schema, string tablename, int v_PagingStartRecord, int v_PagingInterval, out int r_result);
            public abstract DataTable GetDataTableWithPaging(string sqlQuery, int v_PagingStartRecord, int v_PagingInterval, out int r_result);

        #endregion

    }
}
