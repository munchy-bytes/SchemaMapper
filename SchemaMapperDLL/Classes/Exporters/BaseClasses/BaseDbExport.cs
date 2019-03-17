using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SchemaMapper;

namespace SchemaMapper.Exporters
{
    public abstract class  BaseDbExport
    {

        public string ConnectionString { get; set; }


        public abstract string BuildCreateTableQuery(SchemaMapping.SchemaMapper schmapper);
        public abstract int CreateDestinationTable(SchemaMapping.SchemaMapper schmapper);
        public abstract string BuildInsertStatement(SchemaMapping.SchemaMapper schmapper, DataTable dt, int startindex, int rowscount);
        public abstract void InsertIntoDb(SchemaMapping.SchemaMapper schmapper, DataTable dt, int rowsperbatch = 10000);
        public abstract string BuildInsertStatementWithParameters(SchemaMapping.SchemaMapper schmapper, DataTable dt);
        public abstract void InsertIntoDbWithParameters(SchemaMapping.SchemaMapper schmapper, DataTable dt);
    }
}
