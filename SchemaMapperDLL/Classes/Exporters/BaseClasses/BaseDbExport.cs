using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SchemaMapper.SchemaMapping;

namespace SchemaMapper.Exporters
{
    public abstract class  BaseDbExport
    {

        public abstract string BuildCreateTableQuery(SchemaMapper.SchemaMapping.SchemaMapper schmapper);
        public abstract int CreateDestinationTable(SchemaMapper.SchemaMapping.SchemaMapper schmapper,string connection);
        public abstract string BuildInsertStatement(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, int startindex, int rowscount);
        public abstract void InsertIntoDb(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring, int rowsperbatch = 10000);
        public abstract string BuildInsertStatementWithParameters(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt);
        public abstract void InsertIntoDbWithParameters(SchemaMapper.SchemaMapping.SchemaMapper schmapper, DataTable dt, string connectionstring);
    }
}
