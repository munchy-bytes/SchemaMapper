using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SchemaMapperDLL.Classes.SchemaMapping;

namespace SchemaMapperDLL.Classes.Exporters
{
    public abstract class  BaseDbExport
    {

        public abstract string BuildCreateTableQuery(SchemaMapper schmapper);
        public abstract int CreateDestinationTable(SchemaMapper schmapper,string connection);
        public abstract string BuildInsertStatement(SchemaMapper schmapper, DataTable dt, int startindex, int rowscount);
        public abstract void InsertIntoDb(SchemaMapper schmapper, DataTable dt, string connectionstring, int rowsperbatch = 10000);
        public abstract string BuildInsertStatementWithParameters(SchemaMapper schmapper, DataTable dt);
        public abstract void InsertIntoDbWithParameters(SchemaMapper schmapper, DataTable dt, string connectionstring);
    }
}
