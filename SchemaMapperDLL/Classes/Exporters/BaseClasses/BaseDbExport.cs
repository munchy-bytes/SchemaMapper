using System;
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

    }
}
