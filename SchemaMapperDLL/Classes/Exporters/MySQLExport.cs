using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using SchemaMapperDLL.Classes.SchemaMapping;

namespace SchemaMapperDLL.Classes.Exporters
{
    class MySQLExport: BaseDbExport, IDisposable
    {
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public override string BuildCreateTableQuery(SchemaMapping.SchemaMapper schmapper)
        {
            throw new NotImplementedException();
        }

        public override int CreateDestinationTable(SchemaMapping.SchemaMapper schmapper, string connection)
        {
            throw new NotImplementedException();
        }

        #region Insert to Db using SQLBulk

        public void InsertUsingMySQLBulkLoader(SchemaMapper schmapper, DataTable dt, string connectionstring)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
