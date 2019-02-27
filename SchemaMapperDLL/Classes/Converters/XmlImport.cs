using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapperDLL.Classes.Converters
{
    public class XmlImport : BaseImport, IDisposable
    {

        public XmlImport(string xmlpath)
        {


            FilePath = xmlpath;
        }


        public DataSet ReadDataSetFromXml(XmlReadMode ReadMode)
        {

            Maindataset.ReadXml(FilePath, ReadMode);

            return Maindataset;


        }

        public DataTable ReadDataTableFromXml()
        {

            DataTable dt = new DataTable();
            dt.ReadXml(FilePath);

            return dt;


        }

        public void Dispose()
        {
            Maindataset.Dispose();
        }

    }

    
}
