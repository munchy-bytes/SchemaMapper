using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapperDLL.Classes.SchemaMapping
{
    public class SchemaMapper_Column
    {
        #region properties
        public int ID { get; set; }
        public string Name { get; set; }
        public string DefaultValue { get; set; }
        public bool IsExpression { get; set; } = false;
        public string Expression { get; set; }
        public ColumnDataType DataType { get; set; }
        public List<string> MappedColumns { get; set; }

        #endregion

        #region methods
        public SchemaMapper_Column(int id, string name, string defValue, bool isexpression, string expression, ColumnDataType datatype)
        {

            ID = id;
            Name = name;
            DefaultValue = defValue;
            IsExpression = isexpression;
            Expression = expression;
            DataType = datatype;
            MappedColumns = new List<string>();


        }
        #endregion

        #region declarations
        public enum ColumnDataType
        {
            Text = 0,
            Memo = 1,
            Number = 2,
            Date = 3
        }
        #endregion
    }

}
