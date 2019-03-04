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

        public string Name { get; set; }
        public string FixedValue { get; set; }
        public bool IsExpression { get; set; }
        public string Expression { get; set; }
        public ColumnDataType DataType { get; set; }
        public List<string> MappedColumns { get; set; }

        #endregion

        #region construtors

        //Column with expression
        public SchemaMapper_Column(string name, ColumnDataType datatype, bool isexpression, string expression)
        {

            Name = name;
            FixedValue = "";
            IsExpression = isexpression;
            Expression = expression;
            DataType = datatype;
            MappedColumns = new List<string>();


        }

        //Normal Column
        public SchemaMapper_Column(string name, ColumnDataType datatype)
        {

            Name = name;
            FixedValue = "";
            IsExpression = false;
            Expression = "";
            DataType = datatype;
            MappedColumns = new List<string>();


        }

        //Column with fixed value   
        public SchemaMapper_Column(string name, ColumnDataType datatype, string fixedValue)
        {

            Name = name;
            FixedValue = fixedValue;
            IsExpression = false;
            Expression = "";
            DataType = datatype;
            MappedColumns = new List<string>();


        }

        //Column without any specifications
        public SchemaMapper_Column()
        {

            Name = "";
            FixedValue = "";
            IsExpression = false;
            Expression = "";
            DataType = ColumnDataType.Text;
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
