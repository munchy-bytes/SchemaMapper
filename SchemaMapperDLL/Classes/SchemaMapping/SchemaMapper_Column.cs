using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SchemaMapper.SchemaMapping
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

        #region methods

        public ColumnDataType GetCorrespondingDataType(string TypeName, int length)
        {

            switch (TypeName)
            {

                case "System.String":
                case "System.Char":

                    if (length == 4000)
                        return ColumnDataType.Memo;
                    
                    return ColumnDataType.Text;

                case "System.Int32":
                case "System.Int64":
                case "System.Int16":
                case "System.UInt64":
                case "System.UInt32":
                case "System.UInt16":
                case "System.Byte":
                case "System.SByte":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":

                    return ColumnDataType.Number;

                case "System.Boolean":

                    return ColumnDataType.Boolean;

                    
                case "System.DateTime":
                case "System.Date":
                case "System.TimeSpan":

                    return ColumnDataType.Date;
                case "":

                    throw new Exception("Data \"" + TypeName  + "\"type not supported");
                  
            }

            return ColumnDataType.Text;

        }

        #endregion


        #region declarations
        public enum ColumnDataType
        {
            Text = 0,
            Memo = 1,
            Number = 2,
            Date = 3,
            Boolean = 4
        }
        #endregion
    }

}
