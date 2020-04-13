using System;
using System.Data;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.IO;
using System.Xml;

namespace SchemaMapper.SchemaMapping
{
    public class SchemaMapper : IDisposable
    {

        #region properties
        public string TableName { get; set; }
        public string SchemaName { get; set; }

        public List<SchemaMapper_Column> Columns;
        public List<string> IgnoredColumns;
        public List<Variable> Variables;



        #endregion

        #region constructors

        public SchemaMapper()
        {
            SchemaName = "";
            TableName = "";
            Columns = new List<SchemaMapper_Column>();
            IgnoredColumns = new List<string>();
            Variables = new List<Variable>();
        }


        public SchemaMapper(string xmlpath)
        {

            SchemaName = "";
            TableName = "";
            Columns = new List<SchemaMapper_Column>();
            IgnoredColumns = new List<string>();
            Variables = new List<Variable>();

            ReadFromXml(xmlpath);
        }

        public SchemaMapper(string DestinationSchema, string DestinationTable)
        {

            SchemaName = DestinationSchema;
            TableName = DestinationTable;
            Columns = new List<SchemaMapper_Column>();
            IgnoredColumns = new List<string>();
            Variables = new List<Variable>();
        }

        #endregion

        #region Load and Save mapping methods

        public void ReadFromXml(string path)
        {

            System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(SchemaMapper));
            string strObject = string.Empty;
            SchemaMapper smResult = new SchemaMapper();

            using (StreamReader sr = new StreamReader(path))
            {

                strObject = sr.ReadToEnd();
                sr.Close();
            }

            smResult = (SchemaMapper)xs.Deserialize(new StringReader(strObject));

            TableName = smResult.TableName.ToString();
            SchemaName = smResult.SchemaName.ToString();

            Columns.Clear();
            IgnoredColumns.Clear();
            Variables.Clear();

            Columns.AddRange(smResult.Columns);
            IgnoredColumns.AddRange(smResult.IgnoredColumns);

            foreach (var variable in smResult.Variables)
            {
                Variables.Add(new Variable(variable.Name, variable.Value));
            }


        }

        public void WriteToXml(string path, bool overwrite = false)
        {

            try
            {

                System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(SchemaMapper));
                StringWriter sr = new StringWriter();
                string strObject = string.Empty;

                xs.Serialize(sr, this);
                strObject = sr.ToString();

                if (File.Exists(path) && !overwrite)
                {
                    throw new Exception("File already Exists");

                }

                if (!System.IO.Directory.Exists(System.IO.Path.GetDirectoryName(path)))
                {
                    System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(path));
                }

                using (StreamWriter sw = new StreamWriter(path))
                {
                    sw.Write(strObject);
                    sw.Close();
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }


        }

        #endregion

        #region methods


        public bool ChangeTableStructure(ref DataTable dt)
        {
            try
            {
                //Check if all columns are mapped
                if (AreColumnsMapped(dt) == false) return false;


                //Get columns Mapping for select Schema Mapper id
                var lstColsMap = Columns.SelectMany(x =>
                             x.MappedColumns.Select(y =>
                                 new
                                 {
                                     InputCol = y,
                                     OutputCol = x.Name,
                                     Type = x.DataType,
                                     FixedValue = x.FixedValue,
                                     IsExpression = x.IsExpression,
                                     Expression = x.Expression
                                 })).ToList();


                //Store current columns name inside a list and assign temporery names to columns
                List<TempColumn> lstTempColumns = new List<TempColumn>();
                int intCounter = 0;

                foreach (DataColumn dc in dt.Columns)
                {

                    TempColumn tempCol = new TempColumn("TempColumn_" + intCounter.ToString(), dc.ColumnName);
                    lstTempColumns.Add(tempCol);

                    dc.ColumnName = "TempColumn_" + intCounter.ToString();

                    intCounter++;
                }


                //Get Column Mapped to many columns

                var lstGroupByOutputCols = (from col in lstColsMap
                                            join tmp in lstTempColumns
                                            on col.InputCol equals tmp.originalCol
                                            group tmp by col.OutputCol into grp
                                            select new { Column = grp.Key, Inputs = grp.ToList() }).ToList();

                List<String> lstColumnsWithMultipleInput = (from grp in lstGroupByOutputCols
                                                            where grp.Inputs.Count() > 1
                                                            select grp.Column).ToList();

                //Get Columns for remove at the end

                List<String> lstColsToRemove = (from tmp in lstTempColumns
                                                where lstColsMap.Select(x => x.InputCol).ToList().Contains(tmp.originalCol) == false
                                                select tmp.originalCol).ToList();

                lstColsToRemove.AddRange(lstColumnsWithMultipleInput);


                //Assign expresion for multiple mapped columns

                foreach (var grp in lstGroupByOutputCols.Where(x => x.Inputs.Count > 1).ToList())
                {

                    DataColumn dc = new DataColumn();


                    SchemaMapper_Column.ColumnDataType strType = lstColsMap.Where(x =>
                                                        x.OutputCol == grp.Column).Select(y => y.Type).First();

                    string expression = "";


                    switch (strType)
                    {
                        case SchemaMapper_Column.ColumnDataType.Boolean:
                            throw new Exception("Cannot map multiple columns into a column of type \"Boolean\"");

                        case SchemaMapper_Column.ColumnDataType.Date:
                            throw new Exception("Cannot map multiple columns into a column of type \"Date\"");

                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            expression = String.Join(" + ", grp.Inputs.Select(x => "IIF(ISNULL([" + x.tempCol + "],'') = '','',ISNULL([" + x.tempCol + "],'') + '|')").ToArray());
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            throw new Exception("Cannot map multiple columns into a column of type \"Number\"");

                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            expression = String.Join(" + ", grp.Inputs.Select(x => "IIF(ISNULL([" + x.tempCol + "],'') = '','','" + x.originalCol + ":' + ISNULL([" + x.tempCol + "],'')+ '" + Environment.NewLine + "')").ToArray());
                            break;
                    }

                    dc.ColumnName = grp.Column;
                    dc.Expression = expression;
                    dt.Columns.Add(dc);

                }


                //Assign Default Values as expression
                foreach (var col in Columns.Where(x => x.FixedValue != "" && !x.IsExpression))
                {

                    DataColumn dc = new DataColumn();
                    dc.DataType = System.Type.GetType("System.String");

                    string strValue = col.FixedValue;

                    foreach (var variable in Variables.Where(x => x.Name.StartsWith(@"@")).ToList())
                    {
                        strValue = strValue.Replace(variable.Name, variable.Value);
                    }


                    switch (col.DataType)
                    {

                        case SchemaMapper_Column.ColumnDataType.Boolean:
                            dc.DataType = System.Type.GetType("System.Boolean");
                            dc.Expression = strValue;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Date:
                            dc.DataType = System.Type.GetType("System.DateTime");
                            dc.Expression = "CONVERT('" + strValue + "',System.DateTime)";
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            dc.Expression = ("'" + strValue + "'").Replace("''", "'");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            dc.DataType = System.Type.GetType("System.Int64");
                            dc.Expression = strValue;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            dc.Expression = ("'" + strValue + "'").Replace("''", "'");
                            break;
                    }


                    dc.ColumnName = col.Name;
                    dt.Columns.Add(dc);


                }


                //Get columns to be removed at the end
                List<String> lstColsToRemoveWithTemp = (from rmv in lstColsToRemove
                                                        join tmp in lstTempColumns
                                                        on rmv equals tmp.originalCol
                                                        select tmp.tempCol).ToList();

                //Map temp columns with output columns 
                var lstMapTempWithOutputCols = (from col in lstColsMap
                                                join tmp in lstTempColumns
                                                on col.InputCol equals tmp.originalCol
                                                where lstColsToRemoveWithTemp.Contains(tmp.tempCol) == false
                                                select new { tmp.tempCol, tmp.originalCol, col.OutputCol }).ToList();

                //Assign column Expression

                foreach (var col in Columns.Where(x => x.IsExpression == true &&
                                                          x.Expression != ""))
                {

                    DataColumn dc = new DataColumn();
                    switch (col.DataType)
                    {

                        case SchemaMapper_Column.ColumnDataType.Boolean:
                            dc.DataType = System.Type.GetType("System.Boolean");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Date:
                            dc.DataType = System.Type.GetType("System.DateTime");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            dc.DataType = System.Type.GetType("System.Int64");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            break;
                    }

                    string expression = col.Expression;

                    //fix columns names inside expression
                    foreach (var tmp in lstMapTempWithOutputCols)
                    {
                        expression = expression.Replace("[" + tmp.OutputCol + "]", "[" + tmp.tempCol + "]");
                    }

                    foreach (var variable in Variables.Where(x => x.Name.StartsWith(@"@")).ToList())
                    {
                        expression = expression.Replace(variable.Name, "'" + variable.Value + "'");
                    }


                    dc.Expression = expression;
                    dc.ColumnName = col.Name;
                    dt.Columns.Add(dc);


                }


                //persist expressions

                DataTable dtNew = dt.Clone();

                foreach (DataColumn dtCol in dtNew.Columns)
                    dtCol.Expression = "";




                dtNew.Merge(dt);
                dt = dtNew;
                dtNew.Dispose();
                dtNew = null;

                //Remove unsused Columns

                foreach (string str in lstColsToRemoveWithTemp)
                {
                    dt.Columns.Remove(str);
                }


                //Rename remaining Columns

                foreach (var col in lstMapTempWithOutputCols)
                {
                    DataColumn dc = dt.Columns[col.tempCol];
                    dc.ColumnName = col.OutputCol;

                    SchemaMapper_Column smCol = Columns.Where(y => y.Name == col.OutputCol).First();

                    switch (smCol.DataType)
                    {
                        case SchemaMapper_Column.ColumnDataType.Boolean:
                            dc.DataType = System.Type.GetType("System.Boolean");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Date:
                            dc.DataType = System.Type.GetType("System.DateTime");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Text:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 255;
                            break;
                        case SchemaMapper_Column.ColumnDataType.Number:
                            dc.DataType = System.Type.GetType("System.Int64");
                            break;
                        case SchemaMapper_Column.ColumnDataType.Memo:
                            dc.DataType = System.Type.GetType("System.String");
                            dc.MaxLength = 4000;
                            break;

                    }

                }

                return true;
            }
            catch (Exception ex)
            {

                throw ex;

            }
        }

        public bool AreColumnsMapped(DataTable dt)
        {

            var lst = Columns.SelectMany(x => x.MappedColumns.Select(y => y)).ToList();

            int intCount = dt.Columns.Cast<DataColumn>().Where(x => lst.Contains(x.ColumnName) == false &&
                                                                    IgnoredColumns.Contains(x.ColumnName) == false).Count();

            if (intCount > 0)
                return false;
            else
                return true;



        }



        #endregion

        public void Dispose()
        {

        }
    }

    public class TempColumn
    {

        public string tempCol { get; set; }
        public string originalCol { get; set; }

        public TempColumn(string temp, string original)
        {

            tempCol = temp;
            originalCol = original;

        }

    }

    public class Variable
    {
        public string Name { get; set; }
        public string Value { get; set; }

        public Variable(string name, string value)
        {

            Name = name;
            Value = value;

        }

        private Variable()
        {



        }

    }

}
