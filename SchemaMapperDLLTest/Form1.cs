using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using SchemaMapperDLL;


namespace SchemaMapperDLLTest
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }


        public void ReadAccessIntoSQL()
        {

            using (SchemaMapperDLL.Classes.Converters.MsAccessImport smAccess = new SchemaMapperDLL.Classes.Converters.MsAccessImport(@"G:\Passwords.mdb"))
            {

                //Read Access
                smAccess.BuildConnectionString();
                smAccess.getSchemaTable();

                string con = @"Data Source=.\SQLInstance;Initial Catalog=tempdb;integrated security=SSPI;";

                using (SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper SM = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper("dbo", "Passwords"))
                {
                    SM.CreateDestinationTable(con);

                    foreach (DataRow schRow in smAccess.SchemaTable.Rows)
                    {
                        string strTablename = schRow["TABLE_NAME"].ToString().Trim('\'');

                        DataTable dt = smAccess.GetTableByName(strTablename);
                        bool result = SM.ChangeTableStructure(ref dt);

                        if (result == true)
                        {
                            SM.InsertToSQLUsingSQLBulk(dt, con);
                        }
                    }



                }


            }

        }

        public DataTable ReadExcel()
        {


            using (SchemaMapperDLL.Classes.Converters.MsExcelImport smExcel = new SchemaMapperDLL.Classes.Converters.MsExcelImport(@"G:\Passwords.xlsx", "", false))
            {

                //Read Excel
                smExcel.BuildConnectionString();
                var lst = smExcel.GetSheets();

                DataTable dt = smExcel.GetTableByName(lst.First(), true, 0);
                return dt;
            }



        }

        public SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper InitiateTestSchemaMapper(string schema, string table)
        {

            SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper smResult = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper();

            smResult.TableName = table;
            smResult.SchemaName = schema;

            //Add variables
            smResult.Variables.Add(new SchemaMapperDLL.Classes.SchemaMapping.Variable("@Today", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));


            //Add Columns

            SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column smServerCol = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column("Server_Name",
                                                                                                                                     SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column.ColumnDataType.Text);
            SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column smUserCol = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column("User_Name",
                                                                                                                                     SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column.ColumnDataType.Text);
            SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column smPassCol = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column("Password",
                                                                                                                                     SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column.ColumnDataType.Text);

            //// Add column with Fixed Value
            SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column smFixedValueCol = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column("AddedDate",
                                                                                                                                    SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column.ColumnDataType.Text,
                                                                                                                                    "@Today");
            //// Add Column with Expression
            SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column smExpressionCol = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column("UserAndPassword",
                                                                                                                                    SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper_Column.ColumnDataType.Text,
                                                                                                                                    true,
                                                                                                                                    "[User_Name] + '|' + [Password]");

            smResult.Columns.Add(smServerCol);
            smResult.Columns.Add(smUserCol);
            smResult.Columns.Add(smPassCol);
            smResult.Columns.Add(smFixedValueCol);
            smResult.Columns.Add(smExpressionCol);

            //Add all possible input Columns for each Column

            smServerCol.MappedColumns.AddRange(new[] { "server", "server name", "servername", "Server", "Server Name", "ServerName" });
            smUserCol.MappedColumns.AddRange(new[] { "UserName", "User", "login", "Login", "User name" });
            smPassCol.MappedColumns.AddRange(new[] { "Password", "pass", "Pass", "password" });

            //Added columns to ignore if found
            //Sys_SheetName and Sys_ExtraFields is an auto generated column when reading Excel file
            smResult.IgnoredColumns.AddRange(new[] { "Column1", "Sys_Sheetname", "Sys_ExtraFields", "Center Name" });

            //Save Schema Mapper into xml
            smResult.WriteToXml(Environment.CurrentDirectory + "\\SchemaMapper\\1.xml", true);

            return smResult;

        }

        private void Form1_Load(object sender, EventArgs e)
        {

            DataTable dt = ReadExcel();

            using (SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper SM = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper(Environment.CurrentDirectory + "\\SchemaMapper\\1.xml"))
            {

                bool result = SM.ChangeTableStructure(ref dt);
                string con = @"Data Source=.\SQLINSTANCE;Initial Catalog=tempdb;integrated security=SSPI;";

                SM.CreateDestinationTable(con);

                SM.InsertToSQLUsingStoredProcedure(dt, con);

            }

        }

        public void ReadExcelWithPagging()
        {

            using (SchemaMapperDLL.Classes.Converters.MsExcelImport smExcel = new SchemaMapperDLL.Classes.Converters.MsExcelImport(@"U:\Passwords.xlsx", "", false))
            {

                //Read Excel with pagging
                smExcel.BuildConnectionString();
                var lst = smExcel.GetSheets();

                int result = 1;
                int PagingStart = 1, PagingInterval = 10;

                while (result != 0)
                {

                    DataTable dt = smExcel.GetTableByNamewithPaging(lst.First(), PagingStart, PagingInterval, out result, true, 0);
                    PagingStart = PagingStart + PagingInterval;

                }

            }
        }

        public void ReadFlatFile()
        {

            using (SchemaMapperDLL.Classes.Converters.FlatFileImportTools smFlat = new SchemaMapperDLL.Classes.Converters.FlatFileImportTools(@"U:\Passwords.csv", true, 0))
            {

                //Read Excel with pagging
                smFlat.BuildDataTableStructure();

                DataTable dt = smFlat.FillDataTable();

                int Result = dt.Rows.Count;
            }

        }

        public void ReadWordDocument()
        {
            using (SchemaMapperDLL.Classes.Converters.MsWordImportTools smWord = new SchemaMapperDLL.Classes.Converters.MsWordImportTools(@"U:\DocumentTable.docx", true, 0))
            {

                smWord.ImportWordTablesIntoList(";");
                DataSet ds = smWord.ConvertListToTables(";");

                int ct = ds.Tables.Count;
            }
        }
    }
}
