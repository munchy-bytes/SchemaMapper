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


        public DataTable ReadExcel()
        {
             

                using (SchemaMapperDLL.Classes.Converters.MsExcelImport smExcel = new SchemaMapperDLL.Classes.Converters.MsExcelImport(@"U:\Passwords.xlsx","",false))
                  {

                     //Read Excel
                      smExcel.BuildConnectionString();
                      var lst = smExcel.GetSheets();

                      DataTable dt = smExcel.GetTableByName(lst.First(), true, 0);
                      return dt;
                  }

                

        }

        private void Form1_Load(object sender, EventArgs e)
        {
            
            DataTable dt = ReadExcel();
           
            string confdb = "DataSource=" + Environment.CurrentDirectory + "\\Database\\SchemaMapperConfig.sdf";

            using (SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper SM = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper(confdb))
            {

               bool result  = SM.ChangeTableStructure(ref dt, 1);
               string con = @"Data Source=.\SQLInstance;Initial Catalog=tempdb;integrated security=SSPI;";
               SM.CreateDestinationTable(con, 1);

                //SM.InsertToSQLUsingSQLBulk(dt,con,1);
                
                SM.InsertToSQLUsingStoredProcedure(dt,con,1);

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

            using (SchemaMapperDLL.Classes.Converters.FlatFileImportTools smFlat = new SchemaMapperDLL.Classes.Converters.FlatFileImportTools(@"U:\Passwords.csv",true,0))
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
