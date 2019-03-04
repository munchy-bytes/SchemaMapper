# SchemaMapper

SchemaMapper is a data integration class library that facilitates data import process from external sources having different schema definitions. It replaces creating many integration services packages by writing few lines of codes.

It imports tabular data from different data sources (.xls, .xlsx, .csv, .txt, .mdb, .accdb, .htm, .json, .xml, .ppt, .pptx, .doc, .docx) into a SQL table with a user defined table schema after mapping columns between source and destination.

It allows users to add new computed and fixed valued columns.

------------------------

## Used technologies

SchemaMapper utilizes from many technologies to read data from different source such as:

- Microsoft Office Interop libraries to import tables from Word and Powerpoint
- [Json.Net library](https://www.newtonsoft.com/json/help/html/Introduction.htm) to import JSON
- [HtmlAgilityPack](https://html-agility-pack.net/) to import tables from HTML
- [SQL Server Compact 4.0](https://www.microsoft.com/en-us/download/details.aspx?id=17876) database to store configuration and schema information.
- [Microsoft Access database engine](https://www.microsoft.com/en-us/download/details.aspx?id=13255) to import data from Excel worksheets and Access databases. 
- .NET framework 4.5

------------------------

## Project details

SchemaMapper is composed of three main namespaces:

- **Converters:**  It reads data from external files into DataSet
- **DataCleaners:** Cleans files before importing
- **SchemaMapping:** Changes the imported data structure, and to import data to sql server.

-------------------------

## Examples:

### (1) Converters:

**Import data from Excel file (first worksheet)**

```cs
using (SchemaMapperDLL.Classes.Converters.MsExcelImport smExcel = new SchemaMapperDLL.Classes.Converters.MsExcelImport(@"U:\Passwords.xlsx","",false))
{

   //Read Excel
   smExcel.BuildConnectionString();
   var lst = smExcel.GetSheets();
   DataTable dt = smExcel.GetTableByName(lst.First(), true, 0);
   return dt;
}
```

**Import data from Excel file using paging**

```cs
using (SchemaMapperDLL.Classes.Converters.MsExcelImport smExcel = new SchemaMapperDLL.Classes.Converters.MsExcelImport(@"U:\Passwords.xlsx", "", false)){

   //Read Excel with pagging
   smExcel.BuildConnectionString();
   var lst = smExcel.GetSheets();

   int result = 1;
   int PagingStart = 1, PagingInterval = 10;

   while (result != 0){

      DataTable dt = smExcel.GetTableByNamewithPaging(lst.First(), PagingStart, PagingInterval, out result, true, 0);

      PagingStart = PagingStart + PagingInterval;

   }

}
```

**Import data from flat file (.txt, .csv)**

```cs
using (SchemaMapperDLL.Classes.Converters.FlatFileImportTools smFlat = new SchemaMapperDLL.Classes.Converters.FlatFileImportTools(@"U:\Passwords.csv",true,0))
{

   //Read flat file structure
   smFlat.BuildDataTableStructure();
   //Import data from flat file
   DataTable dt = smFlat.FillDataTable();
   int Result = dt.Rows.Count;

}
```

**Import data from word document**

```cs
using (SchemaMapperDLL.Classes.Converters.MsWordImportTools smWord = new SchemaMapperDLL.Classes.Converters.MsWordImportTools(@"U:\DocumentTable.docx", true, 0))
{

   smWord.ImportWordTablesIntoList(";");
   DataSet ds = smWord.ConvertListToTables(";");

   int ct = ds.Tables.Count;
}
```

### (3) SchemaMapping

**Initiate a SchemaMapper class**

First you have to imports `SchemaMapperDLL.Classes.SchemaMapping` namespace.

```cs
using SchemaMapperDLL.Classes.SchemaMapping;

public SchemaMapper InitiateTestSchemaMapper(string schema, string table){

   SchemaMapper smResult = new SchemaMapper();

   smResult.TableName = table;
   smResult.SchemaName = schema;

   //Add variables
   smResult.Variables.Add(new Variable("@Today", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));


   //Define Columns
  SchemaMapper_Column smServerCol = new SchemaMapper_Column("Server_Name", SchemaMapper_Column.ColumnDataType.Text);
   SchemaMapper_Column smUserCol = new SchemaMapper_Column("User_Name", SchemaMapper_Column.ColumnDataType.Text);
   SchemaMapper_Column smPassCol = new SchemaMapper_Column("Password", SchemaMapper_Column.ColumnDataType.Text);

   //Define a column with Fixed Value
   SchemaMapper_Column smFixedValueCol = new SchemaMapper_Column("AddedDate", SchemaMapper_Column.ColumnDataType.Text,"@Today");
   
   //Define a Column with Expression
   SchemaMapper_Column smExpressionCol = new SchemaMapper_Column("UserAndPassword",SchemaMapper_Column.ColumnDataType.Text,true,"[User_Name] + '|' + [Password]");

   //Add columns to SchemaMapper
   smResult.Columns.Add(smServerCol);
   smResult.Columns.Add(smUserCol);
   smResult.Columns.Add(smPassCol);
   smResult.Columns.Add(smFixedValueCol);
   smResult.Columns.Add(smExpressionCol);

   //Add all possible input Columns Names for each Column
   smServerCol.MappedColumns.AddRange(new[] {"server","server name","servername","Server","Server Name","ServerName"});
   smUserCol.MappedColumns.AddRange(new[] { "UserName", "User", "login", "Login", "User name" });
   smPassCol.MappedColumns.AddRange(new[] { "Password","pass", "Pass", "password" });

   //Added columns to ignore if found
   //Sys_SheetName and Sys_ExtraFields is an auto generated column when reading Excel file
   smResult.IgnoredColumns.AddRange(new[] { "Column1", "Sys_Sheetname", "Sys_ExtraFields", "Center Name" });

   //Save Schema Mapper into xml
   smResult.WriteToXml(Environment.CurrentDirectory + "\\SchemaMapper\\1.xml",true);

   return smResult;

}
```

**Change DataTable schema and insert into SQL using stored procedure with Table variable parameter**


```cs
DataTable dt = ReadExcel();

using (SchemaMapper SM = InitiateTestSchemaMapper("dbo","PasswordsTable"))
{

   bool result  = SM.ChangeTableStructure(ref dt);
   string con = @"Data Source=.\SQLINSTANCE;Initial Catalog=tempdb;integrated security=SSPI;";
 
   SM.CreateDestinationTable(con);

   SM.InsertToSQLUsingStoredProcedure(dt,con);

}

```
**Change DataTable schema and insert into SQL using BULK Insert**

```cs
DataTable dt = ReadExcel();

using (SchemaMapper SM = new SchemaMapper(Environment.CurrentDirectory + "\\SchemaMapper\\1.xml"))
{

   bool result  = SM.ChangeTableStructure(ref dt);
   string con = @"Data Source=.\SQLINSTANCE;Initial Catalog=tempdb;integrated security=SSPI;";
 
   SM.CreateDestinationTable(con);

   SM.InsertToSQLUsingSQLBulk(dt,con);

}
```

**Read SchemaMapper class from Saved XML**

```cs
using (SchemaMapper SM = new SchemaMapper("Environment.CurrentDirectory + "\\SchemaMapper\\1.xml")){

\\write your code here

}
```
