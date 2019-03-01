# SchemaMapper

SchemaMapper is a data integration class library that facilitates data import process from external sources having different schema definitions. It replace creating many integration services packages by writing few lines of codes.

It imports tabular data from different data sources (.xls, .xlsx, .csv, .txt, .mdb, .accdb, .htm, .json, .xml, .ppt, .pptx, .doc, .docx) into a SQL table with a user defined table schema after mapping columns between source and destination.

It allow users the to add new computed and fixed values columns.

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
- **DataCleaners:** Clean files before importing
- **SchemaMapping:** Changes the imported data structure, and to import data to sql server.

-------------------------

## Configuration database

The configuration database is an SQL compact 4.0 database which contains 4 tables:

- **DataTypes:** Supported data types for destination columns *(Until now only nvarchar (Text))*
- **SchemaMapper:** Where each schema mapper row define a schema mapping instance where destination columns must be defined, destination schema and table name, input and output columns mapping.
- **SchemaMapper_Columns:** Contains the destination columns for each Schema Mapper
- **SchemaMapper_Mapping:** Contains the Mapping information between the destination columns and each possible input column name (example: if we have `First_Name` column in the destination table it could be mapped to `Fname` and `FirstName`)

---------------------------

## Examples:

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

	while (result != 0)
	{

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

**Change table schema and insert into SQL using Bulk insert**

```cs
using (SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper SM = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper(confdb))
{

bool result  = SM.ChangeTableStructure(ref dt, 1);
string con = @"Data Source=.\SQLInstance;Initial Catalog=tempdb;integrated security=SSPI;";
SM.CreateDestinationTable(con, 1);

SM.InsertToSQLUsingSQLBulk(dt,con,1);

}
```

**Insert into SQL using stored procedure with Table variable parameter**


```cs
using (SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper SM = new SchemaMapperDLL.Classes.SchemaMapping.SchemaMapper(confdb))
{

bool result  = SM.ChangeTableStructure(ref dt, 1);
string con = @"Data Source=.\SQLInstance;Initial Catalog=tempdb;integrated security=SSPI;";
SM.CreateDestinationTable(con, 1);

SM.InsertToSQLUsingStoredProcedure(dt,con,1);

}
```
