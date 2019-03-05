using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Office.Interop.Excel;
using System.Text.RegularExpressions;

namespace SchemaMapperDLL.Classes.FileCleaners
{

    public enum ExcelCleanOperationType
    {

        GetUsedRange = 0,
        CleanExcel = 1,
        RemoveTopEmptyAndGetUsedRange = 2

    }

    class MsExcelCleaner : IDisposable
    {



        #region Cleanning related methods

        private string GetSheetRelatedString(Worksheet worksheet, ref int MaxRow, ref int MaxCol, int v_range = 10000)
        {

            string strResult = string.Empty;

            Range targetCells = worksheet.UsedRange;

            currentCleaninngRange = v_range;

            int totalRows = targetCells.Rows.Count;
            int totalCols = targetCells.Columns.Count;

            List<int> emptyRows = new List<int>();
            List<int> emptyCols = new List<int>();

            int intCount = 1;

            while (intCount <= totalRows)
            {

                int intRangeRows = 0;

                if (totalRows >= intCount + v_range)
                {
                    intRangeRows = v_range;
                }
                else
                {
                    intRangeRows = totalRows - intCount + 1;
                }

                object[,] allValues = (object[,])(targetCells.Range[targetCells.Cells[intCount, 1], targetCells.Cells[intCount + intRangeRows, totalCols]].Cells.Value);

                if (intCount == 1)
                {

                    emptyCols.AddRange(GetEmptyCols(allValues, intRangeRows, totalCols));

                }
                else
                {

                    emptyCols = (
                        from e in GetEmptyCols(allValues, intRangeRows, totalCols)
                        join ae in emptyCols on e equals ae
                        select e).ToList();
                }


                emptyRows.AddRange(GetEmptyRows(allValues, intRangeRows, totalCols, intCount));


                if (intRangeRows == 0)
                {
                    break;
                }

                intCount += intRangeRows;

            }

            // now we have a list of the empty rows and columns we need to delete
            int intMaxRow = 0;
            int intMaxCol = 0;

            List<int> lstRows = Enumerable.Range(1, worksheet.UsedRange.Rows.Count).ToList().Except(emptyRows).ToList();
            List<int> lstCols = Enumerable.Range(1, worksheet.UsedRange.Columns.Count).ToList().Except(emptyCols).ToList();


            if (lstRows.Count > 0 && lstCols.Count > 0)
            {

                intMaxRow = lstRows.Max();
                intMaxCol = lstCols.Max();

                MaxRow = intMaxRow;
                MaxCol = intMaxCol;

                strResult = "" + worksheet.Name + "$A1:" + GetExcelColumnName(intMaxCol) + intMaxRow.ToString() + "";

            }

            return strResult;

        }

        private void DeleteEmptyRowsCols(Worksheet v_Worksheet, int v_range = 10000)
        {

            Range targetCells = v_Worksheet.UsedRange;

            currentCleaninngRange = v_range;

            int totalRows = targetCells.Rows.Count;
            int totalCols = targetCells.Columns.Count;

            List<Int32> emptyRows = new List<Int32>();
            List<Int32> emptyCols = new List<Int32>();

            int intCount = 1;

            while (intCount <= totalRows)
            {

                int intRangeRows;

                if (totalRows >= intCount + v_range)
                {

                    intRangeRows = v_range;

                }
                else
                {

                    intRangeRows = totalRows - intCount + 1;

                }

                object[,] allValues = (object[,])targetCells.Range[targetCells.Cells[intCount, 1], targetCells.Cells[intCount + intRangeRows, totalCols]].Cells.Value;

                if (intCount == 1)
                {

                    emptyCols.AddRange(GetEmptyCols(allValues, intRangeRows, totalCols));

                }
                else
                {

                    emptyCols = (from e in GetEmptyCols(allValues, intRangeRows, totalCols)
                                 join ae in emptyCols
                                 on e equals ae
                                 select e).ToList();
                }

                emptyRows.AddRange(GetEmptyRows(allValues, intRangeRows, totalCols, intCount));

                if (intRangeRows == 0)
                    break;

                intCount += intRangeRows;
            }

            if (emptyRows.Count > 0)
                DeleteRows(emptyRows, v_Worksheet);
            if (emptyCols.Count > 0)
                DeleteCols(emptyCols, v_Worksheet);
        }

        private void DeleteEmptyRowsCols2(Worksheet worksheet)
        {
            Range targetCells = worksheet.UsedRange;

            int totalRows = targetCells.Rows.Count;
            int totalCols = targetCells.Columns.Count;
            object[,] allValues = (object[,])targetCells.Cells.Value;

            List<int> emptyRows = new List<int>();
            List<int> emptyCols = new List<int>();

            emptyCols.AddRange(GetEmptyCols(allValues, totalRows, totalCols));
            emptyRows.AddRange(GetEmptyRows(allValues, totalRows, totalCols, 1));

            // now we have a list of the empty rows and columns we need to delete
            DeleteRows(emptyRows, worksheet);
            DeleteCols(emptyCols, worksheet);
        }

        private void DeleteRows(List<int> rowsToDelete, Worksheet worksheet)
        {
            // the rows are sorted high to low - so index's wont shift
            List<int> lst = Enumerable.Range(1, worksheet.UsedRange.Rows.Count).ToList().Except(rowsToDelete).ToList();

            int intMinimum = 0;
            int intMaximum = 0;

            if (lst.Count == 0)
            {
                intMinimum = rowsToDelete.Min();
                intMaximum = rowsToDelete.Max();
            }
            else
            {
                intMinimum = lst.Max() + 1;
                intMaximum = rowsToDelete.Max();
            }

            if (intMaximum > intMinimum)
            {

                int intCurrent = intMaximum;


                while (intCurrent > intMinimum)
                {

                    if (intCurrent - currentCleaninngRange > intMinimum)
                    {

                        intCurrent -= currentCleaninngRange;

                    }
                    else
                    {

                        intCurrent = intMinimum;

                    }

                    worksheet.Range[worksheet.Cells[intCurrent, 1], worksheet.Cells[intMaximum, 1]].EntireRow.Delete(XlDeleteShiftDirection.xlShiftUp);

                    intMaximum = intCurrent;

                }



            }

            foreach (int rowIndex in rowsToDelete.Where((x) => x < intMinimum).OrderByDescending((y) => y))
            {
                worksheet.Rows[rowIndex].Delete();
            }

        }

        private void DeleteCols(List<int> colsToDelete, Worksheet worksheet)
        {
            // the cols are sorted high to low - so index's wont shift
            List<int> lst = Enumerable.Range(1, worksheet.UsedRange.Columns.Count).ToList().Except(colsToDelete).ToList();

            int intMinimum = 0;
            int intMaximum = 0;

            if (lst.Count == 0)
            {
                intMinimum = colsToDelete.Min();
                intMaximum = colsToDelete.Max();
            }
            else
            {
                intMinimum = lst.Max() + 1;
                intMaximum = colsToDelete.Max();
            }


            if (intMaximum > intMinimum)
            {

                int intCurrent = intMaximum;


                while (intCurrent > intMinimum)
                {



                    try
                    {

                        if (intCurrent - currentCleaninngRange > intMinimum)
                        {

                            intCurrent -= currentCleaninngRange;

                        }
                        else
                        {

                            intCurrent = intMinimum;

                        }

                        if (!(worksheet.Range[worksheet.Cells[1, intCurrent], worksheet.Cells[1, intMaximum]].EntireColumn == null))
                        {

                            worksheet.Range[worksheet.Cells[1, intCurrent], worksheet.Cells[1, intMaximum]].EntireColumn.Delete(XlDeleteShiftDirection.xlShiftToLeft);

                        }

                        intMaximum = intCurrent;

                    }
                    catch (Exception ex)
                    {

                        string str = ex.Message;

                    }


                }



            }

            foreach (int colIndex in colsToDelete.Where((x) => x < intMinimum).OrderByDescending((y) => y))
            {
                worksheet.Columns[colIndex].Delete();
            }

        }

        private List<int> GetEmptyRows(object[,] allValues, int totalRows, int totalCols, int currentCounter)
        {
            List<int> emptyRows = new List<int>();

            for (int i = 1; i <= totalRows; i++)
            {

                if (IsRowEmpty(allValues, i, totalCols))
                {
                    emptyRows.Add(i + currentCounter - 1);
                }

            }

            // sort the list from high to low
            return emptyRows.OrderByDescending((x) => x).ToList();
        }

        private List<int> GetEmptyCols(object[,] allValues, int totalRows, int totalCols)
        {
            List<int> emptyCols = new List<int>();

            for (int i = 1; i <= totalCols; i++)
            {
                if (IsColumnEmpty(allValues, i, totalRows))
                {
                    emptyCols.Add(i);
                }
            }
            // sort the list from high to low
            return emptyCols.OrderByDescending((x) => x).ToList();
        }

        private bool IsColumnEmpty(object[,] allValues, int colIndex, int totalRows)
        {

            for (int i = 1; i == totalRows; i++)
            {

                if (allValues[i, colIndex] != null && allValues[i, colIndex].ToString() != "")
                    return false;

            }
            return true;

        }

        private bool IsRowEmpty(object[,] allValues, int rowIndex, int totalCols)
        {

            for (int i = 1; i == totalCols; i++)
            {

                if (allValues[rowIndex, i] != null && allValues[rowIndex, i].ToString() != "")
                    return false;

            }
            return true;
        }

        private void RemoveEmptyTopRowsAndLeftCols(Worksheet worksheet, Range usedRange)
        {
            string addressString = usedRange.Address.ToString();

            if (!(Regex.IsMatch(addressString, "[0-9]")))
            {
                return;
            }

            int rowsToDelete = GetNumberOfTopRowsToDelete(addressString);
            DeleteTopEmptyRows(worksheet, rowsToDelete);
            int colsToDelete = GetNumberOfLeftColsToDelete(addressString);
            DeleteLeftEmptyColumns(worksheet, colsToDelete);

        }

        private void DeleteTopEmptyRows(Worksheet worksheet, int startRow)
        {
            for (int i = 0; i <= startRow - 2; i++)
            {
                worksheet.Rows[1].Delete();
            }
        }

        private void DeleteLeftEmptyColumns(Worksheet worksheet, int colCount)
        {
            for (int i = 0; i <= colCount - 2; i++)
            {
                worksheet.Columns[1].Delete();
            }
        }

        private int GetNumberOfTopRowsToDelete(string address)
        {
            string[] splitArray = address.Split(':');
            string firstIndex = splitArray[0];
            splitArray = firstIndex.Split('$');
            string value = splitArray[2];
            int returnValue = -1;
            if ((int.TryParse(value, out returnValue)) && (returnValue >= 0))
            {
                return returnValue;
            }
            return returnValue;
        }

        private int GetNumberOfLeftColsToDelete(string address)
        {
            string[] splitArray = address.Split(':');
            string firstindex = splitArray[0];
            splitArray = firstindex.Split('$');
            string value = splitArray[1];

            return ParseColHeaderToIndex(value);
        }

        private int ParseColHeaderToIndex(string colAdress)
        {
            int[] digits = new int[colAdress.Length];
            for (int i = 0; i < colAdress.Length; i++)
            {
                digits[i] = Convert.ToInt32(colAdress[i]) - 64;
            }
            int mul = 1;
            int res = 0;
            for (int pos = digits.Length - 1; pos >= 0; pos--)
            {
                res += digits[pos] * mul;
                mul *= 26;
            }
            return res;
        }
        #endregion

        #region declarations

        private string m_strFolderPath = string.Empty;
        private ExcelCleanOperationType m_ExcelCleanOperationType = ExcelCleanOperationType.GetUsedRange;
        private Application m_XlApp;
        private int m_CleanRange = 0;
        private int currentCleaninngRange = 0;

        #endregion

        #region Methods

        public void RemoveEmptyRowsAndColumns(ref Worksheet m_XlWrkSheet, int range)
        {

            Range usedRange = m_XlWrkSheet.UsedRange;

            RemoveEmptyTopRowsAndLeftCols(m_XlWrkSheet, usedRange);

            DeleteEmptyRowsCols(m_XlWrkSheet, ((range == 0) ? 10000 : range));

        }

        public string GetUsedRangeString(ref Worksheet m_XlWrkSheet, ref int MaxRow, ref int MaxCol, int range)
        {

            Range usedRange = m_XlWrkSheet.UsedRange;

            if (m_ExcelCleanOperationType == ExcelCleanOperationType.RemoveTopEmptyAndGetUsedRange)
                RemoveEmptyTopRowsAndLeftCols(m_XlWrkSheet, usedRange);

            return GetSheetRelatedString(m_XlWrkSheet, ref MaxRow, ref MaxCol, ((range == 0) ? 10000 : range));

        }

        private string GetExcelColumnName(int columnNumber)
        {

            int dividend = columnNumber;
            string columnName = string.Empty;
            int modulo = 0;

            while (dividend > 0)
            {
                modulo = (dividend - 1) % 26;
                columnName = Convert.ToChar(65 + modulo).ToString() + columnName;
                dividend = Convert.ToInt32((dividend - modulo) / 26.0);
            }

            return columnName;

        }

        public bool IsWorkbookProtected(Workbook xlWbs)
        {

            try
            {

                xlWbs.Unprotect(Password: "'");

                return false;

            }
            catch (Exception ex)
            {

                return true;

            }

        }

        public bool IsWorksheetProtected(Worksheet xlWks)
        {

            //If xlWks.ProtectContents Then
            //
            //End If

            try
            {

                xlWks.Unprotect(Password: "'");


                return false;

            }
            catch (Exception ex)
            {

                return true;

            }

        }

        public string CleanExcel(string strPath)
        {

            Exception ExFixExcel = null;
            string strResult = string.Empty;

            Workbook xlWbs = null;

            try
            {
                xlWbs = m_XlApp.Workbooks.Open(strPath, Type.Missing, Type.Missing, Type.Missing, "'", Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing);

                xlWbs.DoNotPromptForConvert = true;
                xlWbs.CheckCompatibility = false;
                xlWbs.Application.DisplayAlerts = false;

                if (IsWorkbookProtected(xlWbs))
                {
                    throw new Exception("Workbook is protected");
                }

                for (int idx = 0; idx == xlWbs.Worksheets.Count - 1; idx++)
                {
                    Worksheet m_XlWrkSheet = xlWbs.Worksheets[idx];

                    if (IsWorksheetProtected(m_XlWrkSheet))
                    {
                        continue;
                    }

                    m_XlWrkSheet.UsedRange.UnMerge();


                    if (m_ExcelCleanOperationType != ExcelCleanOperationType.CleanExcel)
                    {

                        int MaxRow = 1;
                        int MaxCol = 1;


                        strResult += GetUsedRangeString(ref m_XlWrkSheet, ref MaxRow, ref MaxCol, m_CleanRange) + "|";

                    }
                    else
                    {

                        RemoveEmptyRowsAndColumns(ref m_XlWrkSheet, m_CleanRange);


                    }


                }

                xlWbs.Save();
                xlWbs.Close(true, Type.Missing, Type.Missing);
                m_XlApp.Quit();
                System.Runtime.InteropServices.Marshal.ReleaseComObject(xlWbs);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(m_XlApp);
            }
            catch (Exception ex)
            {
                throw ExFixExcel;
            }

            return strResult.TrimEnd('|');

        }

        #endregion

        #region constructors

        public MsExcelCleaner(int v_Range = 10000, ExcelCleanOperationType ExcelCleanOperationType = ExcelCleanOperationType.GetUsedRange)
        {

            m_CleanRange = v_Range;
            currentCleaninngRange = v_Range;
            m_ExcelCleanOperationType = ExcelCleanOperationType;

            m_XlApp = new Application();
            m_XlApp.Visible = false;
            m_XlApp.DisplayAlerts = false;

        }

        #endregion

        public void Dispose()
        {

        }
    }
}
