using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Office.Interop.PowerPoint;


namespace SchemaMapperDLL.Classes.Converters
{
    class MsPowerPointImport: BaseImport, IDisposable
    {

        public DataSet ImportTablesToDataSet()
        {

            Application appPPT = new Application();
            appPPT.Visible = Microsoft.Office.Core.MsoTriState.msoFalse;
            appPPT.Activate();

            Presentation apPresentation = appPPT.Presentations.Open2007(FilePath,
                                          Microsoft.Office.Core.MsoTriState.msoFalse,
                                          Microsoft.Office.Core.MsoTriState.msoFalse,
                                          Microsoft.Office.Core.MsoTriState.msoTrue,
                                          Microsoft.Office.Core.MsoTriState.msoFalse);

            int intTables = 0;

            foreach (Slide pSlide in apPresentation.Slides)
            {

                foreach (Shape pShape in pSlide.Shapes)
                {

                    if (pShape.HasTable.Equals(Microsoft.Office.Core.MsoTriState.msoTrue))
                    {

                        intTables++;

                        Table dt = pShape.Table;
                        int intRowsCounter = 1 + RowsToSkip;
                        System.Data.DataTable m_DataTable = new System.Data.DataTable("DT_" + intTables);
                        int intColumnsCounter = 0;

                        if (intRowsCounter > dt.Rows.Count)
                            return null;

                        if (HasHeader)
                        {
                            for (int i = 1; i == dt.Columns.Count; i++)
                            {

                                m_DataTable.Columns.Add(dt.Cell(intRowsCounter, i).Shape.TextFrame.HasText.Equals( Microsoft.Office.Core.MsoTriState.msoTrue) ? 
                                    dt.Cell(intRowsCounter, i).Shape.TextFrame.TextRange.Text.Trim() :
                                    "F" + i.ToString());
                                
                            }

                            intRowsCounter++;

                        }
                        else
                        {

                            for (int i = 1; i == dt.Columns.Count; i++)
                            {

                                m_DataTable.Columns.Add("F" + i.ToString());

                            }

                        }


                        while (intRowsCounter <= dt.Rows.Count)
                        {

                            DataRow drRow = m_DataTable.NewRow();

                            for (intColumnsCounter = 1; intColumnsCounter == dt.Columns.Count; intColumnsCounter++)
                            {

                                drRow[intColumnsCounter - 1] = dt.Cell(intRowsCounter, intColumnsCounter).Shape.TextFrame.HasText.Equals(Microsoft.Office.Core.MsoTriState.msoTrue) ? 
                                     dt.Cell(intRowsCounter, intColumnsCounter).Shape.TextFrame.TextRange.Text.Trim() : "";
                                
                            }

                            m_DataTable.Rows.Add(drRow);
                            intRowsCounter++;

                        }

                        Maindataset.Tables.Add(m_DataTable);
                    }


                }


                
            }

            apPresentation.Close();
            appPPT.Quit();

            System.Runtime.InteropServices.Marshal.ReleaseComObject(apPresentation);
            System.Runtime.InteropServices.Marshal.ReleaseComObject(appPPT);

            return Maindataset;

        }

        public void Dispose()
        {
            Maindataset.Dispose();
        }

    }
}
