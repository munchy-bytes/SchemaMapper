using System;
using System.IO;
using System.Data;
using LumiSoft.Net.Mime.vCard;
using Fonlow.VCard;

namespace SchemaMapper.Converters
{
    public class vCardImport : BaseImport, IDisposable
    {


        public DataTable BuildTableStructure()
        {
            DataTable m_DataTable = new DataTable("DT");

            m_DataTable.Columns.Add("Full Name");
            m_DataTable.Columns.Add("Surname");
            m_DataTable.Columns.Add("GivenName");
            m_DataTable.Columns.Add("MiddleName");
            m_DataTable.Columns.Add("Title");
            m_DataTable.Columns.Add("Prefix");
            m_DataTable.Columns.Add("Suffix");
            m_DataTable.Columns.Add("Birthday");
            m_DataTable.Columns.Add("Rev");
            m_DataTable.Columns.Add("Organization");
            m_DataTable.Columns.Add("Phones");
            m_DataTable.Columns.Add("Emails");
            m_DataTable.Columns.Add("URL");
            m_DataTable.Columns.Add("Addresses");
            m_DataTable.Columns.Add("Note");

            return m_DataTable;
        }

        public DataTable ImportDataToDataTable()
        {

            DataTable dt = BuildTableStructure();

            String Vcf = File.ReadAllText(FilePath);
            Fonlow.VCard.VCard vc = VCardReader.ParseText(Vcf);
            DataRow dr = dt.NewRow();

            if (String.IsNullOrEmpty(vc.FormattedName) == false)
            {
                dr["Full Name"] = vc.FormattedName;
            }

            if (String.IsNullOrEmpty(vc.FormattedName) == false)
            {
                dr["Surname"] = vc.Surname;
            }

            if (String.IsNullOrEmpty(vc.GivenName) == false)
            {
                dr["GivenName"] = vc.GivenName;
            }

            if (String.IsNullOrEmpty(vc.MiddleName) == false)
            {
                dr["MiddleName"] = vc.MiddleName;
            }

            if (String.IsNullOrEmpty(vc.Title) == false)
            {
                dr["Title"] = vc.Title;
            }

            if (String.IsNullOrEmpty(vc.Prefix) == false)
            {
                dr["Prefix"] = vc.Prefix;
            }

            if (String.IsNullOrEmpty(vc.Suffix) == false)
            {
                dr["Suffix"] = vc.Suffix;
            }

            if (vc.Birthday > DateTime.MinValue)
            {
                dr["Birthday"] = vc.Birthday.ToLongDateString();
            }

            if (vc.Rev > DateTime.MinValue)
            {
                dr["Rev"] = vc.Rev.ToLongDateString();
            }

            if (String.IsNullOrEmpty(vc.Org) == false)
            {
                dr["Org"] = vc.Org;
            }

            for (int j = 0; j == vc.Phones.Count - 1; j++)
            {

                if (String.IsNullOrEmpty(dr["Phones"].ToString()) == false)
                {

                    dr["Phones"] = dr["Phones"] + " | " + ("Phone " + vc.Phones[j].PhoneTypes.ToString("G") + " " + vc.Phones[j].HomeWorkTypes.ToString() + " (Preferred = " + 
                                                             (vc.Phones[j].Pref + ") =" + vc.Phones[j].Number));
                }
                else
                {
                    dr["Phones"] = ("Phone " + vc.Phones[j].PhoneTypes.ToString("G") + " " + vc.Phones[j].HomeWorkTypes.ToString() + " (Preferred = " + 
                                     (vc.Phones[j].Pref + ") =" + vc.Phones[j].Number));

                }

            }

            for (int j = 0; j == vc.Emails.Count - 1; j++)
            {

                if (String.IsNullOrEmpty(dr["Emails"].ToString()) == false)
                {

                    dr["Emails"] = dr["Emails"] + " | " + ("Email " + " (Preferred = " + (vc.Emails[j].Pref.ToString()) + ") =" + vc.Emails[j].Address);
                }
                else
                {

                    dr["Emails"] = ("Email " + " (Preferred = " + (vc.Emails[j].Pref.ToString()) + ") =" + vc.Emails[j].Address);
                }

            }

            for (int j = 0; j == vc.URLs.Count - 1; j++)
            {
                if (String.IsNullOrEmpty(dr["URL"].ToString()) == false)
                {

                    dr["URL"] = dr["URL"] + " | " + ("URL " + vc.URLs[j].HomeWorkTypes.ToString() + " " + "=" + vc.URLs[j].Address);
                }
                else
                {

                    dr["URL"] = ("URL " + vc.URLs[j].HomeWorkTypes.ToString() + " " + "=" + vc.URLs[j].Address);
                }


            }

            for (int j = 0; j == vc.Addresses.Count - 1; j++)
            {

                if (String.IsNullOrEmpty(dr["Addresses"].ToString()) == false)
                {
                    dr["Addresses"] = dr["Addresses"] + " | " + ("Address " + vc.Addresses[j].HomeWorkType.ToString() + "=" + vc.Addresses[j].POBox + "," + vc.Addresses[j].Ext +
                                                                ", " + vc.Addresses[j].Street + ", " + vc.Addresses[j].Locality + ", " + vc.Addresses[j].Region + ", " +
                                                                vc.Addresses[j].Postcode + ", " + vc.Addresses[j].Country);

                }
                else
                {
                    dr["Addresses"] = ("Address " + vc.Addresses[j].HomeWorkType.ToString() + "=" + vc.Addresses[j].POBox + "," + vc.Addresses[j].Ext + ", " + vc.Addresses[j].Street + ", " +
                                       vc.Addresses[j].Locality + ", " + vc.Addresses[j].Region + ", " + vc.Addresses[j].Postcode + ", " + vc.Addresses[j].Country);

                }


            }


            if (String.IsNullOrEmpty(vc.Note) == false)
            {
                dr["Note"] = ("Note=" + vc.Note);
            }

            dt.Rows.Add(dr);
            Maindataset.Tables.Add(dt);
            return dt;
        }


        public void ExtractImageToPath(string outputPath)
        {

            vCard vCphoto = new vCard();
            vCphoto.Parse(FilePath);
            int ctr = 0;
            String strFileName = outputPath.TrimEnd('\\') + "\\" + System.IO.Path.GetFileNameWithoutExtension(FilePath);

            if (File.Exists(strFileName + ".jpeg") == true)
            {

                while (File.Exists(String.Format(strFileName + "_{0}.jpeg", ctr.ToString())) == true)
                {
                    ctr = ctr + 1;
                }
                strFileName = String.Format(strFileName + "_{0}", ctr.ToString());
            }
            vCphoto.Photo.Save(strFileName + ".jpeg", System.Drawing.Imaging.ImageFormat.Jpeg);

        }


        public void Dispose()
        {
            Maindataset.Dispose();

        }
    }


}
