using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class MatchItem
  {
    public object street_zip_id;
    public string zip;
    public string street_name;
  }

  class InsertItem : IInsertBufferItem
  {
    public object street_zip_id;
    public string hno;
    public string hno_extension;
    public object ap_lat;
    public object ap_lng;

    public void Insert()
    {

    }
  }


  class MatchThread
  {
    private const string queryCommandText = "SELECT addr.ROAD_LINK_ID, pt.ADDRESS, pt.LAT, pt.LNG " +
                                            "FROM POL_RDF_ADDR addr " +
                                            " LEFT JOIN POL_RDF_POINT pt using (ROAD_LINK_ID) " +
                                            "WHERE addr.LEFT_POSTAL_CODE = @1 AND addr.STREET_BASE_NAME = @2 " +
                                            " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL ";

    private const string insertCommandText = "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, AP_LAT, AP_LNG) VALUES(@1, @2, @3, @4, @5)";

    private InsertBuffer _insertBuffer;


    private void Match(MatchItem item)
    {
      var zip = item.zip;
      var street_name = item.street_name;

      _addrQueryCommand.Parameters[0].Value = zip;
      _addrQueryCommand.Parameters[1].Value = street_name;
      var addrReader = _addrQueryCommand.ExecuteReader();

      var numMatches = 0;

      while (addrReader.Read())
      {
        numMatches++;
        SplitAddress(addrReader.GetString("ADDRESS"), out var hno, out var hno_extension);
        var newItem = new InsertItem
        {
          street_zip_id = item.street_zip_id,
          hno = hno,
          hno_extension = hno_extension,
          ap_lat = addrReader.GetString("LAT").Insert(2, "."),
          ap_lng = addrReader.GetString("LNG").Insert(2, ".")
        };
        _insertBuffer.Insert(newItem);
      }

      addrReader.Close();

      if (numMatches == 0)
      {
        // Console.WriteLine($"No match for {item.street_name}, {item.zip}");
      }
    }

    private void SplitAddress(string address, out string hno, out string hno_extension)
    {
      for (int i = 0; i < address.Length; i++)
      {
        if (char.IsLetter(address[i])
          || address[i] == '/'
          || address[i] == '?')
        {
          hno = address.Substring(0, i);
          hno_extension = address.Substring(i);
          return;
        }
      }
      hno = address;
      hno_extension = "";
    }
  }
}

