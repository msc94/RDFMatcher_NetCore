using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  struct Coordinate
  {
    public float lat, lng;
  }
  class ZipGeo
  {
    public static void DoZipGeo(string districtCode)
    {
      var coordinates = new List<Coordinate>();
      var coordinateReader = MySqlHelper.ExecuteReader(DB.ConnectionString,
        "SELECT addr.LEFT_DISTRICT_CODE, pt.LAT, pt.LNG " +
        "FROM NLD_RDF_ADDR addr " +
        " LEFT JOIN NLD_RDF_POINT pt USING (ROAD_LINK_ID) " +
        $"WHERE addr.LEFT_DISTRICT_CODE = {districtCode} " +
        " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL ");

      while (coordinateReader.Read())
      {
        string latString = coordinateReader.GetString("LAT").Insert(2, ",");
        string lonString = coordinateReader.GetString("LNG").Insert(1, ",");
        coordinates.Add(new Coordinate
        {
          lat = float.Parse(latString),
          lng = float.Parse(lonString)
        });
      }

      coordinateReader.Close();

      if (coordinates.Count == 0)
      {
        Console.WriteLine($"District code {districtCode} has no coordinates!");
        return;
      }

      float midLat = coordinates.Sum(c => c.lat) / coordinates.Count;
      float midLng = coordinates.Sum(c => c.lng) / coordinates.Count;

      const string latFormat = "00.00000";
      const string lngFormat = "0.00000";

      var insertString =
        "INSERT INTO district_code_koo1 " +
        $"VALUES ({districtCode}, {midLat.ToString(latFormat).Replace(',', '.')}, {midLng.ToString(lngFormat).Replace(',', '.')})";

      MySqlHelper.ExecuteNonQuery(DB.ConnectionString, insertString);
    }
    public static void DoZipGeo()
    {
      var districtCodeReader = MySqlHelper.ExecuteReader(DB.ConnectionString, 
        "SELECT DISTINCT DISTRICT_CODE FROM street_zip");

      while (districtCodeReader.Read())
      {
        var districtCode = districtCodeReader.GetString("DISTRICT_CODE");
        DoZipGeo(districtCode);
      }

      districtCodeReader.Close();
    }
  }
}
