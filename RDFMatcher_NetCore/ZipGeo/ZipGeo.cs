using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Maps;
using Google.Maps.Geocoding;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  struct Coordinate
  {
    public float lat, lng;
  }

  class ZipGeo
  {
    private static Coordinate? MatchGoogle(string zip)
    {
      var request = new GeocodingRequest();
      request.Address = $"{zip}, Polen";
      var response = new GeocodingService().GetResponse(request);

      if (response.Status == ServiceResponseStatus.Ok && response.Results.Count() == 1)
      {
        var firstResult = response.Results.First();
        var result = new Coordinate
        {
          lat = (float) firstResult.Geometry.Location.Latitude,
          lng = (float) firstResult.Geometry.Location.Longitude
        };
        return result;
      }
      else
      {
        return null;
      }
    }

    public static void DoZipGeo(string zip)
    {
      var coordinates = new List<Coordinate>();
      var coordinateReader = MySqlHelper.ExecuteReader(DB.ConnectionString,
        "SELECT addr.LEFT_POSTAL_CODE, pt.LAT, pt.LNG " +
        "FROM pol_rdf_addr addr " +
        " LEFT JOIN pol_rdf_point pt USING (ROAD_LINK_ID) " +
        $"WHERE addr.LEFT_POSTAL_CODE = '{zip}' " +
        " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL ");

      while (coordinateReader.Read())
      {
        string latString = coordinateReader.GetString("LAT").Insert(2, ",");
        string lonString = coordinateReader.GetString("LNG").Insert(2, ",");
        coordinates.Add(new Coordinate
        {
          lat = float.Parse(latString),
          lng = float.Parse(lonString)
        });
      }

      coordinateReader.Close();

      bool matched = false;
      float midLat = -1.0f, midLng = -1.0f;

      if (coordinates.Count > 0)
      {
        midLat = coordinates.Sum(c => c.lat) / coordinates.Count;
        midLng = coordinates.Sum(c => c.lng) / coordinates.Count;
        matched = true;
      }
      else
      {
        Coordinate? googleCoordinate = MatchGoogle(zip);

        if (googleCoordinate != null)
        {
          midLat = googleCoordinate.Value.lat;
          midLng = googleCoordinate.Value.lng;
          matched = true;
        }
      }

      if (!matched)
      {
        Console.WriteLine($"Could not match {zip}");
        return;
      }

      const string latFormat = "00.00000";
      const string lngFormat = "0.00000";

      var insertString =
        "INSERT INTO zip_koo " +
        $"VALUES ('{zip}', " +
        $"{Utils.FloatToStringInvariantCulture(midLat, latFormat)}, " +
        $"{Utils.FloatToStringInvariantCulture(midLng, lngFormat)})";

      MySqlHelper.ExecuteNonQuery(DB.ConnectionString, insertString);
    }

    public static void DoZipGeo()
    {
      GoogleSigned.AssignAllServices(new GoogleSigned("AIzaSyCzOcXgCQ_1Nng6shWR9FRS2tRFBItyG0E"));

      var districtCodeReader = MySqlHelper.ExecuteReader(DB.ConnectionString, 
        "SELECT DISTINCT ZIP " +
        "FROM street_zip " +
        "WHERE ZIP NOT IN (SELECT DISTRICT_CODE FROM ZIP_KOO)");

      var taskList = new List<Task>();

      while (districtCodeReader.Read())
      {
        var zip = districtCodeReader.GetString("ZIP");
        DoZipGeo(zip);
      }

      Task.WaitAll(taskList.ToArray());

      districtCodeReader.Close();
    }
  }
}
