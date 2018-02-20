using DatabaseLibrary;
using DatabaseLibrary.Utilities;
using Google.Maps;
using Google.Maps.Geocoding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ZipKoo
{
  class Program
  {
    private static Progress _progress = new Progress();

    private static string _googleSearchCountry = "Russland";

    static void Main(string[] args)
    {
      GlobalLibraryState.Init("ZipKoo", "Marcel", "YyQzKeSSX0TlgsI4", "RUS");
      GoogleSigned.AssignAllServices(new GoogleSigned("AIzaSyCzOcXgCQ_1Nng6shWR9FRS2tRFBItyG0E"));

      var taskList = new List<Task>();

      var zipReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT DISTINCT ZIP " +
        "FROM street_zip " +
        "WHERE ZIP <> '' " +
        "AND ZIP NOT IN (SELECT ZIP FROM zip_koo);");

      using (zipReader)
      {
        while (zipReader.Read())
        {
          string zip = zipReader.GetString("ZIP");
          taskList.Add(Task.Run(() => AddZipCoordinate(zip)));
        }
      }

      var whenAll = Task.WhenAll(taskList);
      while (!whenAll.IsCompleted)
      {
        _progress.PrintProgress();
        Thread.Sleep(1000);
        Log.Flush();
      }
    }

    private static void AddZipCoordinate(string zip)
    {
      _progress.IncrementItemsDone();

      var coordinates = GetCoordinateRdf(zip);
      if (coordinates != null)
      {
        Log.WriteLine($"Matched {zip} from RDF data");
      }
      else
      {
        // coordinates = GetCoordinatesGoogle(zip);
        if (coordinates != null)
        {
          Log.WriteLine($"Matched {zip} from Google data");
        }
      }

      if (coordinates == null)
      {
        Log.WriteLine($"Could not match {zip}");
        return;
      }

      const string latFormat = "00.00000";
      const string lngFormat = "00.00000";
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "INSERT INTO zip_koo VALUES(@1, @2, @3);",
        zip,
        Utils.DoubleToStringInvariantCulture(coordinates.Lat, latFormat),
        Utils.DoubleToStringInvariantCulture(coordinates.Lng, lngFormat));

      _progress.IncrementItemsSuccessful();
    }

    private static Coordinates<double> GetCoordinateRdf(string zip)
    {
      var coordinates = new List<Coordinates<double>>();

      var coordinateReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT seg.LAT, seg.LON " +
        $"FROM {GlobalLibraryState.RdfAddrTable} addr " +
        $" JOIN {GlobalLibraryState.RdfSegTable} seg USING (ROAD_LINK_ID) " +
        "WHERE addr.LEFT_POSTAL_CODE = @1;",
        zip);

      using (coordinateReader)
      {
        while (coordinateReader.Read())
        {
          var latString = coordinateReader.GetInt64("LAT").ToString();
          var lonString = coordinateReader.GetInt64("LON").ToString();
          latString = Utils.RdfCoordinateInsertDecimal(latString);
          lonString = Utils.RdfCoordinateInsertDecimal(lonString);

          coordinates.Add(new Coordinates<double>
          {
            Lat = Utils.ParseDoubleInvariantCulture(latString),
            Lng = Utils.ParseDoubleInvariantCulture(lonString)
          });
        }
      }

      if (coordinates.Count == 0)
        return null;

      double midLat = coordinates.Average(c => c.Lat);
      double midLng = coordinates.Average(c => c.Lng);

      return new Coordinates<double>
      {
        Lat = midLat,
        Lng = midLng
      };
    }

    
    private static Coordinates<double> GetCoordinatesGoogle(string zip)
    {
      if (_googleSearchCountry.Length == 0)
        throw new ArgumentException("Country is empty!", nameof(_googleSearchCountry));

      var request = new GeocodingRequest();
      request.Address = $"{zip}, {_googleSearchCountry}";

      GeocodingService geocodingService = new GeocodingService();
      var response = geocodingService.GetResponse(request);

      if (response.Status != ServiceResponseStatus.Ok)
        return null;

      if (response.Results.Length != 1)
        return null;

      var firstResult = response.Results[0];
      var result = new Coordinates<double>
      {
        Lat = firstResult.Geometry.Location.Latitude,
        Lng = firstResult.Geometry.Location.Longitude
      };
      return result;
    }
  }
}
