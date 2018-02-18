using DatabaseLibrary;
using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ZoneKoo
{
  class Zone
  {
    public long Id;

    public string Level4ZoneName;
    public string Level3ZoneName;
    public string Level2ZoneName;

    public string LocalityName;
    public string DepartmentName;
  }
  class Program
  {
    private static Progress _progress = new Progress();

    static void Main(string[] args)
    {
      GlobalLibraryState.Init("ZoneKoo", "Marcel", "YyQzKeSSX0TlgsI4", "RUS");

      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString, "TRUNCATE TABLE zone_koo;");

      var taskList = new List<Task>();

      var zoneReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT z4.ID as Z4_ID, z4a.ZONE_NAME AS Z4_NAME, z3a.ZONE_NAME AS Z3_NAME, z2a.ZONE_NAME AS Z2_NAME " +
        "FROM zone z4 " +
        "  LEFT JOIN zone z4a ON (z4a.ZONE_MASTER_ID = z4.ID) " +
        "  LEFT JOIN zone z3 ON (z4.LEVEL_3_ZONE_ID = z3.ID) " +
        "  LEFT JOIN zone z3a ON (z3a.ZONE_MASTER_ID = z3.ID) " +
        "  LEFT JOIN zone z2 ON (z3.LEVEL_2_ZONE_ID = z2.ID) " +
        "  LEFT JOIN zone z2a ON (z2a.ZONE_MASTER_ID = z2.ID) " +
        "WHERE z4.LEVEL = 4;");

      using (zoneReader)
      {
        while (zoneReader.Read())
        {
          var zone = new Zone
          {
            Id = zoneReader.GetInt64("Z4_ID"),
            Level4ZoneName = zoneReader.GetString("Z4_NAME"),
            Level3ZoneName = zoneReader.GetString("Z3_NAME"),
            Level2ZoneName = zoneReader.GetString("Z2_NAME")
          };
          taskList.Add(Task.Run(() => AddZoneCoordinate(zone)));
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

    private static void AddZoneCoordinate(Zone zone)
    {
      _progress.IncrementItemsDone();

      zone.LocalityName = zone.Level4ZoneName;
      zone.DepartmentName = zone.Level3ZoneName.Length == 0 ? zone.Level2ZoneName : zone.Level3ZoneName;

      if (zone.LocalityName.Length == 0 ||
        zone.DepartmentName.Length == 0)
      {
        Log.WriteLine($"Could not match {zone.LocalityName}, {zone.DepartmentName}");
        return;
      }

      var coordinates = GetCoordinateRdf(zone);

      if (coordinates == null)
      {
        Log.WriteLine($"Could not match {zone.LocalityName}, {zone.DepartmentName}");
        return;
      }

      const string latFormat = "00.00000";
      const string lngFormat = "00.00000";
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "INSERT INTO zone_koo VALUES(@1, @2, @3);",
        zone.Id,
        Utils.DoubleToStringInvariantCulture(coordinates.Lat, latFormat),
        Utils.DoubleToStringInvariantCulture(coordinates.Lng, lngFormat));

      _progress.IncrementItemsSuccessful();
    }

    private static Coordinates<double> GetCoordinateRdf(Zone zone)
    {
      var coordinates = new List<Coordinates<double>>();

      var departmentName = '%' + zone.DepartmentName + '%';
      var coordinateReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT seg.LAT, seg.LON " +
        $"FROM {GlobalLibraryState.RdfAddrTable} addr " +
        $" JOIN {GlobalLibraryState.RdfSegTable} seg USING (ROAD_LINK_ID) " +
        "WHERE addr.LEFT_LOCALITY_NAME = @1 " +
        " AND addr.LEFT_DEPARTMENT_NAME LIKE @2;",
        zone.LocalityName, departmentName);

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
  }
}
