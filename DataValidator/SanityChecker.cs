using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataValidator
{
  class SanityChecker
  {
    private string _connectionString;

    public SanityChecker(string connectionString)
    {
      _connectionString = connectionString;
    }

    public string RunTests()
    {
      var taskList = new List<Task<string>>();

      taskList.Add(Task.Run(() => TestBuildingSize()));
      taskList.Add(Task.Run(() => TestDuplicates()));
      taskList.Add(Task.Run(() => TestMatchedBuildings()));
      taskList.Add(Task.Run(() => TestBuildingStructure()));
      taskList.Add(Task.Run(() => TestStreetSegStructure()));
      taskList.Add(Task.Run(() => TestStreetSeg()));
      taskList.Add(Task.Run(() => TestCoordinates()));

      var results = taskList.Select(t => t.Result);
      return string.Join(Environment.NewLine, results);
    }

    private string TestMatchedBuildings()
    {
      long numberOfBuildingsWithWrongCoordinates =
        (long)MySqlHelper.ExecuteScalar(_connectionString, 
        "SELECT COUNT(*)\n" +
        "FROM building b\n" +
        "WHERE (b.AP_LAT IS NOT NULL OR b.AP_LNG IS NOT NULL)\n" +
        "AND b.ID NOT IN (SELECT BUILDING_ID FROM match_building);");

      if (numberOfBuildingsWithWrongCoordinates > 0)
        return "[!] There are buildings without a match that have a coordinate.";
      else
        return "There are no buildings without a match that have a coordinate.";
    }

    const string _buildingSize = "SELECT COUNT(*) FROM building;";
    const string _buildingWithCoordinateSize = "SELECT COUNT(*) FROM building WHERE AP_LAT IS NOT NULL OR AP_LNG IS NOT NULL;";

    private string TestBuildingSize()
    {
      long numberOfBuildings = (long)MySqlHelper.ExecuteScalar(_connectionString, _buildingSize);
      long numberOfBuildingsWithCoordinate = (long)MySqlHelper.ExecuteScalar(_connectionString, _buildingWithCoordinateSize);

      double matchedPercentage = numberOfBuildingsWithCoordinate / (double)numberOfBuildings * 100.0;

      return  
        $"There are {numberOfBuildings} buildings in the table. Please check input file size." + Environment.NewLine +
        $"There are {numberOfBuildingsWithCoordinate} buildings with a coordiante. {matchedPercentage.ToString("00.00")}% matched.";
    }

    const string _duplicates = "SELECT COUNT(*) FROM " +
      "(SELECT COUNT(*), STREET_ZIP_ID, HNO, HNO_EXTENSION " +
      "FROM building " +
      "GROUP BY STREET_ZIP_ID, HNO, HNO_EXTENSION " +
      "HAVING COUNT(*) > 1) as duplicates;";

    private string TestDuplicates()
    {
      long numberOfDuplicates = (long)MySqlHelper.ExecuteScalar(_connectionString, _duplicates);

      if (numberOfDuplicates > 0)
      {
        return $"[!] There are {numberOfDuplicates} duplicates in the building table.";
      }
      else
      {
        return "There are no duplicates in the building table";
      }
    }


    const string _buildingViewSize =
      "SELECT COUNT(*)\n" +
      "FROM zone z1\n" +
      " JOIN zone z2 ON (z2.LEVEL_1_ZONE_ID = z1.ID)\n" +
      " JOIN street s ON (s.ZONE_ID = z2.ID)\n" +
      " JOIN street_zip sz ON (sz.STREET_ID = s.ID)\n" +
      " JOIN building b ON (b.STREET_ZIP_ID = sz.ID)\n";

    private string TestBuildingStructure()
    {
      long numberOfBuildings = (long)MySqlHelper.ExecuteScalar(_connectionString, _buildingSize);
      long numberOfBuildingsInView = (long)MySqlHelper.ExecuteScalar(_connectionString, _buildingViewSize);

      return $"{numberOfBuildingsInView} in building view. {numberOfBuildings} in building table.";
    }

    const string _streetSegKooSize = "SELECT COUNT(*) FROM street_seg_koo_group;";
    const string _streetSegKooViewSize =
      "SELECT COUNT(*)\n" +
      "FROM zone z1\n" +
      " JOIN zone z2 ON (z2.LEVEL_1_ZONE_ID = z1.ID)\n" +
      " JOIN street s ON (s.ZONE_ID = z2.ID)\n" +
      " JOIN street_zip sz ON (sz.STREET_ID = s.ID)\n" +
      " JOIN street_seg ss ON (ss.STREET_ZIP_ID = sz.ID)\n" +
      " JOIN street_seg_koo_group sskg ON (sskg.STREET_SEG_ID = ss.ID)" +
      "WHERE z1.LEVEL = 1 AND z2.LEVEL = 2\n" +
      "AND sskg.COORDINATES IS NOT NULL;";

    private string TestStreetSegStructure()
    {
      long numberOfStreetSegKoo = (long)MySqlHelper.ExecuteScalar(_connectionString, _streetSegKooSize);
      long numberOfStreetSegKooInView = (long)MySqlHelper.ExecuteScalar(_connectionString, _streetSegKooSize);

      return $"{numberOfStreetSegKooInView} in StreetSegKoo view. {numberOfStreetSegKoo} in StreetSegKoo table.";
    }

    private string TestStreetSeg()
    {
      long numberOfSegmentsWithWrongCoordinate =
        (long)MySqlHelper.ExecuteScalar(_connectionString,
        "SELECT COUNT(*)\n" +
        "FROM street_seg ss\n" +
        "WHERE (ss.CENTER_LAT IS NOT NULL OR ss.CENTER_LNG IS NOT NULL)\n" +
        "AND ss.ID NOT IN (SELECT STREET_SEG_ID FROM street_seg_koo);");

      if (numberOfSegmentsWithWrongCoordinate > 0)
        return "[!] There are segments with a coordinate that have no interpolated coordinates.";
      else
        return "There are no segments with a coordinate that have no interpolated coordinates.";
    }

    private void ReadMinMax(MySqlDataReader reader, ref double minLat, ref double maxLat, ref double minLng, ref double maxLng)
    {
      using (reader)
      {
        reader.Read();

        minLat = Math.Min(minLat, reader.GetDouble("LAT_MIN"));
        maxLat = Math.Max(maxLat, reader.GetDouble("LAT_MAX"));

        minLng = Math.Min(minLng, reader.GetDouble("LNG_MIN"));
        maxLng = Math.Max(maxLng, reader.GetDouble("LNG_MAX"));
      }
    }

    private string TestCoordinates()
    {
      double minLat = double.MaxValue, maxLat = double.MinValue;
      double minLng = double.MaxValue, maxLng = double.MinValue;

      var reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT " +
        "MAX(CAST(AP_LAT as DOUBLE)) as LAT_MAX, " +
        "MIN(CAST(AP_LAT as DOUBLE)) as LAT_MIN, " +
        "MAX(CAST(AP_LNG as DOUBLE)) as LNG_MAX, " +
        "MIN(CAST(AP_LNG as DOUBLE)) as LNG_MIN " +
        "FROM building;");

      ReadMinMax(reader, ref minLat, ref maxLat, ref minLng, ref maxLng);

      reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT " +
        "MAX(CAST(CENTER_LAT as DOUBLE)) as LAT_MAX, " +
        "MIN(CAST(CENTER_LAT as DOUBLE)) as LAT_MIN, " +
        "MAX(CAST(CENTER_LNG as DOUBLE)) as LNG_MAX, " +
        "MIN(CAST(CENTER_LNG as DOUBLE)) as LNG_MIN " +
        "FROM street_seg;");

      ReadMinMax(reader, ref minLat, ref maxLat, ref minLng, ref maxLng);

      reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT " +
        "MAX(CAST(LAT as DOUBLE)) as LAT_MAX, " +
        "MIN(CAST(LAT as DOUBLE)) as LAT_MIN, " +
        "MAX(CAST(LNG as DOUBLE)) as LNG_MAX, " +
        "MIN(CAST(LNG as DOUBLE)) as LNG_MIN " +
        "FROM street_seg_koo;");

      ReadMinMax(reader, ref minLat, ref maxLat, ref minLng, ref maxLng);

      reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT " +
        "MAX(CAST(lat as DOUBLE)) as LAT_MAX, " +
        "MIN(CAST(lat as DOUBLE)) as LAT_MIN, " +
        "MAX(CAST(lng as DOUBLE)) as LNG_MAX, " +
        "MIN(CAST(lng as DOUBLE)) as LNG_MIN " +
        "FROM zip_koo;");

      ReadMinMax(reader, ref minLat, ref maxLat, ref minLng, ref maxLng);

      var minCoordinate = new Coordinate
      {
        Lat = minLat, Lng = minLng
      };

      var maxCoordinate = new Coordinate
      {
        Lat = maxLat, Lng = maxLng
      };

      return $"Coordinates are in range {minCoordinate} to {maxCoordinate}.";
    }
  }
}
