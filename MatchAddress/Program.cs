using DatabaseLibrary;
using DatabaseLibrary.DatabaseClasses;
using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchAddress
{
  class Program
  {
    private static Progress _progress = new Progress();

    static void Main(string[] args)
    {
      GlobalLibraryState.Init("MatchAddress", "Marcel", "YyQzKeSSX0TlgsI4", "RUS");

      string commandText = "SELECT b.ID as B_ID, b.HNO, b.HNO_EXTENSION, b.STREET_ZIP_ID as SZ_ID " +
                           "FROM building b " +
                           "WHERE b.ID NOT IN (SELECT BUILDING_ID FROM match_building) " +
                           "AND b.STREET_ZIP_ID IN (SELECT SZ_ID FROM match_sz);";

      var taskList = new List<Task>();

      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString, commandText);
      while (reader.Read())
      {
        var item = new MatchAddressItem
        {
          BuildingId = reader.GetInt64("B_ID"),
          StreetZipId = reader.GetInt64("SZ_ID"),
          HouseNumber = reader.GetString("HNO"),
          HouseNumberExtension = reader.GetString("HNO_EXTENSION")
        };

        taskList.Add(Task.Run(() => 
          MatchBuilding(item)
        ));
      }

      var whenAll = Task.WhenAll(taskList);
      while (!whenAll.IsCompleted)
      {
        _progress.PrintProgress();
        Thread.Sleep(1000);
        Log.Flush();
      }
    }

    private static void MatchBuilding(MatchAddressItem item)
    {
      _progress.IncrementItemsDone();

      var address = (item.HouseNumber + item.HouseNumberExtension).TrimStart('0');

      var matchedPoints = GetRdfPointsForAddress(item.StreetZipId, address);
      var numMatches = matchedPoints.Count;

      if (numMatches == 0)
      {
        Log.WriteLine($"No match for {item.BuildingId}, {item.StreetZipId}, {address}");
        return;
      }
      if (numMatches > 1)
      {
        Log.WriteLine($"More than one match for {item.BuildingId}, {item.StreetZipId}, {address}");
        return;
      }

      var match = matchedPoints[0];
      var coordinates = match.Coordinates;

      coordinates.Lat = Utils.RdfCoordinateInsertDecimal(coordinates.Lat);
      coordinates.Lng = Utils.RdfCoordinateInsertDecimal(coordinates.Lng);

      InsertMatchedBuildingItem(new MatchedAddressItem
      {
        StreetZipId = item.StreetZipId,
        Address = address,
        RoadLinkId = match.RoadLinkId,
        BuildingId = item.BuildingId,
        Coordinates = coordinates
      });

      _progress.IncrementItemsSuccessful();
    }

    private static List<RdfPointItem> GetRdfPointsForAddress(long streetZipId, string address)
    {
      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT pt.ROAD_LINK_ID, pt.ADDRESS, pt.LAT, pt.LNG " +
        $"FROM match_sz msz " +
        $"  JOIN {GlobalLibraryState.RdfPointTable} pt using (ROAD_LINK_ID) " +
        "WHERE msz.SZ_ID = @1 " +
        " AND pt.ADDRESS = @2 " +
        " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL;",
          streetZipId,
          address);

      List<RdfPointItem> coordinateList = new List<RdfPointItem>();
      using (reader)
      {
        while (reader.Read())
        {
          var coordinates = new Coordinates<string>()
          {
            Lat = reader.GetString("LAT"),
            Lng = reader.GetString("LNG")
          };

          var rdfPointEntry = new RdfPointItem()
          {
            RoadLinkId = reader.GetInt32("ROAD_LINK_ID"),
            Address = reader.GetString("ADDRESS"),
            Coordinates = coordinates
          };

          coordinateList.Add(rdfPointEntry);
        }
      }
      return coordinateList;
    }

    private static void InsertMatchedBuildingItem(MatchedAddressItem item)
    {
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "INSERT INTO match_building VALUES (@1, @2, @3, @4, @5)",
          item.RoadLinkId,
          item.Address,
          item.BuildingId,
          item.Coordinates.Lat,
          item.Coordinates.Lng);
    }
  }
}
