using DatabaseLibrary;
using DatabaseLibrary.Countries;
using DatabaseLibrary.Utilities;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchSz
{
  class Program
  {
    private static Progress _progress = new Progress();

    static void Main(string[] args)
    {
      GlobalLibraryState.Init("MatchSz", "Marcel", "YyQzKeSSX0TlgsI4", "RUS");
      // DebuggerUtils.WaitForDebugger();

      var taskList = new List<Task>();

      // Get all street_zip entries
      string commandText = "SELECT sz.ID as SZ_ID, sz.ZIP, s.NAME as S_NAME, s.TYPE as S_TYPE, sa.NAME as SA_NAME, sa.TYPE as SA_TYPE, z4a.ZONE_NAME as Z4_NAME, z3a.ZONE_NAME as Z3_NAME, z2a.ZONE_NAME as Z2_NAME, z1a.ZONE_NAME as Z1_NAME " +
                           "FROM street_zip sz " +
                           "  LEFT JOIN street s ON (s.id = sz.STREET_ID) " +
                           "  LEFT JOIN street sa ON (sa.STREET_MASTER_ID = s.ID) " +
                           "  LEFT JOIN zone z4 ON (s.ZONE_ID = z4.ID) " +
                           "  LEFT JOIN zone z4a ON (z4a.ZONE_MASTER_ID = z4.ID) " +
                           "  LEFT JOIN zone z3 ON (z4.LEVEL_3_ZONE_ID = z3.ID) " +
                           "  LEFT JOIN zone z3a ON (z3a.ZONE_MASTER_ID = z3.ID) " +
                           "  LEFT JOIN zone z2 ON (z3.LEVEL_2_ZONE_ID = z2.ID) " +
                           "  LEFT JOIN zone z2a ON (z2a.ZONE_MASTER_ID = z2.ID) " +
                           "  LEFT JOIN zone z1 ON (z2.LEVEL_1_ZONE_ID = z1.ID) " +
                           "  LEFT JOIN zone z1a ON (z1a.ZONE_MASTER_ID = z1.ID) " +
                           "WHERE sz.ID NOT IN (SELECT SZ_ID FROM match_sz);";

      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString, commandText);
      while (reader.Read())
      {
        var item = new MatchSzItem
        {
          StreetZipId = reader.GetInt32("SZ_ID"),
          Zip = reader.GetString("ZIP"),

          StreetName = reader.GetString("S_NAME"),
          StreetType = reader.GetString("S_TYPE"),

          StreetNameAlias = reader.GetString("SA_NAME"),
          StreetTypeAlias = reader.GetString("SA_TYPE"),

          Level4ZoneName = reader.GetString("Z4_NAME"),
          Level3ZoneName = reader.GetString("Z3_NAME"),
          Level2ZoneName = reader.GetString("Z2_NAME"),
          Level1ZoneName = reader.GetString("Z1_NAME")
        };

        taskList.Add(Task.Run(() =>
          MatchSz(item)
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

    static void MatchSz(MatchSzItem item)
    {
      _progress.IncrementItemsDone();

      if (item.StreetName.Length == 0)
      {
        Log.WriteLine($"Streetname is empty in {item.Zip}, {item.StreetNameAlias}, {item.StreetTypeAlias}");
        return;
      }

      item.StreetTypeAlias = RUS.ReplaceStreetType(item.StreetTypeAlias);
      item.StreetNameAlias = RUS.ReplaceStreetName(item.StreetNameAlias);

      var departmentName = item.Level3ZoneName.Length == 0 ? item.Level2ZoneName : item.Level3ZoneName;
      var addrItems = GetMatchingRdfAddrItemsZoneNames(item.Level4ZoneName, departmentName, item.StreetNameAlias, item.StreetTypeAlias);

      if (addrItems.Count == 0)
      {
        addrItems = GetMatchingRdfAddrItems(item.Zip, item.StreetNameAlias, item.StreetTypeAlias);
      }

      if (addrItems.Count == 0)
      {
        Log.WriteLine($"No match for {item.Zip}, {item.StreetNameAlias}, {item.StreetTypeAlias}, {item.Level4ZoneName}, {departmentName}");
        return;
      }

      foreach (var addrItem in addrItems)
      {
        InsertMatchedSzItem(new MatchedSzItem
        {
          RoadLinkId = addrItem,
          StreetZipId = item.StreetZipId
        });
      }

      _progress.IncrementItemsSuccessful();
    }

    private static void InsertMatchedSzItem(MatchedSzItem item)
    {
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "INSERT INTO match_sz VALUES (@1, @2)", item.StreetZipId, item.RoadLinkId);
    }

    private static List<long> GetMatchingRdfAddrItems(string zip, string streetName, string streetType)
    {
      if (zip.Length == 0)
      {
        Log.WriteLine($"Zip is empty");
        return new List<long>();
      }

      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT addr.ROAD_LINK_ID " +
        $"FROM {GlobalLibraryState.RdfAddrTable} addr " +
        "WHERE " +
        " addr.LEFT_POSTAL_CODE = @1 " +
        " AND addr.STREET_BASE_NAME = @2 " +
        " AND addr.STREET_TYPE = @3;",
         zip, streetName, streetType);

      var rdfAddrItemList = new List<long>();
      using (reader)
      {
        while (reader.Read())
        {
          rdfAddrItemList.Add(reader.GetInt64("ROAD_LINK_ID"));
        }
      }
      return rdfAddrItemList;
    }

    private static List<long> GetMatchingRdfAddrItemsZoneNames(string localityName, string departmentName, string streetName, string streetType)
    {
      if (departmentName.Length == 0)
      {
        Log.WriteLine($"Department name is empty");
        return new List<long>();
      }

      if (localityName.Length == 0)
      {
        Log.WriteLine($"Locality name is empty");
        return new List<long>();
      }

      departmentName = '%' + departmentName + '%';

      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT addr.ROAD_LINK_ID " +
        $"FROM {GlobalLibraryState.RdfAddrTable} addr " +
        "WHERE " +
        " addr.LEFT_LOCALITY_NAME = @1 " +
        " AND addr.LEFT_DEPARTMENT_NAME LIKE @2 " +
        " AND addr.STREET_BASE_NAME = @3 " +
        " AND addr.STREET_TYPE = @4;",
        localityName, departmentName, streetName, streetType);

      var rdfAddrItemList = new List<long>();
      using (reader)
      {
        while (reader.Read())
        {
          rdfAddrItemList.Add(reader.GetInt64("ROAD_LINK_ID"));
        }
      }
      return rdfAddrItemList;
    }
  }
}