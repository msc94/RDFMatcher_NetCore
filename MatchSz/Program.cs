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
      string commandText = "SELECT sz.ID as SZ_ID, sz.ZIP, s.NAME as S_NAME, s.TYPE as S_TYPE, sa.NAME as SA_NAME, sa.TYPE as SA_TYPE " +
                           "FROM street_zip sz " +
                           "  LEFT JOIN street s ON (s.id = sz.STREET_ID) " +
                           "  LEFT JOIN street sa on (sa.STREET_MASTER_ID = s.ID) " +
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
          StreetTypeAlias = reader.GetString("SA_TYPE")
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

      if (item.Zip.Length == 0)
      {
        Log.WriteLine($"Zip is empty in {item.Zip}, {item.StreetNameAlias}, {item.StreetTypeAlias}");
        return;
      }

      if (item.StreetName.Length == 0)
      {
        Log.WriteLine($"Streetname is empty in {item.Zip}, {item.StreetNameAlias}, {item.StreetTypeAlias}");
        return;
      }

      item.StreetTypeAlias = RUS.ReplaceStreetType(item.StreetTypeAlias);
      var addrItems = GetMatchingRdfAddrItems(item.Zip, item.StreetNameAlias, item.StreetTypeAlias);

      if (addrItems.Count == 0)
      {
        Log.WriteLine($"No match for {item.Zip}, {item.StreetNameAlias}, {item.StreetTypeAlias}");
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
  }
}