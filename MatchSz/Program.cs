using DatabaseLibrary;
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
      GlobalLibraryState.Init("MatchSz", "root", "bloodrayne", "nor");

      // Get all street_zip entries
      string commandText = "SELECT sz.ID as SZ_ID, sz.ZIP, s.NAME " +
                           "FROM street_zip sz " +
                           "  LEFT JOIN street s ON s.id = sz.STREET_ID " +
                           "WHERE sz.ID NOT IN (SELECT SZ_ID FROM match_sz)";

      var taskList = new List<Task>();

      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString, "TRUNCATE TABLE match_sz;");

      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString, commandText);
      while (reader.Read())
      {
        var item = new MatchSzItem
        {
          StreetZipId = reader.GetInt32("SZ_ID"),
          Zip = reader.GetString("ZIP"),
          StreetName = reader.GetString("NAME")
        };

        taskList.Add(Task.Run(() => MatchSz(item)));
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

      var addrItems = GetMatchingRdfAddrItems(item.Zip, item.StreetName, item.StreetType);

      if (addrItems.Count == 0)
      {
        Log.WriteLine($"No match for {item.Zip}, {item.StreetName}, {item.StreetType}");
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
        " addr.LEFT_POSTAL_CODE = @1 AND " +
        " (addr.STREET_FULL_NAME = @2 OR addr.STREET_FULL_NAME2 = @2);",
         zip, streetName);

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