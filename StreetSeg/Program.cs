using DatabaseLibrary;
using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace StreetSeg
{
  class Program
  {
    private static Progress _progress = new Progress();

    static void Main(string[] args)
    {
      GlobalLibraryState.Init("StreetSeg", "Marcel", "YyQzKeSSX0TlgsI4", "RUS");

      var taskList = new List<Task>();

      var szIDReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT sz.ID " +
        "FROM street_zip sz " +
        " LEFT JOIN street_seg ss ON (ss.STREET_ZIP_ID = sz.ID) " +
        "WHERE sz.ID NOT IN (SELECT STREET_ZIP_ID FROM street_seg) " +
        " OR ss.ID NOT IN (SELECT STREET_SEG_ID FROM street_seg_koo_group);");

      while (szIDReader.Read())
      {
        var item = new StreetSegItem
        {
          StreetZipId = szIDReader.GetInt64("ID")
        };

        taskList.Add(Task.Run(() =>
          AddSegment(item)
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

    private static void Cleanup(long streetZipId)
    {
      long streetSegId = Convert.ToInt64(DatabaseHelper.ExecuteScalar(GlobalLibraryState.ConnectionString,
        "SELECT ID FROM street_seg WHERE STREET_ZIP_ID = @1", streetZipId));

      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "DELETE FROM street_seg_koo_group WHERE STREET_SEG_ID = @1;" +
        "DELETE FROM street_seg_koo WHERE STREET_SEG_ID = @1;" +
        "DELETE FROM street_seg WHERE ID = @1",
        streetSegId);
    }

    private static void AddSegment(StreetSegItem item)
    {
      _progress.IncrementItemsDone();

      Cleanup(item.StreetZipId);

      var houseNumbers = new List<int>();

      var houseNumberReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT HNO " +
        "FROM building " +
        "WHERE STREET_ZIP_ID = @1 " +
        "AND HNO IS NOT NULL AND HNO <> '';",
        item.StreetZipId);

      using (houseNumberReader)
      {
        while (houseNumberReader.Read())
        {
          var houseNumber = int.Parse(houseNumberReader.GetString("HNO"));
          houseNumbers.Add(houseNumber);
        }
      }

      int? minHNO = null;
      int? maxHNO = null;
      int? scheme = null;

      if (houseNumbers.Count > 0)
      {
        minHNO = houseNumbers.Min();
        maxHNO = houseNumbers.Max();

        bool odd = false, even = false;
        foreach (var hno in houseNumbers)
        {
          if (hno % 2 == 0)
            even = true;
          else
            odd = true;
        }

        if (odd && even)
          scheme = 3;
        else if (even)
          scheme = 2;
        else
          scheme = 1;
      }

      long streetSegId = Convert.ToInt64(DatabaseHelper.ExecuteScalar(GlobalLibraryState.ConnectionString,
        "INSERT INTO street_seg (STREET_ZIP_ID, HN_START, HN_END, SCHEME) " +
        "VALUES (@1, @2, @3, @4); " +
        "SELECT LAST_INSERT_ID();",
        item.StreetZipId, minHNO, maxHNO, scheme));

      var coordinatesItem = new CoordinatesItem
      {
        HouseNumberStart = minHNO,
        HouseNumberEnd = maxHNO,
        SegmentId = streetSegId,
        StreetZipId = item.StreetZipId
      };

      if (Coordinates.AddCoordinates(coordinatesItem))
      {
        _progress.IncrementItemsSuccessful();
      }
    }
  }
}
