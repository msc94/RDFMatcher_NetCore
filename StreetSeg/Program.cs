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
      GlobalLibraryState.Init("StreetSeg", "root", "bloodrayne", "nor");

      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "TRUNCATE TABLE street_seg; " +
        "TRUNCATE TABLE street_seg_koo; " +
        "TRUNCATE TABLE street_seg_koo_group;");

      var taskList = new List<Task>();

      var szIDReader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString, "SELECT ID FROM street_zip;");
      while (szIDReader.Read())
      {
        var item = new StreetSegItem
        {
          StreetZipId = szIDReader.GetInt64("ID")
        };

        taskList.Add(Task.Run(() => AddSegment(item)));
      }

      var whenAll = Task.WhenAll(taskList);
      while (!whenAll.IsCompleted)
      {
        _progress.PrintProgress();
        Thread.Sleep(1000);
        Log.Flush();
      }
    }

    private static void AddSegment(StreetSegItem item)
    {
      _progress.IncrementItemsDone();

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
          var houseNumber = houseNumberReader.GetInt32("HNO");
          houseNumbers.Add(houseNumber);
        }
      }

      if (houseNumbers.Count == 0)
        return;

      var minHNO = houseNumbers.Min();
      var maxHNO = houseNumbers.Max();

      if (minHNO == maxHNO)
        return;

      bool odd = false, even = false;
      foreach (var hno in houseNumbers)
      {
        if (hno % 2 == 0)
          even = true;
        else
          odd = true;
      }

      int scheme = 0;
      if (odd && even)
        scheme = 3;
      else if (even)
        scheme = 2;
      else
        scheme = 1;

      long streetSegId = (long) DatabaseHelper.ExecuteScalar(GlobalLibraryState.ConnectionString,
        "INSERT INTO street_seg (STREET_ZIP_ID, HN_START, HN_END, SCHEME) " +
        "VALUES (@1, @2, @3, @4); " +
        "SELECT LAST_INSERT_ID();",
        item.StreetZipId, minHNO, maxHNO, scheme);

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
