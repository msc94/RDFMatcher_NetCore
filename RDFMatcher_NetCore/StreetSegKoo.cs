using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  public class StreetSegKoo
  {
    private static StreetSegKooProgress _streetSegKooProgress = new StreetSegKooProgress();
    private static BlockingCollection<StreetSegKooItem> _workQueue = new BlockingCollection<StreetSegKooItem>();

    public static void DoStreetSegKoo()
    {
      var streetSegThreads = new List<StreetSegKooThread>();
      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        streetSegThreads.Add(new StreetSegKooThread(_streetSegKooProgress, _workQueue));
      }

      var szIDReader = MySqlHelper.ExecuteReader(DB.ConnectionString, "SELECT ID, HN_START, HN_END, SCHEME " +
                                                                      "FROM street_seg seg " +
                                                                      "WHERE " +
                                                                      " seg.STREET_ZIP_ID in " +
                                                                      " (SELECT DISTINCT b.STREET_ZIP_ID " +
                                                                      " FROM match_test m " +
                                                                      " LEFT JOIN building b ON m.BUILDING_ID = b.ID)");
      while (szIDReader.Read())
      {
        var item = new StreetSegKooItem
        {
          szID = szIDReader.GetValue(szIDReader.GetOrdinal("ID")),
          hnStart = szIDReader.GetInt32("HN_START"),
          hnEnd = szIDReader.GetInt32("HN_END"),
          scheme = szIDReader.GetInt32("SCHEME")
        };

        _workQueue.Add(item);
      }
      szIDReader.Close();

      _workQueue.CompleteAdding();
      while (_workQueue.IsCompleted == false)
      {
        Console.WriteLine($"Done: {_streetSegKooProgress.done}, Left: {_workQueue.Count}");
        Thread.Sleep(2000);
      }
    }
  }
}