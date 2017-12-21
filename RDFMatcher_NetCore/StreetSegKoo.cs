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
      for (int i = 0; i < 1; i++)
      {
        streetSegThreads.Add(new StreetSegKooThread(_streetSegKooProgress, _workQueue));
      }

      var szIDReader = MySqlHelper.ExecuteReader(DB.connectionString, "SELECT ID, HN_START, HN_END, SCHEME " +
                                                                      "FROM street_seg");
      while (szIDReader.Read())
      {
        var item = new StreetSegKooItem
        {
          szID = szIDReader.GetValue(0),
          hnStart = szIDReader.GetInt32(1),
          hnEnd = szIDReader.GetInt32(2),
          scheme = szIDReader.GetInt32(3)
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