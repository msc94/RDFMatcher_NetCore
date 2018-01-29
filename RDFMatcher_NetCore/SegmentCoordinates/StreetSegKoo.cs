using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  public class StreetSegKoo
  {
    public static void DoStreetSegKoo()
    {
      var workQueue = new BlockingCollection<StreetSegKooItem>();
      var workerThreadsProgress = new WorkerThreadsProgress();

      var streetSegThreads = new List<StreetSegKooThread>();
      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        streetSegThreads.Add(new StreetSegKooThread(workerThreadsProgress, workQueue));
      }

      MySqlHelper.ExecuteNonQuery(DB.ConnectionString,
        "TRUNCATE TABLE street_seg_koo; " +
        "TRUNCATE TABLE street_seg_koo_group;");

      var szIDReader = MySqlHelper.ExecuteReader(DB.ConnectionString, "SELECT ID, HN_START, HN_END, SCHEME " +
                                                                      "FROM street_seg seg " +
                                                                      "WHERE TRUE;"); // +
                                                                      //" seg.STREET_ZIP_ID in " +
                                                                      //" (SELECT DISTINCT m.SZ_ID FROM match_sz m) AND" +
                                                                      //" seg.ID not in" +
                                                                      //" (SELECT STREET_SEG_ID FROM street_seg_koo)");
      while (szIDReader.Read())
      {
        var item = new StreetSegKooItem
        {
          SegmentId = szIDReader.GetValue(szIDReader.GetOrdinal("ID")),
          HouseNumberStart = szIDReader.GetInt32("HN_START"),
          HouseNumberEnd = szIDReader.GetInt32("HN_END"),
          Scheme = szIDReader.GetInt32("SCHEME")
        };

        workQueue.Add(item);
      }
      szIDReader.Close();

      workQueue.CompleteAdding();
      while (workQueue.IsCompleted == false)
      {
        workerThreadsProgress.PrintProgress();
        Log.Flush();
        Thread.Sleep(2000);
      }
    }
  }
}