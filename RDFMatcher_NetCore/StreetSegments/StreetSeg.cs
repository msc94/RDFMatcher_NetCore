using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class StreetSeg
  {
    public static void DoStreetSeg()
    {
      var workQueue = new BlockingCollection<StreetSegItem>();
      var workerThreadsProgress = new WorkerThreadsProgress();

      var streetSegThreads = new List<StreetSegThread>();
      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        streetSegThreads.Add(new StreetSegThread(workerThreadsProgress, workQueue));
      }

      var szIDReader = MySqlHelper.ExecuteReader(DB.ConnectionString, "SELECT ID FROM street_zip");
      while (szIDReader.Read())
      {
        StreetSegItem item = new StreetSegItem
        {
          szID = szIDReader.GetValue(0)
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
