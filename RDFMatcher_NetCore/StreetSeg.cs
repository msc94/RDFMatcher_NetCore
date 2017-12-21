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
    private static StreetSegProgress _streetSegProgress = new StreetSegProgress();
    private static BlockingCollection<StreetSegItem> _workQueue = new BlockingCollection<StreetSegItem>();

    public static void DoStreetSeg()
    {
      var streetSegThreads = new List<StreetSegThread>();
      for (int i = 0; i < 8; i++)
      {
        streetSegThreads.Add(new StreetSegThread(_streetSegProgress, _workQueue));
      }

      var szIDReader = MySqlHelper.ExecuteReader(DB.connectionString, "SELECT ID FROM street_zip");
      while (szIDReader.Read())
      {
        StreetSegItem item = new StreetSegItem
        {
          szID = szIDReader.GetValue(0)
        };

        _workQueue.Add(item);
      }
      szIDReader.Close();

      _workQueue.CompleteAdding();
      while (_workQueue.IsCompleted == false)
      {
        Console.WriteLine($"Done: {_streetSegProgress.done}, Left: {_workQueue.Count}");
        Thread.Sleep(2000);
      }
    }
  }
}
