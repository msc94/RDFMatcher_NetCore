using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class Match
  {
    private static WorkerThreadsProgress _workerThreadProgress = new WorkerThreadsProgress();
    private static BlockingCollection<MatchAddressItem> _workQueue = new BlockingCollection<MatchAddressItem>();

    private static int _lastDoneItems = 0;
    private static void PrintProgress()
    {
      var done = _workerThreadProgress.ItemsDone;
      var matched = _workerThreadProgress.ItemsSuccessful;

      var matchedPercentage = matched / done;
      var matchedString = matchedPercentage.ToString("0.00");

      var matchesPerSecond = done - _lastDoneItems;
      _lastDoneItems = done;

      Console.WriteLine($"Done: {done}, Matched: {matched}, Percentage: {matchedString}, Items/s: {matchesPerSecond}");
    }


    public static void DoMatch()
    {
      string commandText = "SELECT sz.ID as SZ_ID, sz.ZIP, s.NAME " +
                           "FROM street_zip sz " +
                           "  LEFT JOIN street s ON s.id = sz.STREET_ID " +
                           "";
      // "WHERE sz.ID NOT IN (SELECT street_zip_id FROM building)";


      var matchingThreadsProgress = new WorkerThreadsProgress();
      var matchingThreadWorkQueue = new BlockingCollection<MatchAddressItem>();
      var matchingThreads = new List<WorkerThread<MatchAddressItem>>();

      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        matchingThreads.Add(new MatchAddressThread(matchingThreadsProgress, matchingThreadWorkQueue));
      }

      var reader = MySqlHelper.ExecuteReader(DB.ConnectionString, commandText);
      while (reader.Read())
      {
        try
        {
          var item = new MatchAddressItem
          {
            StreetZipId = reader.GetValue(reader.GetOrdinal("SZ_ID")),
            Zip = reader.GetString("ZIP"),
            StreetName = reader.GetString("NAME"),
          };

          _workQueue.Add(item);
        }
        catch (SqlNullValueException)
        {
        }
      }

      _workQueue.CompleteAdding();
      while (_workQueue.IsCompleted == false)
      {
        PrintProgress();
        Thread.Sleep(1000);
      }

      foreach (var matchingThread in matchingThreads)
      {
        // matchingThread.FlushInsertBuffer();
      }

    }
  }
}
