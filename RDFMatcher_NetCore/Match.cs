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
    private static MatchProgress _matchProgress = new MatchProgress();
    private static BlockingCollection<MatchItem> _workQueue = new BlockingCollection<MatchItem>();

    private static int _lastDoneItems = 0;
    private static void PrintProgress()
    {
      var done = _matchProgress.done;
      var matched = _matchProgress.matched;

      var matchedPercentage = (float)_matchProgress.matched / done;
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


      var matchingThreads = new List<MatchThread>();
      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        matchingThreads.Add(new MatchThread(_matchProgress, _workQueue));
      }

      var reader = MySqlHelper.ExecuteReader(DB.ConnectionString, commandText);
      while (reader.Read())
      {
        try
        {
          var item = new MatchItem
          {
            street_zip_id = reader.GetValue(reader.GetOrdinal("SZ_ID")),
            zip = reader.GetString("ZIP"),
            street_name = reader.GetString("NAME"),
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
        matchingThread.FlushInsertBuffer();
      }

    }
  }
}
