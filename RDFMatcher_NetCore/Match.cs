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
      string commandText = "SELECT b.id as BUILDING_ID, sz.ID as SZ_ID, sz.DISTRICT_CODE, s.NAME, b.HNO, b.HNO_EXTENSION " +
                           "FROM building b " +
                           "LEFT JOIN street_zip sz ON sz.id = b.STREET_ZIP_ID " +
                           "LEFT JOIN street s ON s.id = sz.STREET_ID " +
                           "WHERE b.ID NOT IN (SELECT BUILDING_ID FROM match_test2)";


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
            building_id = reader.GetValue(reader.GetOrdinal("BUILDING_ID")),
            street_zip_id = reader.GetValue(reader.GetOrdinal("SZ_ID")),
            district_code = reader.GetString("DISTRICT_CODE"),
            street_name = reader.GetString("NAME"),
            hno = reader.GetString("HNO").TrimStart('0'),
            hno_extension = reader.GetString("HNO_EXTENSION")
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
