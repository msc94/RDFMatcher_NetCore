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
      while (true)
      {
        var done = _matchProgress.done;
        var matched = _matchProgress.matched;

        var matchedPercentage = (float)_matchProgress.matched / done;
        var matchedString = matchedPercentage.ToString("0.00");

        var matchesPerSecond = (done - _lastDoneItems) / 2;
        _lastDoneItems = done;

        Console.WriteLine($"Done: {done}, Matched: {matched}, Percentage: {matchedString}, Items/s: {matchesPerSecond}");
        Thread.Sleep(2000);
      }
    }


    public static void DoMatch()
    {
      string commandText = "select b.id, sz.DISTRICT_CODE, s.NAME, b.HNO, b.HNO_EXTENSION\n" +
                           "from building b\n" +
                           "left join street_zip sz on sz.id = b.street_zip_id\n" +
                           "left join street s on s.id = sz.street_id\n" +
                           "where b.id not in (select building_id from match_test2)";


      Task.Run(() => PrintProgress());

      var matchingThreads = new List<MatchThread>();
      for (int i = 0; i < 8; i++)
      {
        matchingThreads.Add(new MatchThread(_matchProgress, _workQueue));
      }

      var reader = MySqlHelper.ExecuteReader(DB.connectionString, commandText);
      while (reader.Read())
      {
        try
        {
          var item = new MatchItem
          {
            building_id = reader.GetValue(0),
            district_code = reader.GetString(1),
            street_name = reader.GetString(2),
            hno = reader.GetString(3).TrimStart('0'),
            hno_extension = reader.GetString(4)
          };
          item.full_hno = CombineHNO(item.hno, item.hno_extension);

          _workQueue.Add(item);
        }
        catch (SqlNullValueException)
        {
        }
      }

      _workQueue.CompleteAdding();
      while (_workQueue.IsCompleted == false)
        Thread.Sleep(1000);
    }

    private static string CombineHNO(string hno, string hnoExtension)
    {
      if (hnoExtension == "")
        return hno;
      if (char.IsDigit(hnoExtension[0]))
        return hno + "-" + hnoExtension;
      else
        return hno + hnoExtension;
    }
  }
}
