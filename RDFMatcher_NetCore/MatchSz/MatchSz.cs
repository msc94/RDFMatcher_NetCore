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
  class MatchSz
  {
    public static void DoMatch()
    {
      string commandText = "SELECT sz.ID as SZ_ID, sz.ZIP, s.NAME " +
                           "FROM street_zip sz " +
                           "  LEFT JOIN street s ON s.id = sz.STREET_ID " +
                           "WHERE sz.ID NOT IN (SELECT SZ_ID FROM match_sz)";


      var matchingThreadsProgress = new WorkerThreadsProgress();
      var matchingThreadWorkQueue = new BlockingCollection<MatchSzItem>();
      var matchingThreads = new List<MatchSzThread>();

      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        matchingThreads.Add(new MatchSzThread(matchingThreadsProgress, matchingThreadWorkQueue));
      }

      var reader = MySqlHelper.ExecuteReader(DB.ConnectionString, commandText);
      while (reader.Read())
      {
        try
        {
          var item = new MatchSzItem
          {
            StreetZipId = reader.GetInt32("SZ_ID"),
            Zip = reader.GetString("ZIP"),
            StreetName = reader.GetString("NAME")
          };

          matchingThreadWorkQueue.Add(item);
        }
        catch (SqlNullValueException)
        {
        }
      }

      matchingThreadWorkQueue.CompleteAdding();
      while (matchingThreadWorkQueue.IsCompleted == false)
      {
        matchingThreadsProgress.PrintProgress();
        Log.Flush();
        Thread.Sleep(1000);
      }
    }
  }
}
