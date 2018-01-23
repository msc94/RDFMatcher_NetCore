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
    public static void DoMatch()
    {
      string commandText = "SELECT b.ID as B_ID, b.HNO, b.HNO_EXTENSION, sz.ID as SZ_ID, sz.ZIP, s.NAME " +
                           "FROM building b " +
                           "  LEFT JOIN street_zip sz ON sz.ID = b.STREET_ZIP_ID " +
                           "  LEFT JOIN street s ON s.id = sz.STREET_ID " +
                           "WHERE b.ID NOT IN (SELECT BUILDING_ID FROM match_test)";


      var matchingThreadsProgress = new WorkerThreadsProgress();
      var matchingThreadWorkQueue = new BlockingCollection<MatchAddressItem>();
      var matchingThreads = new List<MatchAddressThread>();

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
            BuildingId = reader.GetValue(reader.GetOrdinal("B_ID")),
            StreetZipId = reader.GetValue(reader.GetOrdinal("SZ_ID")),
            HouseNumber = reader.GetString("HNO"),
            HouseNumberExtension = reader.GetString("HNO_EXTENSION"),
            Zip = reader.GetString("ZIP"),
            StreetName = reader.GetString("NAME")
          };

          item.HouseNumber = item.HouseNumber.TrimStart('0');

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
        Thread.Sleep(1000);
      }

      foreach (var matchingThread in matchingThreads)
      {
        matchingThread.FlushInsertBuffer();
      }

    }
  }
}
