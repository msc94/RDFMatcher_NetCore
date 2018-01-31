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
  class MatchAddress
  {
    public static void DoMatch()
    {
      string commandText = "SELECT b.ID as B_ID, b.HNO, b.HNO_EXTENSION, sz.ID as SZ_ID, sz.ZIP, s.NAME " +
                           "FROM building b " +
                           "  LEFT JOIN street_zip sz ON (b.STREET_ZIP_ID = sz.ID) " +
                           "  LEFT JOIN street s ON s.id = sz.STREET_ID " +
                           "WHERE b.ID NOT IN (SELECT BUILDING_ID FROM match_building);";


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
            BuildingId = reader.GetInt64("B_ID"),
            StreetZipId = reader.GetInt64("SZ_ID"),
            Zip = reader.GetString("ZIP"),
            StreetName = reader.GetString("NAME"),
            // StreetType = reader.GetString("TYPE"),
            HouseNumber = reader.GetString("HNO"),
            HouseNumberExtension = reader.GetString("HNO_EXTENSION")
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
