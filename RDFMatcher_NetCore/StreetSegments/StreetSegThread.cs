using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class StreetSegThread : WorkerThread<StreetSegItem>
  {
    private const string queryCommandText = "SELECT ID, HNO " +
                                            "FROM building " +
                                            "WHERE STREET_ZIP_ID = @1 " +
                                            "AND HNO IS NOT NULL AND HNO <> ''";

    private const string insertCommandText = "INSERT INTO street_seg (STREET_ZIP_ID, HN_START, HN_END, SCHEME) " +
                                             "VALUES (@1, @2, @3, @4)";

    public StreetSegThread(WorkerThreadsProgress workerThreadProgress, BlockingCollection<StreetSegItem> workQueue) 
      : base(workerThreadProgress, workQueue)
    {
    }

    public override WorkResult Work(StreetSegItem item)
    {
      var houseNumbers = new List<int>();

      var houseNumberReader = MySqlHelper.ExecuteReader(DB.ConnectionString, queryCommandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", item.szID)
        });

      using (houseNumberReader)
      {
        while (houseNumberReader.Read())
        {
          var buildingId = houseNumberReader.GetInt32("ID");
          var houseNumber = houseNumberReader.GetInt32("HNO");
          houseNumbers.Add(houseNumber);
        }
      }

      if (houseNumbers.Count == 0)
        return WorkResult.Failed;

      var minHNO = houseNumbers.Min();
      var maxHNO = houseNumbers.Max();

      if (minHNO == maxHNO)
        return WorkResult.Failed;

      bool odd = false, even = false;
      foreach (var hno in houseNumbers)
      {
        if (hno % 2 == 0)
          even = true;
        else
          odd = true;
      }

      int scheme = 0;
      if (odd && even)
        scheme = 3;
      else if (even)
        scheme = 2;
      else
        scheme = 1;

      MySqlHelper.ExecuteNonQuery(DB.ConnectionString, insertCommandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", item.szID),
          new MySqlParameter("@2", minHNO),
          new MySqlParameter("@3", maxHNO),
          new MySqlParameter("@4", scheme)
        });

      return WorkResult.Successful;
    }
  }
}
