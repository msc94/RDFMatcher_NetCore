using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class StreetSegProgress
  {
    public int done = 0;
  }

  class StreetSegItem
  {
    public object szID;
  }

  class StreetSegThread
  {
    private const string queryCommandText = "SELECT ID, HNO " +
                                            "FROM building " +
                                            "WHERE STREET_ZIP_ID = @1 " +
                                            "AND HNO IS NOT NULL AND HNO <> '00000'";

    private const string insertCommandText = "INSERT INTO street_seg (STREET_ZIP_ID, HN_START, HN_END, SCHEME) " +
                                             "VALUES (@1, @2, @3, @4)";

    private readonly StreetSegProgress _streetSegProgress;
    private readonly BlockingCollection<StreetSegItem> _workQueue;

    private readonly MySqlConnection _hnoQueryConnection;
    private readonly MySqlConnection _insertConnection;

    private readonly MySqlCommand _hnoQueryCommand;
    private readonly MySqlCommand _insertCommand;

    public StreetSegThread(StreetSegProgress streetSegProgress, BlockingCollection<StreetSegItem> workQueue)
    {
      _streetSegProgress = streetSegProgress;
      _workQueue = workQueue;

      _hnoQueryConnection = new MySqlConnection(DB.connectionString);
      _hnoQueryConnection.Open();

      _hnoQueryCommand = _hnoQueryConnection.CreateCommand();
      _hnoQueryCommand.CommandText = queryCommandText;
      _hnoQueryCommand.Prepare();

      _hnoQueryCommand.Parameters.AddWithValue("@1", null);

      _insertConnection = new MySqlConnection(DB.connectionString);
      _insertConnection.Open();

      _insertCommand = _insertConnection.CreateCommand();
      _insertCommand.CommandText = insertCommandText;
      _insertCommand.Prepare();

      _insertCommand.Parameters.AddWithValue("@1", null);
      _insertCommand.Parameters.AddWithValue("@2", null);
      _insertCommand.Parameters.AddWithValue("@3", null);
      _insertCommand.Parameters.AddWithValue("@4", null);

      new Thread(WorkLoop).Start();
    }

    private void WorkLoop()
    {
      while (!_workQueue.IsCompleted)
      {
        StreetSegItem item = null;
        try
        {
          item = _workQueue.Take();
        }
        catch (InvalidOperationException) { }

        if (item != null)
        {
          StreetSeg(item);
        }
      }
    }

    private void StreetSeg(StreetSegItem item)
    {
      _hnoQueryCommand.Parameters[0].Value = item.szID;

      var houseNumbers = new List<int>();

      var houseNumberReader = _hnoQueryCommand.ExecuteReader();
      while (houseNumberReader.Read())
      {
        houseNumbers.Add(houseNumberReader.GetInt32(1));
      }
      houseNumberReader.Close();

      if (houseNumbers.Count == 0)
        return;

      var minHNO = houseNumbers.Min();
      var maxHNO = houseNumbers.Max();

      if (minHNO == maxHNO)
        return;

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

      _insertCommand.Parameters[0].Value = item.szID;
      _insertCommand.Parameters[1].Value = minHNO;
      _insertCommand.Parameters[2].Value = maxHNO;
      _insertCommand.Parameters[3].Value = scheme;
      _insertCommand.ExecuteNonQuery();

      Interlocked.Increment(ref _streetSegProgress.done);
    }
  }
}
