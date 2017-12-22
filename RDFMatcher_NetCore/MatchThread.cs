using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class MatchProgress
  {
    public int matched = 0;
    public int done = 0;
  }

  class MatchItem
  {
    public object building_id;
    public object street_zip_id;
    public string district_code;
    public string street_name;
    public string hno;
    public string hno_extension;
  }

  class InsertItem
  {
    public object building_id;
    public object street_zip_id;
    public object road_link_id;
    public string full_hno;
  }

  class MatchThread
  {
    private const string queryCommandText = "SELECT pt.ROAD_LINK_ID, ADDRESS " +
                                            "FROM NLD_RDF_ADDR addr " +
                                            "LEFT JOIN NLD_RDF_ADDR_STREET street USING (ROAD_LINK_ID) " +
                                            "LEFT JOIN NLD_RDF_POINT pt USING (ROAD_LINK_ID) " +
                                            "WHERE LEFT_DISTRICT_CODE = @1 AND STREET_FULL_NAME = @2 AND ADDRESS = @3";

    private const string insertCommandText = "INSERT INTO match_test2 VALUES(@1, @2, @3, @4)";

    private readonly MatchProgress _matchProgress;
    private readonly BlockingCollection<MatchItem> _workQueue;

    private readonly List<InsertItem> _insertBuffer = new List<InsertItem>();

    private readonly MySqlConnection _addrQueryConnection;
    private readonly MySqlConnection _insertConnection;

    private readonly MySqlCommand _addrQueryCommand;
    private readonly MySqlCommand _insertCommand;

    public MatchThread(MatchProgress matchProgress, BlockingCollection<MatchItem> workQueue)
    {
      _matchProgress = matchProgress;
      _workQueue = workQueue;

      _addrQueryConnection = new MySqlConnection(DB.ConnectionString);
      _addrQueryConnection.Open();

      _addrQueryCommand = _addrQueryConnection.CreateCommand();
      _addrQueryCommand.CommandText = queryCommandText;
      _addrQueryCommand.Prepare();

      _addrQueryCommand.Parameters.AddWithValue("@1", null);
      _addrQueryCommand.Parameters.AddWithValue("@2", null);
      _addrQueryCommand.Parameters.AddWithValue("@3", null);

      _insertConnection = new MySqlConnection(DB.ConnectionString);
      _insertConnection.Open();

      _insertCommand = _insertConnection.CreateCommand();
      _insertCommand.CommandText = insertCommandText;
      _insertCommand.Prepare();

      _insertCommand.Parameters.AddWithValue("@1", null);
      _insertCommand.Parameters.AddWithValue("@2", null);
      _insertCommand.Parameters.AddWithValue("@3", null);
      _insertCommand.Parameters.AddWithValue("@4", null);

      new Thread(workLoop).Start();
    }

    private void workLoop()
    {
      while (!_workQueue.IsCompleted)
      {
        MatchItem item = null;
        try
        {
          item = _workQueue.Take();
        }
        catch (InvalidOperationException) { }

        if (item != null)
        {
          Match(item);
        }
      }
    }

    private void Match(MatchItem item)
    {
      var full_hno = CombineHNO(item.hno, item.hno_extension);

      _addrQueryCommand.Parameters[0].Value = item.district_code;
      _addrQueryCommand.Parameters[1].Value = item.street_name;
      _addrQueryCommand.Parameters[2].Value = full_hno;
      var addrReader = _addrQueryCommand.ExecuteReader();

      var num_matches = 0;
      while (addrReader.Read())
      {
        num_matches++;

        _insertBuffer.Add(new InsertItem
        {
          building_id = item.building_id,
          street_zip_id = item.street_zip_id,
          road_link_id = addrReader.GetValue(addrReader.GetOrdinal("ROAD_LINK_ID")),
          full_hno = full_hno
        });
      }

      addrReader.Close();

      if (num_matches > 0)
      {
        Interlocked.Increment(ref _matchProgress.matched);
      }

      Interlocked.Increment(ref _matchProgress.done);

      Random rand = new Random();
      if (_insertBuffer.Count > DB.InsertBufferSize)
      {
        FlushInsertBuffer();
      }
    }

    public void FlushInsertBuffer()
    {
      Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} flushing input buffer...");
      var transaction = _insertConnection.BeginTransaction();
      _insertCommand.Connection = _insertConnection;
      _insertCommand.Transaction = transaction;
      foreach (var item in _insertBuffer)
      {
        _insertCommand.Parameters[0].Value = item.building_id;
        _insertCommand.Parameters[1].Value = item.road_link_id;
        _insertCommand.Parameters[2].Value = item.full_hno;
        _insertCommand.Parameters[3].Value = item.street_zip_id;
        _insertCommand.ExecuteNonQuery();
      }
      transaction.Commit();

      _insertBuffer.Clear();
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

