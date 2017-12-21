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
    public string district_code;
    public string street_name;
    public string hno;
    public string hno_extension;
    public string full_hno;
  }

  class MatchThread
  {
    private const string queryCommandText = "select pt.ROAD_LINK_ID, ADDRESS\n" +
                                            "from nld_rdf_addr addr\n" +
                                            "left join nld_rdf_point pt using (ROAD_LINK_ID)\n" +
                                            "where DISTRICT_CODE = @1 and STREET_FULL_NAME = @2 and ADDRESS = @3";

    private const string insertCommandText = "insert into match_test2 values(@1, @2, @3)";

    private readonly MatchProgress _matchProgress;
    private readonly BlockingCollection<MatchItem> _workQueue;

    private readonly MySqlConnection _addrQueryConnection;
    private readonly MySqlConnection _insertConnection;

    private readonly MySqlCommand _addrQueryCommand;
    private readonly MySqlCommand _insertCommand;

    public MatchThread(MatchProgress matchProgress, BlockingCollection<MatchItem> workQueue)
    {
      _matchProgress = matchProgress;
      _workQueue = workQueue;

      _addrQueryConnection = new MySqlConnection(DB.connectionString);
      _addrQueryConnection.Open();

      _addrQueryCommand = _addrQueryConnection.CreateCommand();
      _addrQueryCommand.CommandText = queryCommandText;
      _addrQueryCommand.Prepare();

      _addrQueryCommand.Parameters.AddWithValue("@1", null);
      _addrQueryCommand.Parameters.AddWithValue("@2", null);
      _addrQueryCommand.Parameters.AddWithValue("@3", null);

      _insertConnection = new MySqlConnection(DB.connectionString);
      _insertConnection.Open();

      _insertCommand = _insertConnection.CreateCommand();
      _insertCommand.CommandText = insertCommandText;
      _insertCommand.Prepare();

      _insertCommand.Parameters.AddWithValue("@1", null);
      _insertCommand.Parameters.AddWithValue("@2", null);
      _insertCommand.Parameters.AddWithValue("@3", null);

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
          match(item);
        }
      }
    }

    private void match(MatchItem item)
    {
      _addrQueryCommand.Parameters[0].Value = item.district_code;
      _addrQueryCommand.Parameters[1].Value = item.street_name;
      _addrQueryCommand.Parameters[2].Value = item.full_hno;
      var addrReader = _addrQueryCommand.ExecuteReader();

      var num_matches = 0;
      while (addrReader.Read())
      {
        num_matches++;

        _insertCommand.Parameters[0].Value = item.building_id;
        _insertCommand.Parameters[1].Value = addrReader.GetValue(0);
        _insertCommand.Parameters[2].Value = item.full_hno;
        _insertCommand.ExecuteNonQuery();
      }

      addrReader.Close();

      if (num_matches > 0)
      {
        Interlocked.Increment(ref _matchProgress.matched);
      }

      Interlocked.Increment(ref _matchProgress.done);
    }
  }
}

