using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class StreetSegKooProgress
  {
    public int done = 0;
  }

  class StreetSegKooItem
  {
    public object szID;
    public int hnStart;
    public int hnEnd;
    public int scheme;
  }

  class StreetSegKooThread
  {
    private const string _getRoadLinkIDText = "SELECT DISTINCT m.ROAD_LINK_ID " +
                                              "FROM match_test m " +
                                              "  LEFT JOIN building b ON m.BUILDING_ID = b.ID " +
                                              "  LEFT JOIN street_seg seg ON b.STREET_ZIP_ID = seg.STREET_ZIP_ID " +
                                              "WHERE seg.ID = @1";
    private readonly MySqlConnection _getRoadLinkIDConnection;
    private readonly MySqlCommand _getRoadLinkIDCommand;


    private const string _getAddrText = "SELECT LEFT_HNO_START, LEFT_HNO_END, RIGHT_HNO_START, RIGHT_HNO_END " +
                                        "FROM NLD_RDF_ADDR addr " +
                                        "WHERE ROAD_LINK_ID = @1";
    private readonly MySqlConnection _getAddrConnection;
    private readonly MySqlCommand _getAddrCommand;

    private const string _getSegText = "SELECT LAT, LON " +
                                       "FROM NLD_RDF_SEG seg " +
                                       "WHERE ROAD_LINK_ID = @1";
    private readonly MySqlConnection _getSegConnection;
    private readonly MySqlCommand _getSegCommand;

    private const string _insertKooText = "INSERT INTO street_seg_koo (STREET_SEG_ID, ORD, POS, LAT, LNG, CENTER_LAT, CENTER_LNG) " +
                                          "VALUES (@1, @2, @3, @4, @5, @6, @7)";
    private readonly MySqlConnection _insertKooConnection;
    private readonly MySqlCommand _insertKooCommand;

    // TODO: Koo_groups

    private readonly StreetSegKooProgress _streetSegKooProgress;
    private readonly BlockingCollection<StreetSegKooItem> _workQueue;

    private MySqlConnection createConnection()
    {
      var conn = new MySqlConnection(DB.connectionString);
      conn.Open();
      return conn;
    }

    private MySqlCommand createCommand(MySqlConnection conn, string command, int numParameters)
    {
      var comm = conn.CreateCommand();

      comm.CommandText = command;

      for (int i = 0; i < numParameters; i++)
      {
        comm.Parameters.AddWithValue($"@{i+1}", null);
      }

      return comm;
    }

    public StreetSegKooThread(StreetSegKooProgress streetSegKooProgress, BlockingCollection<StreetSegKooItem> workQueue)
    {
      _streetSegKooProgress = streetSegKooProgress;
      _workQueue = workQueue;

      _getRoadLinkIDConnection = createConnection();
      _getRoadLinkIDCommand = createCommand(_getRoadLinkIDConnection, _getRoadLinkIDText, 1);

      _getAddrConnection = createConnection();
      _getAddrCommand = createCommand(_getAddrConnection, _getAddrText, 1);

      _getSegConnection = createConnection();
      _getSegCommand = createCommand(_getSegConnection, _getSegText, 1);

      _insertKooConnection = createConnection();
      _insertKooCommand = createCommand(_insertKooConnection, _insertKooText, 7);

      new Thread(WorkLoop).Start();
    }

    private void WorkLoop()
    {
      while (!_workQueue.IsCompleted)
      {
        StreetSegKooItem item = null;
        try
        {
          item = _workQueue.Take();
        }
        catch (InvalidOperationException) { }

        if (item != null)
        {
          StreetSegKoo(item);
        }
      }
    }

    private void StreetSegKoo(StreetSegKooItem item)
    {
      var streetSegID = (int) item.szID;
      
      // Get matched ROAD_LINK_ID
      var roadLinkID = getRoadLinkID(streetSegID);

      int leftHnoStart, leftHnoEnd, rightHnoStart, rightHnoEnd;

      _getAddrCommand.Parameters[0].Value = roadLinkID;
      using (var reader = _getAddrCommand.ExecuteReader())
      {
        reader.Read();
        // We are using TryParse, so we can gracefully handle empty fields ('')
        leftHnoStart = int.TryParse(reader.GetString(0), out leftHnoStart) ? leftHnoStart : 0;
        leftHnoEnd = int.TryParse(reader.GetString(1), out leftHnoEnd) ? leftHnoEnd : 0;
        rightHnoStart = int.TryParse(reader.GetString(2), out rightHnoStart) ? rightHnoStart : 0;
        rightHnoEnd = int.TryParse(reader.GetString(3), out rightHnoEnd) ? rightHnoEnd : 0;
      }

      if (leftHnoStart > leftHnoEnd)
        Swap(ref leftHnoStart, ref leftHnoEnd);

      if (rightHnoStart > rightHnoEnd)
        Swap(ref rightHnoStart, ref rightHnoEnd);



      Interlocked.Increment(ref _streetSegKooProgress.done);
    }


    private int getRoadLinkID(int streetSegId)
    {
      _getRoadLinkIDCommand.Parameters[0].Value = streetSegId;

      var numMatches = 0;
      var roadLinkID = 0;

      using (var reader = _getRoadLinkIDCommand.ExecuteReader())
      {
        while (reader.Read())
        {
          numMatches++;
          roadLinkID = reader.GetInt32(0);
        }
      }

      if (numMatches > 0)
      {
        Console.WriteLine($"Warning: More than 1 ROAD_LINK_ID match for street segment {streetSegId}");
      }
      if (numMatches == 0)
      {
        Console.WriteLine($"No ROAD_LINK_ID match for street segment {streetSegId}");
      }

      return roadLinkID;
    }

    private void Swap<T>(ref T lhs, ref T rhs)
    {
      var temp = lhs;
      lhs = rhs;
      rhs = temp;
    }

  }
}