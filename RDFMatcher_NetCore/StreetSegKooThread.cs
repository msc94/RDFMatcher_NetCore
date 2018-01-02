using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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

  enum MatchingSide
  {
    UNKNOWN,
    RIGHT,
    LEFT
  }

  class NLD_RDF_ADDR
  {
    public int ROAD_LINK_ID;

    public int? leftHnoStart;
    public int? leftHnoEnd;
    public int? rightHnoStart;
    public int? rightHnoEnd;

    public int scheme;

    // If we have swapped HNO's we need to reverse the NLD_RDF_SEG data later
    public bool swappedHno;

    public int getMinHno()
    {
      var matchingHnos = new List<int>();

      switch (scheme)
      {
        case 3:
          matchingHnos.AddIfNotNull(leftHnoStart);
          matchingHnos.AddIfNotNull(leftHnoEnd);
          matchingHnos.AddIfNotNull(rightHnoStart);
          matchingHnos.AddIfNotNull(rightHnoEnd);
          break;
        case 2:
          matchingHnos.AddIfNotNullAndEven(leftHnoStart);
          matchingHnos.AddIfNotNullAndEven(leftHnoEnd);
          matchingHnos.AddIfNotNullAndEven(rightHnoStart);
          matchingHnos.AddIfNotNullAndEven(rightHnoEnd);
          break;
        case 1:
          matchingHnos.AddIfNotNullAndNotEven(leftHnoStart);
          matchingHnos.AddIfNotNullAndNotEven(leftHnoEnd);
          matchingHnos.AddIfNotNullAndNotEven(rightHnoStart);
          matchingHnos.AddIfNotNullAndNotEven(rightHnoEnd);
          break;
      }

      return matchingHnos.Min();
    }

    public static int Compare(NLD_RDF_ADDR a1, NLD_RDF_ADDR a2)
    {
      return a1.getMinHno() - a2.getMinHno();
    }
  }

  class Segment : IEquatable<Segment>
  {
    public float LAT;
    public float LON;

    public bool Equals(Segment other)
    {
      const float epsilonDistance = 0.00001f;
      return Math.Abs(LAT - other.LAT) < epsilonDistance &&
             Math.Abs(LON - other.LON) < epsilonDistance;
    }
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
                                       "WHERE ROAD_LINK_ID = @1 " +
                                       "ORDER BY SEQ_NUM";
    private readonly MySqlConnection _getSegConnection;
    private readonly MySqlCommand _getSegCommand;

    private const string _insertKooText = "INSERT INTO street_seg_koo (STREET_SEG_ID, ORD, POS, LAT, LNG) " +
                                          "VALUES (@1, @2, @3, @4, @5)";
    private readonly MySqlConnection _insertKooConnection;
    private readonly MySqlCommand _insertKooCommand;

    private const string _updateSegText = "UPDATE street_seg " +
                                          "SET CENTER_LAT = @2, " +
                                          "CENTER_LNG = @3 " +
                                          "WHERE ID = @1";
    private readonly MySqlConnection _updateSegConnection;
    private readonly MySqlCommand _updateSegCommand;

    private const string _createKooGroupText = "INSERT INTO street_seg_koo_group " +
                                                "SELECT STREET_SEG_ID, GROUP_CONCAT(CONCAT(LAT, ',', LNG) SEPARATOR '@') " +
                                                "FROM street_seg_koo " +
                                                "WHERE STREET_SEG_ID = @1 " +
                                                "GROUP BY STREET_SEG_ID " +
                                                "ORDER BY ORD ";
    private readonly MySqlConnection _createKooGroupConnection;
    private readonly MySqlCommand _createKooGroupCommand;


    // TODO: Koo_groups

    private readonly StreetSegKooProgress _streetSegKooProgress;
    private readonly BlockingCollection<StreetSegKooItem> _workQueue;

    private MySqlConnection createConnection()
    {
      var conn = new MySqlConnection(DB.ConnectionString);
      conn.Open();
      return conn;
    }

    private MySqlCommand createCommand(MySqlConnection conn, string command, int numParameters)
    {
      var comm = conn.CreateCommand();

      comm.CommandText = command;

      for (int i = 0; i < numParameters; i++)
      {
        comm.Parameters.AddWithValue($"@{i + 1}", null);
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
      _insertKooCommand = createCommand(_insertKooConnection, _insertKooText, 5);

      _updateSegConnection = createConnection();
      _updateSegCommand = createCommand(_updateSegConnection, _updateSegText, 3);

      _createKooGroupConnection = createConnection();
      _createKooGroupCommand = createCommand(_createKooGroupConnection, _createKooGroupText, 1);

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

    private void StreetSegKoo(StreetSegKooItem segmentItem)
    {
      Interlocked.Increment(ref _streetSegKooProgress.done);

      var streetSegID = (int)segmentItem.szID;

      // Get matched ROAD_LINK_ID
      var roadLinkIds = GetRoadLinkId(streetSegID);

      // Get NLD_RDF_ADDR data
      var nldRdfAddrData = GetNldRdfAddrData(roadLinkIds);

      // Remove not-matching entries from data
      // Iterate backwards, so we can remove items while iterating
      for (int i = nldRdfAddrData.Count - 1; i >= 0; i--)
      {
        NLD_RDF_ADDR addrItem = nldRdfAddrData[i];


        if (!Match(segmentItem, addrItem))
        {
          nldRdfAddrData.RemoveAt(i);
          continue;
        }

        addrItem.scheme = segmentItem.scheme;
      }

      if (nldRdfAddrData.Count == 0)
      {
        // Console.WriteLine($"No NLD_RDF_DATA match for {streetSegID}");
        return;
      }

      nldRdfAddrData.Sort(NLD_RDF_ADDR.Compare);

      List<Segment> segments = new List<Segment>();
      foreach (var addr in nldRdfAddrData)
      {
        segments.AddRange(GetSegmentsForAddr(addr));
      }

      segments = RemoveDuplicates(segments);
      segments = AddSegmentsInBetween(segments);

      if (segments.Count == 0)
      {
        Console.WriteLine("WARNING: segments array is empty!");
        return;
      }

      const string latFormat = "00.00000";
      const string lngFormat = "0.00000";

      for (int i = 0; i < segments.Count; i++)
      {
        // pos: 1: Begin, 2: middle, 3: end
        int pos = 2;
        if (i == 0)
          pos = 1;
        else if (i == segments.Count - 1)
          pos = 3;

        var currentSegment = segments[i];

        _insertKooCommand.Parameters[0].Value = segmentItem.szID;
        _insertKooCommand.Parameters[1].Value = i; // ORD
        _insertKooCommand.Parameters[2].Value = pos; // POS
        _insertKooCommand.Parameters[3].Value = currentSegment.LAT.ToString(latFormat).Replace(',', '.'); // LAT
        _insertKooCommand.Parameters[4].Value = currentSegment.LON.ToString(lngFormat).Replace(',', '.'); // LON
        _insertKooCommand.ExecuteNonQuery();
      }

      var middleSegment = segments[segments.Count / 2];

      _updateSegCommand.Parameters[0].Value = segmentItem.szID;
      _updateSegCommand.Parameters[1].Value = middleSegment.LAT;
      _updateSegCommand.Parameters[2].Value = middleSegment.LON;
      _updateSegCommand.ExecuteNonQuery();

      _createKooGroupCommand.Parameters[0].Value = segmentItem.szID;
      _createKooGroupCommand.ExecuteNonQuery();
    }

    private List<Segment> RemoveDuplicates(List<Segment> segments)
    {
      var result = new List<Segment>();

      foreach (var segment in segments)
      {
        if (result.Contains(segment) == false)
          result.Add(segment);
      }

      return result;
    }

    private IEnumerable<Segment> GetSegmentsForAddr(NLD_RDF_ADDR addr)
    {
      var segments = new List<Segment>();

      _getSegCommand.Parameters[0].Value = addr.ROAD_LINK_ID;
      using (var reader = _getSegCommand.ExecuteReader())
      {
        while (reader.Read())
        {
          string latString = reader.GetString("LAT").Insert(2, ",");
          string lonString = reader.GetString("LON").Insert(1, ",");
          segments.Add(new Segment()
          {
            LAT = float.Parse(latString),
            LON = float.Parse(lonString)
          });
        }
      }

      if (addr.swappedHno)
        segments.Reverse();

      return segments;
    }

    // https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
    private float DistanceBetweenInKilometers(Segment s1, Segment s2)
    {
      float earthRadius = 6371.0f;

      float dLat = (s2.LAT - s1.LAT).ToRadians();
      float dLon = (s2.LON - s1.LON).ToRadians();

      float a = (float)
          (Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
          Math.Cos(s1.LAT) * Math.Cos(s2.LAT) *
          Math.Sin(dLon / 2) * Math.Sin(dLon / 2));

      float c = 2.0f * (float)Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
      float distance = earthRadius * c; // Distance in km

      return distance;
    }

    private const float minDistance = 0.011f;
    private List<Segment> AddSegmentsInBetween(List<Segment> segments)
    {
      var result = new List<Segment>();

      for (int i = 0; i < segments.Count; i++)
      {
        var currentSegment = segments[i];

        // Always add the current segment
        // This will also include the last one
        result.Add(currentSegment);

        // Check if we have a following segment
        if (i + 1 < segments.Count)
        {
          var nextSegment = segments[i + 1];
          float distance = DistanceBetweenInKilometers(currentSegment, nextSegment);
          if (distance > minDistance)
          {
            int numSegmentsToAdd = (int)(distance / minDistance);
            result.AddRange(CreateSegmentsBetween(currentSegment, nextSegment, distance, numSegmentsToAdd));
          }
        }
      }

      return result;
    }

    private List<Segment> CreateSegmentsBetween(Segment s1, Segment s2, float distance, int numSegmentsToAdd)
    {
      float forwardLat = (s2.LAT - s1.LAT) / (numSegmentsToAdd + 1);
      float forwardLon = (s2.LON - s1.LON) / (numSegmentsToAdd + 1);

      float startLat = s1.LAT;
      float startLon = s1.LON;

      var newSegments = new List<Segment>();
      for (int i = 1; i <= numSegmentsToAdd; i++) // i = 0 would be our starting point
      {
        newSegments.Add(new Segment
        {
          LAT = startLat + forwardLat * i,
          LON = startLon + forwardLon * i
        });
      }
      return newSegments;
    }

    private List<int> GetRoadLinkId(int streetSegId)
    {
      var roadLinkIds = new List<int>();

      _getRoadLinkIDCommand.Parameters[0].Value = streetSegId;
      using (var reader = _getRoadLinkIDCommand.ExecuteReader())
      {
        while (reader.Read())
        {
          roadLinkIds.Add(reader.GetInt32("ROAD_LINK_ID"));
        }
      }

      if (roadLinkIds.Count == 0)
      {
        Console.WriteLine($"No ROAD_LINK_ID match for street segment {streetSegId}");
      }

      return roadLinkIds;
    }

    private List<NLD_RDF_ADDR> GetNldRdfAddrData(List<int> roadLinkIds)
    {
      var result = new List<NLD_RDF_ADDR>();
      foreach (var id in roadLinkIds)
      {
        // Get Data from NLD_RDF_ADDR
        NLD_RDF_ADDR newItem = new NLD_RDF_ADDR();
        newItem.ROAD_LINK_ID = id;

        _getAddrCommand.Parameters[0].Value = id;
        using (var reader = _getAddrCommand.ExecuteReader())
        {
          reader.Read();
          newItem.leftHnoStart = ParseIntHandleEmpty(reader.GetString("LEFT_HNO_START"));
          newItem.leftHnoEnd = ParseIntHandleEmpty(reader.GetString("LEFT_HNO_END"));
          newItem.rightHnoStart = ParseIntHandleEmpty(reader.GetString("RIGHT_HNO_START"));
          newItem.rightHnoEnd = ParseIntHandleEmpty(reader.GetString("RIGHT_HNO_END"));
        }

        newItem.swappedHno = false;

        if (newItem.leftHnoStart != null && newItem.leftHnoEnd != null &&
          newItem.leftHnoStart > newItem.leftHnoEnd)
        {
          Swap(ref newItem.leftHnoStart, ref newItem.leftHnoEnd);
          newItem.swappedHno = true;
        }

        if (newItem.rightHnoStart != null && newItem.rightHnoEnd != null &&
          newItem.rightHnoStart > newItem.rightHnoEnd)
        {
          Swap(ref newItem.rightHnoStart, ref newItem.rightHnoEnd);
          newItem.swappedHno = true;
        }

        result.Add(newItem);
      }

      return result;
    }

    private int? ParseIntHandleEmpty(string s)
    {
      if (s == "")
        return null;
      if (!int.TryParse(s, out var i))
        return null;
      return i;
    }

    private void Swap<T>(ref T lhs, ref T rhs)
    {
      var temp = lhs;
      lhs = rhs;
      rhs = temp;
    }

    private bool Match(StreetSegKooItem seg, NLD_RDF_ADDR addr)
    {
      // TODO: Handle Scheme
      int leftStart = 0, leftEnd = 0, rightStart = 0, rightEnd = 0;
      switch (seg.scheme)
      {
        case 1:
          {

            if (addr.leftHnoStart != null && addr.leftHnoEnd != null)
            {
              if (addr.leftHnoStart % 2 != 0 || addr.leftHnoEnd % 2 != 0)
              {
                leftStart = addr.leftHnoStart.GetValueOrDefault(0);
                leftEnd = addr.leftHnoEnd.GetValueOrDefault(0);
              }
            }

            if (addr.rightHnoStart != null && addr.rightHnoEnd != null)
            {
              if (addr.rightHnoStart % 2 != 0 || addr.rightHnoEnd % 2 != 0)
              {
                rightStart = addr.rightHnoStart.GetValueOrDefault(0);
                rightEnd = addr.rightHnoEnd.GetValueOrDefault(0);
              }
            }

            break;

          }
        case 2:
          {

            if (addr.leftHnoStart != null && addr.leftHnoEnd != null)
            {
              if (addr.leftHnoStart % 2 == 0 || addr.leftHnoEnd % 2 == 0)
              {
                leftStart = addr.leftHnoStart.GetValueOrDefault(0);
                leftEnd = addr.leftHnoEnd.GetValueOrDefault(0);
              }
            }

            if (addr.rightHnoStart != null && addr.rightHnoEnd != null)
            {
              if (addr.rightHnoStart % 2 == 0 || addr.rightHnoEnd % 2 == 0)
              {
                rightStart = addr.rightHnoStart.GetValueOrDefault(0);
                rightEnd = addr.rightHnoEnd.GetValueOrDefault(0);
              }
            }

            break;

          }
        case 3:
          {
            leftStart = addr.leftHnoStart.GetValueOrDefault(0);
            leftEnd = addr.leftHnoEnd.GetValueOrDefault(0);
            rightStart = addr.rightHnoStart.GetValueOrDefault(0);
            rightEnd = addr.rightHnoEnd.GetValueOrDefault(0);
            break;
          }
      }

      // Add a tolerance to the segments house numbers
      int hnStart = seg.hnStart;
      if (seg.hnStart == 2)
        hnStart = seg.hnStart - 1;
      else if (seg.hnStart > 2)
        hnStart = seg.hnStart - 2;

      int hnEnd = seg.hnEnd + 3;

      if (hnStart <= leftStart && hnEnd >= leftEnd)
        return true;

      if (hnStart <= rightStart && hnEnd >= rightEnd)
        return true;

      return false;
    }

  }
}