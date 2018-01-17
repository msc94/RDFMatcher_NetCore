using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.DBHelper;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  class StreetSegKooProgress
  {
    public int done = 0;
  }

  class StreetSegKooItem
  {
    public object segId;
    public int hnStart;
    public int hnEnd;
    public int scheme;
  }

  class StreetSegKooThread
  {

    private readonly StreetSegKooProgress _streetSegKooProgress;
    private readonly BlockingCollection<StreetSegKooItem> _workQueue;

    private ThreadLocal<Database> _db = new ThreadLocal<Database>(
      () =>
      {
        return new Database();
      });


    public StreetSegKooThread(StreetSegKooProgress streetSegKooProgress, BlockingCollection<StreetSegKooItem> workQueue)
    {
      _streetSegKooProgress = streetSegKooProgress;
      _workQueue = workQueue;

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

      var streetSegID = (int)segmentItem.segId;

      var debugIds = new List<int> { 763, 259, 750 };
      if (debugIds.Contains(streetSegID))
        Debugger.Break();

      // Get matched ROAD_LINK_ID
      var roadLinkIds = GetRoadLinkId(streetSegID);

      // Get RdfAddr data
      var nldRdfAddrData = GetNldRdfAddrData(roadLinkIds);

      // Remove not-matching entries from data
      // Iterate backwards, so we can remove items while iterating
      for (int i = nldRdfAddrData.Count - 1; i >= 0; i--)
      {
        RdfAddr addrItem = nldRdfAddrData[i];

        if (!Match(segmentItem, addrItem))
        {
          nldRdfAddrData.RemoveAt(i);
          continue;
        }

        addrItem.Scheme = segmentItem.scheme;
      }

      if (nldRdfAddrData.Count == 0)
      {
        // Console.WriteLine($"No NLD_RDF_DATA match for {streetSegID}");
        return;
      }

      nldRdfAddrData = SortNldRdfAddrData(nldRdfAddrData);

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

      if (segments.Count > 200)
      {
        // Console.WriteLine("WARNING: segments array too big!");
        return;
      }


      const string latFormat = "00.00000";
      const string lngFormat = "00.00000";

      for (int i = 0; i < segments.Count; i++)
      {
        // pos: 1: Begin, 2: middle, 3: end
        int pos = 2;
        if (i == 0)
          pos = 1;
        else if (i == segments.Count - 1)
          pos = 3;

        var currentSegment = segments[i];
        var latString = Utils.FloatToStringInvariantCulture(currentSegment.LAT, latFormat);
        var lngString = Utils.FloatToStringInvariantCulture(currentSegment.LON, lngFormat);

        _db.Value.InsertKoo(segmentItem.segId, i, pos, latString, lngString);
      }

      var middleSegment = segments[segments.Count / 2];
      _db.Value.UpdateSeg(segmentItem.segId, middleSegment.LAT, middleSegment.LON);
      _db.Value.InsertKooGroup(segmentItem.segId);
    }

    private List<RdfAddr> SortNldRdfAddrData(List<RdfAddr> nldRdfAddrData)
    {
      nldRdfAddrData.Sort(RdfAddr.Compare);
      return nldRdfAddrData;
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

    private IEnumerable<Segment> GetSegmentsForAddr(RdfAddr addr)
    {
      var segments = _db.Value.GetRdfSeg(addr.RoadLinkId);

      if (addr.SwappedHno)
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
      var roadLinkIds = _db.Value.GetMatchedRoadLinkIdsForStreetSeg(streetSegId);

      if (roadLinkIds.Count == 0)
      {
        Console.WriteLine($"No ROAD_LINK_ID match for street segment {streetSegId}");
      }

      return roadLinkIds;
    }

    private List<RdfAddr> GetNldRdfAddrData(List<int> roadLinkIds)
    {
      var result = new List<RdfAddr>();
      foreach (var id in roadLinkIds)
      {
        // Get Data from RdfAddr
        RdfAddr newItem = _db.Value.GetRdfAddr(id);

        newItem.SwappedHno = false;

        if (newItem.LeftHnoStart != null && newItem.LeftHnoEnd != null &&
          newItem.LeftHnoStart > newItem.LeftHnoEnd)
        {
          Utils.Swap(ref newItem.LeftHnoStart, ref newItem.LeftHnoEnd);
          newItem.SwappedHno = true;
        }

        if (newItem.RightHnoStart != null && newItem.RightHnoEnd != null &&
          newItem.RightHnoStart > newItem.RightHnoEnd)
        {
          Utils.Swap(ref newItem.RightHnoStart, ref newItem.RightHnoEnd);
          newItem.SwappedHno = true;
        }

        result.Add(newItem);
      }

      return result;
    }

    

    private bool Match(StreetSegKooItem seg, RdfAddr addr)
    {
      // TODO: Handle Scheme
      int leftStart = 0, leftEnd = 0, rightStart = 0, rightEnd = 0;
      switch (seg.scheme)
      {
        case 1:
          {

            if (addr.LeftHnoStart != null && addr.LeftHnoEnd != null)
            {
              if (addr.LeftHnoStart % 2 != 0 || addr.LeftHnoEnd % 2 != 0)
              {
                leftStart = addr.LeftHnoStart.GetValueOrDefault(0);
                leftEnd = addr.LeftHnoEnd.GetValueOrDefault(0);
              }
            }

            if (addr.RightHnoStart != null && addr.RightHnoEnd != null)
            {
              if (addr.RightHnoStart % 2 != 0 || addr.RightHnoEnd % 2 != 0)
              {
                rightStart = addr.RightHnoStart.GetValueOrDefault(0);
                rightEnd = addr.RightHnoEnd.GetValueOrDefault(0);
              }
            }

            break;

          }
        case 2:
          {

            if (addr.LeftHnoStart != null && addr.LeftHnoEnd != null)
            {
              if (addr.LeftHnoStart % 2 == 0 || addr.LeftHnoEnd % 2 == 0)
              {
                leftStart = addr.LeftHnoStart.GetValueOrDefault(0);
                leftEnd = addr.LeftHnoEnd.GetValueOrDefault(0);
              }
            }

            if (addr.RightHnoStart != null && addr.RightHnoEnd != null)
            {
              if (addr.RightHnoStart % 2 == 0 || addr.RightHnoEnd % 2 == 0)
              {
                rightStart = addr.RightHnoStart.GetValueOrDefault(0);
                rightEnd = addr.RightHnoEnd.GetValueOrDefault(0);
              }
            }

            break;

          }
        case 3:
          {
            leftStart = addr.LeftHnoStart.GetValueOrDefault(0);
            leftEnd = addr.LeftHnoEnd.GetValueOrDefault(0);
            rightStart = addr.RightHnoStart.GetValueOrDefault(0);
            rightEnd = addr.RightHnoEnd.GetValueOrDefault(0);
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