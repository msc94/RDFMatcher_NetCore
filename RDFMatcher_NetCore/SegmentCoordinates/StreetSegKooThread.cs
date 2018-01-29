﻿using System;
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

  // TODO:
  // 1. Evtl nur innerhalb von Segmenten aus RDF neue Punkte einfuegen?
  // 2. Segmente haben einen gemeinsamen Punkt... evtl kann man nach diesem sortieren?

  class StreetSegKooThread : WorkerThread<StreetSegKooItem>
  {
    public StreetSegKooThread(WorkerThreadsProgress workerThreadsProgress, BlockingCollection<StreetSegKooItem> workQueue)
      : base(workerThreadsProgress, workQueue)
    {
    }

    public override WorkResult Work(StreetSegKooItem segmentItem)
    {
      var streetSegID = segmentItem.SegmentId;

      // Get matched ROAD_LINK_ID
      var roadLinkIds = GetRoadLinkId(streetSegID);

      if (roadLinkIds.Count == 0)
      {
        Log.WriteLine($"No ROAD_LINK_ID match for street segment {streetSegID}");
        return WorkResult.Failed;
      }

      // Get RdfAddr data
      var nldRdfAddrData = GetNldRdfAddrData(roadLinkIds);

      if (nldRdfAddrData.Count == 0)
      {
        Log.WriteLine($"No NLD_RDF_DATA match for {streetSegID}");
        return WorkResult.Failed;
      }

      nldRdfAddrData = SortNldRdfAddrData(nldRdfAddrData);

      List<Segment> segments = new List<Segment>();
      foreach (var addr in nldRdfAddrData)
      {
        segments.Add(GetSegmentsForAddr(addr));
      }

      if (segments.Count == 0)
      {
        Log.WriteLine("WARNING: segments array is empty!");
        return WorkResult.Failed;
      }

      var coordinates = new List<SegmentCoordinate>();
      foreach (var segment in segments)
      {
        segment.Coordinates = Segment.AddCoordinatesInBetween(segment.Coordinates);
        // coordinates.AddRange(segment.Coordinates);
      }

      List<Segment> segmentList = new List<Segment>(segments);
      while (segmentList.Count > 0)
      {
        // Build graph from this segment
        Segment startSegment = segmentList[0];
        segmentList.RemoveAt(0);

        startSegment.AddChildren(segmentList);

        var fullSegmentCoordinates = startSegment.GetCoordinates();
        coordinates.AddRange(fullSegmentCoordinates);
      }

      coordinates = RemoveDuplicates(coordinates);
      coordinates = ShrinkToHouseNumberRange(coordinates, segmentItem);

      if (coordinates.Count > 1000)
      {
        Log.WriteLine("WARNING: segments array too big!");
        return WorkResult.Failed;
      }


      const string latFormat = "00.00000";
      const string lngFormat = "00.00000";

      for (int i = 0; i < coordinates.Count; i++)
      {
        // pos: 1: Begin, 2: middle, 3: end
        int pos = 2;
        if (i == 0)
          pos = 1;
        else if (i == coordinates.Count - 1)
          pos = 3;

        var currentSegment = coordinates[i];
        var latString = Utils.DoubleToStringInvariantCulture(currentSegment.Lat, latFormat);
        var lngString = Utils.DoubleToStringInvariantCulture(currentSegment.Lng, lngFormat);

        _db.InsertKoo(segmentItem.SegmentId, i, pos, latString, lngString);
      }

      var middleSegment = coordinates[coordinates.Count / 2];
      _db.UpdateSeg(segmentItem.SegmentId, middleSegment.Lat, middleSegment.Lng);
      _db.InsertKooGroup(segmentItem.SegmentId);

      return WorkResult.Successful;
    }

    private SegmentCoordinate GetCoordinateForHouseNumber(long streetZipId, int houseNumber)
    {
      var reader = MySqlHelper.ExecuteReader(DB.ConnectionString,
        "SELECT AP_LAT, AP_LNG " +
        "FROM building b " +
        "WHERE b.STREET_ZIP_ID = @1 AND b.HNO = @2 AND " +
        "AP_LAT IS NOT NULL AND AP_LNG IS NOT NULL;",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", streetZipId),
          new MySqlParameter("@2", houseNumber)
        });

      using (reader)
      {
        if (!reader.Read())
          return null;

        return new SegmentCoordinate
        {
          Lat = Utils.ParseDoubleInvariantCulture(reader.GetString("AP_LAT")),
          Lng = Utils.ParseDoubleInvariantCulture(reader.GetString("AP_LNG"))
        };
      }
    }

    private List<SegmentCoordinate> ShrinkToHouseNumberRange(List<SegmentCoordinate> coordinates, StreetSegKooItem segmentItem)
    {
      // Get the matched coordinate for the min/max housenumber
      var minHnoCoordinate = GetCoordinateForHouseNumber(segmentItem.StreetZipId, segmentItem.HouseNumberStart);
      var maxHnoCoordinate = GetCoordinateForHouseNumber(segmentItem.StreetZipId, segmentItem.HouseNumberEnd);

      var result = new List<SegmentCoordinate>();

      long minIndex = 0;
      double minDistanceMin = double.MaxValue;

      long maxIndex = coordinates.Count;
      double minDistanceMax = double.MaxValue;

      for (int i = 0; i < coordinates.Count; i++)
      {
        var currentCoordinate = coordinates[i];

        if (minHnoCoordinate != null)
        {
          var currentDistanceMin = Utils.DistanceBetweenInKilometers(minHnoCoordinate, currentCoordinate);
          if (currentDistanceMin < minDistanceMin)
          {
            minDistanceMin = currentDistanceMin;
            minIndex = i;
          }
        }

        if (maxHnoCoordinate != null)
        {
          var currentDistanceMax = Utils.DistanceBetweenInKilometers(maxHnoCoordinate, currentCoordinate);
          if (currentDistanceMax < minDistanceMax)
          {
            minDistanceMax = currentDistanceMax;
            maxIndex = i;
          }
        }
      }

      if (minIndex == maxIndex)
      {
        if (minIndex > 0)
          --minIndex;
        if (maxIndex < coordinates.Count)
          ++maxIndex;
      }

      bool reverseRange = false;
      if (minIndex > maxIndex)
      {
        Utils.Swap(ref minIndex, ref maxIndex);
        reverseRange = true;
      }

      var startIndex = (int)minIndex;
      var length = (int)(maxIndex - minIndex);
      var newCoordinates = coordinates.GetRange(startIndex, length);

      if (reverseRange)
        newCoordinates.Reverse();

      return newCoordinates;
    }

    private List<RdfAddr> SortNldRdfAddrData(List<RdfAddr> nldRdfAddrData)
    {
      nldRdfAddrData.Sort(RdfAddr.Compare);
      return nldRdfAddrData;
    }

    private List<SegmentCoordinate> RemoveDuplicates(List<SegmentCoordinate> segments)
    {
      var result = new List<SegmentCoordinate>();

      foreach (var segment in segments)
      {
        if (result.Contains(segment) == false)
          result.Add(segment);
      }

      return result;
    }

    private Segment GetSegmentsForAddr(RdfAddr addr)
    {
      var segment = _db.GetRdfSeg(addr.RoadLinkId);

      if (addr.SwappedHno)
        segment.Coordinates.Reverse();

      return segment;
    }


    private List<int> GetRoadLinkId(long streetSegId)
    {
      var roadLinkIds = _db.GetMatchedRoadLinkIdsForStreetSeg(streetSegId);
      return roadLinkIds;
    }

    private List<RdfAddr> GetNldRdfAddrData(List<int> roadLinkIds)
    {
      var result = new List<RdfAddr>();
      foreach (var id in roadLinkIds)
      {
        // Get Data from RdfAddr
        RdfAddr newItem = _db.GetRdfAddr(id);

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
      int leftStart = -1, leftEnd = -1, rightStart = -1, rightEnd = -1;
      switch (seg.Scheme)
      {
        case 1:
          {

            if (addr.LeftHnoStart != null && addr.LeftHnoEnd != null)
            {
              if (addr.LeftHnoStart % 2 != 0 || addr.LeftHnoEnd % 2 != 0)
              {
                leftStart = addr.LeftHnoStart.GetValueOrDefault(-1);
                leftEnd = addr.LeftHnoEnd.GetValueOrDefault(-1);
              }
            }

            if (addr.RightHnoStart != null && addr.RightHnoEnd != null)
            {
              if (addr.RightHnoStart % 2 != 0 || addr.RightHnoEnd % 2 != 0)
              {
                rightStart = addr.RightHnoStart.GetValueOrDefault(-1);
                rightEnd = addr.RightHnoEnd.GetValueOrDefault(-1);
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
                leftStart = addr.LeftHnoStart.GetValueOrDefault(-1);
                leftEnd = addr.LeftHnoEnd.GetValueOrDefault(-1);
              }
            }

            if (addr.RightHnoStart != null && addr.RightHnoEnd != null)
            {
              if (addr.RightHnoStart % 2 == 0 || addr.RightHnoEnd % 2 == 0)
              {
                rightStart = addr.RightHnoStart.GetValueOrDefault(-1);
                rightEnd = addr.RightHnoEnd.GetValueOrDefault(-1);
              }
            }

            break;

          }
        case 3:
          {
            leftStart = addr.LeftHnoStart.GetValueOrDefault(-1);
            leftEnd = addr.LeftHnoEnd.GetValueOrDefault(-1);
            rightStart = addr.RightHnoStart.GetValueOrDefault(-1);
            rightEnd = addr.RightHnoEnd.GetValueOrDefault(-1);
            break;
          }
      }

      // Add a tolerance to the segments house numbers
      int hnStart = seg.HouseNumberStart;
      if (seg.HouseNumberStart == 2)
        hnStart = seg.HouseNumberStart - 1;
      else if (seg.HouseNumberStart > 2)
        hnStart = seg.HouseNumberStart - 2;

      int hnEnd = seg.HouseNumberEnd + 3;

      if (hnStart <= leftStart && hnEnd >= leftEnd)
        return true;

      if (hnStart <= rightStart && hnEnd >= rightEnd)
        return true;

      return false;
    }


  }
}