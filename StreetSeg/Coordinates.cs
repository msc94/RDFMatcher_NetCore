using DatabaseLibrary;
using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;

namespace StreetSeg
{
  internal class CoordinatesItem
  {
    public long SegmentId;
    public long StreetZipId;

    public int HouseNumberStart;
    public int HouseNumberEnd;
  }

  internal class Coordinates
  {
    internal static bool AddCoordinates(CoordinatesItem item)
    {
      // Get matched ROAD_LINK_ID
      var roadLinkIds = GetRoadLinkId(item.SegmentId);
      if (roadLinkIds.Count == 0)
      {
        Log.WriteLine($"No ROAD_LINK_ID match for street segment {item.SegmentId}");
        return false;
      }

      // Get RdfAddr data
      var nldRdfAddrData = GetNldRdfAddrData(roadLinkIds);
      if (nldRdfAddrData.Count == 0)
      {
        Log.WriteLine($"No RDF_DATA match for {item.SegmentId}");
        return false;
      }

      // Sort the data so the segments with lower housenumbers come first.
      // This is just so we start approx. at tht right side of the street
      nldRdfAddrData.Sort(RdfAddr.Compare);

      // For each segment, get the coordinates from the rdf_seg table
      List<Segment> segments = new List<Segment>();
      foreach (var addr in nldRdfAddrData)
      {
        segments.Add(GetSegmentsForAddr(addr));
      }

      if (segments.Count == 0)
      {
        Log.WriteLine("Segments array is empty!");
        return false;
      }

      // List of all coordinates we have so far
      var coordinates = new List<SegmentCoordinate>();

      // Only add coordinates in between segments.
      // The segments themselves will be connected by the graph algorithm below.
      // (And in the best case, there should be no holes between them)
      foreach (var segment in segments)
      {
        segment.Coordinates = Segment.AddCoordinatesInBetween(segment.Coordinates);
      }

      // Take the first item, and try to build a segment graph from it
      // Do it, as long as there are items left
      // This ensures, that we include every segment in the graph
      List<Segment> segmentList = new List<Segment>(segments);
      while (segmentList.Count > 0)
      {
        // Take the first item (=> the one with the lowest housenumber),
        // and start from there
        var startSegment = segmentList[0];
        segmentList.RemoveAt(0);

        // Let the item take its children
        startSegment.AddChildren(segmentList);

        var fullSegmentCoordinates = startSegment.GetCoordinates();
        coordinates.AddRange(fullSegmentCoordinates);
      }

      coordinates = RemoveDuplicates(coordinates);
      coordinates = ShrinkToHouseNumberRange(coordinates, item);

      // TODO: We could actually remove this
      if (coordinates.Count > 2000)
      {
        Log.WriteLine("Segments array too big!");
        return false;
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

        InsertKoo(item.SegmentId, i, pos, latString, lngString);
      }

      var middleSegment = coordinates[coordinates.Count / 2];
      UpdateSeg(item.SegmentId, middleSegment.Lat, middleSegment.Lng);
      InsertKooGroup(item.SegmentId);

      return true;
    }

    private static SegmentCoordinate GetCoordinateForHouseNumber(long streetZipId, int houseNumber)
    {
      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT AP_LAT, AP_LNG " +
        "FROM building b " +
        "WHERE b.STREET_ZIP_ID = @1 " +
        " AND b.HNO = @2 " +
        " AND AP_LAT IS NOT NULL AND AP_LNG IS NOT NULL;" +
        streetZipId, houseNumber);

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

    // Shrink the graph to the range between
    // (1) the point that is nearest to the minimal matched house number
    // (2) the point that is nearest to the maximal matched house number
    // If on of them is not matched, return a one-side-open range
    // If both of them are not matched, return the full street
    private static List<SegmentCoordinate> ShrinkToHouseNumberRange(List<SegmentCoordinate> coordinates, CoordinatesItem segmentItem)
    {
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
          var currentDistanceMin = SegmentCoordinate.DistanceBetweenInKilometers(minHnoCoordinate, currentCoordinate);
          if (currentDistanceMin < minDistanceMin)
          {
            minDistanceMin = currentDistanceMin;
            minIndex = i;
          }
        }

        if (maxHnoCoordinate != null)
        {
          var currentDistanceMax = SegmentCoordinate.DistanceBetweenInKilometers(maxHnoCoordinate, currentCoordinate);
          if (currentDistanceMax < minDistanceMax)
          {
            minDistanceMax = currentDistanceMax;
            maxIndex = i;
          }
        }
      }

      // If the minIndex is bigger than the maxIndex
      // something has gone wrong in the graph building
      // See if we can get a nice graph by reversing the whole thing
      if (minIndex > maxIndex)
      {
        Utils.Swap(ref minIndex, ref maxIndex);
        coordinates = new List<SegmentCoordinate>(coordinates);
        coordinates.Reverse();
      }

      // Add some segments at the beginning and the end,
      // This is for some countries, where house numbers in streets seem to be pretty random (NOR)...
      const int addRange = 3;
      minIndex = Math.Max(0, minIndex - addRange);
      maxIndex = Math.Min(coordinates.Count, maxIndex + addRange);

      var startIndex = (int)minIndex;
      var length = (int)(maxIndex - minIndex);
      var newCoordinates = coordinates.GetRange(startIndex, length);

      return newCoordinates;
    }

    private static List<SegmentCoordinate> RemoveDuplicates(List<SegmentCoordinate> segmentCoordinates)
    {
      var result = new List<SegmentCoordinate>();

      foreach (var segmentCoordinate in segmentCoordinates)
      {
        if (result.Contains(segmentCoordinate) == false)
          result.Add(segmentCoordinate);
      }

      return result;
    }

    private static Segment GetSegmentsForAddr(RdfAddr addr)
    {
      var segment = GetRdfSeg(addr.RoadLinkId);

      // TODO: Enable this again?
      // if (addr.SwappedHno)
      //  segment.Coordinates.Reverse();

      return segment;
    }

    private static Segment GetRdfSeg(int roadLinkId)
    {
      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT LAT, LON " +
        $"FROM {GlobalLibraryState.RdfSegTable} seg " +
        "WHERE ROAD_LINK_ID = @1 " +
        "ORDER BY SEQ_NUM;", roadLinkId);

      var segment = new Segment();
      using (reader)
      {
        while (reader.Read())
        {
          string latString = Utils.RdfCoordinateInsertDecimal(reader.GetString("LAT"));
          string lonString = Utils.RdfCoordinateInsertDecimal(reader.GetString("LON"));

          segment.Coordinates.Add(new SegmentCoordinate()
          {
            Lat = Utils.ParseDoubleInvariantCulture(latString),
            Lng = Utils.ParseDoubleInvariantCulture(lonString)
          });
        }
      }
      return segment;
    }


    private static List<int> GetRoadLinkId(long streetSegId)
    {
      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT DISTINCT m.ROAD_LINK_ID " +
        "FROM street_seg seg " +
        " JOIN match_sz msz ON (msz.SZ_ID = seg.STREET_ZIP_ID) " +
        "WHERE seg.ID = @1;",
          streetSegId);

      var roadLinkIds = new List<int>();
      using (reader)
      {
        while (reader.Read())
        {
          roadLinkIds.Add(reader.GetInt32("ROAD_LINK_ID"));
        }
      }
      return roadLinkIds;
    }

    private static List<RdfAddr> GetNldRdfAddrData(List<int> roadLinkIds)
    {
      var result = new List<RdfAddr>();
      foreach (var id in roadLinkIds)
      {
        // Get Data from RdfAddr
        RdfAddr newItem = GetRdfAddr(id);

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

    private static RdfAddr GetRdfAddr(int roadLinkID)
    {
      var reader = DatabaseHelper.ExecuteReader(GlobalLibraryState.ConnectionString,
        "SELECT LEFT_HNO_START, LEFT_HNO_END, RIGHT_HNO_START, RIGHT_HNO_END " +
        $"FROM {GlobalLibraryState.RdfAddrTable} addr " +
        "WHERE ROAD_LINK_ID = @1;", roadLinkID);

      var newItem = new RdfAddr();
      using (reader)
      {
        reader.Read();
        newItem.RoadLinkId = roadLinkID;
        newItem.LeftHnoStart = Utils.ParseIntHandleEmpty(reader.GetString("LEFT_HNO_START"));
        newItem.LeftHnoEnd = Utils.ParseIntHandleEmpty(reader.GetString("LEFT_HNO_END"));
        newItem.RightHnoStart = Utils.ParseIntHandleEmpty(reader.GetString("RIGHT_HNO_START"));
        newItem.RightHnoEnd = Utils.ParseIntHandleEmpty(reader.GetString("RIGHT_HNO_END"));
      }
      return newItem;
    }

    private static void InsertKoo(object segmentId, int ord, int pos, string lat, string lon)
    {
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "INSERT INTO street_seg_koo (STREET_SEG_ID, ORD, POS, LAT, LNG) " +
        "VALUES (@1, @2, @3, @4, @5);",
          segmentId, ord, pos, lat, lon);
    }

    private static void UpdateSeg(object segmentId, double lat, double lon)
    {
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "UPDATE street_seg " +
        "SET CENTER_LAT = @2, " +
        "CENTER_LNG = @3 " +
        "WHERE ID = @1;",
        segmentId, lat, lon);
    }

    private static void InsertKooGroup(object segmentId)
    {
      DatabaseHelper.ExecuteNonQuery(GlobalLibraryState.ConnectionString,
        "INSERT INTO street_seg_koo_group " +
        "SELECT STREET_SEG_ID, GROUP_CONCAT(CONCAT(LAT, ',', LNG) SEPARATOR '@') " +
        "FROM street_seg_koo " +
        "WHERE STREET_SEG_ID = @1 " +
        "GROUP BY STREET_SEG_ID " +
        "ORDER BY ORD;",
         segmentId);
    }
  }
}