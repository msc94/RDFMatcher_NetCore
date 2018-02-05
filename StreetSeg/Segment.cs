using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace StreetSeg
{
  class SegmentCoordinate : IEquatable<SegmentCoordinate>
  {
    public double Lat;
    public double Lng;

    public bool Equals(SegmentCoordinate other)
    {
      const double epsilonDistance = 0.00001;
      return Math.Abs(Lat - other.Lat) < epsilonDistance &&
             Math.Abs(Lng - other.Lng) < epsilonDistance;
    }

    // https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
    public static double DistanceBetweenInKilometers(SegmentCoordinate s1, SegmentCoordinate s2)
    {
      double earthRadius = 6371.0;

      double dLat = (s2.Lat - s1.Lat).ToRadians();
      double dLon = (s2.Lng - s1.Lng).ToRadians();

      double a =
          (Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
          Math.Cos(s1.Lat) * Math.Cos(s2.Lat) *
          Math.Sin(dLon / 2) * Math.Sin(dLon / 2));

      double c = 2.0f * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
      double distance = earthRadius * c; // Distance in km

      return distance;
    }
  }

  class Segment
  {
    public List<SegmentCoordinate> Coordinates = new List<SegmentCoordinate>();

    public List<Segment> BeginChildren = new List<Segment>();
    public List<Segment> EndChildren = new List<Segment>();

    public List<Segment> Children = new List<Segment>();

    public double Length()
    {
      if (Coordinates.Count < 2)
        return 0.0;

      double totalDistance = 0.0;
      for (int i = 0; i < Coordinates.Count - 1; i++)
      {
        totalDistance += SegmentCoordinate.DistanceBetweenInKilometers(Coordinates[i], Coordinates[i + 1]);
      }
      return totalDistance;
    }

    private int ChildrenSorter(Segment a, Segment b)
    {
      return a.SegmentLength() < b.SegmentLength() ? -1 : 1;
    }

    public List<SegmentCoordinate> GetCoordinates()
    {
      var list = new List<SegmentCoordinate>();

      BeginChildren.Sort(ChildrenSorter);
      foreach (var child in BeginChildren)
      {
        list.AddRange(child.GetCoordinates());
      }

      list.AddRange(Coordinates);

      EndChildren.Sort(ChildrenSorter);
      foreach (var child in EndChildren)
      {
        list.AddRange(child.GetCoordinates());
      }

      return list;
    }

    // Switches begin and end if the length of the begin-segment is longer 
    // than the length of the end-segment
    // This helps in some cases, to give a nice way through the street that always
    // goes forward through the street
    public void RearrangeChildren()
    {
      double beginChildrenLength = 0.0;
      if (BeginChildren.Count > 0)
        beginChildrenLength = BeginChildren.Max((c) => c.SegmentLength());

      double endChildrenLength = 0.0f;
      if (EndChildren.Count > 0)
        endChildrenLength = EndChildren.Max((c) => c.SegmentLength());

      if (beginChildrenLength > endChildrenLength)
      {
        Utils.Swap(ref BeginChildren, ref EndChildren);
        Coordinates.Reverse();
      }
    }

    public void AddChildren(List<Segment> segmentList)
    {
      if (segmentList.Count == 0)
        return;

      // If we add a segment to our children, remove it from the (graph-)global segment list
      for (int i = segmentList.Count - 1; i >= 0; i--)
      {
        var currentSegment = segmentList[i];
        if (AddToLists(currentSegment))
        {
          segmentList.RemoveAt(i);
        }
      }

      // Now try if we can build up the graph further by letting our children add segments
      Children = new List<Segment>(BeginChildren.Union(EndChildren));
      foreach (var child in Children)
      {
        child.AddChildren(segmentList);
      }

      RearrangeChildren();
    }

    public double SegmentLength()
    {
      double segmentLength = Length();
      foreach (var child in Children)
      {
        segmentLength += child.SegmentLength();
      }
      return segmentLength;
    }

    public bool AddToLists(Segment segment)
    {
      SegmentCoordinate firstCoordinateSelf = Coordinates[0];
      SegmentCoordinate lastCoordinateSelf = Coordinates[Coordinates.Count - 1];

      SegmentCoordinate firstCoordinateOther = segment.Coordinates[0];
      SegmentCoordinate lastCoordinateOther = segment.Coordinates[segment.Coordinates.Count - 1];

      if (firstCoordinateSelf.Equals(firstCoordinateOther))
      {
        BeginChildren.Add(segment);
        return true;
      }

      if (firstCoordinateSelf.Equals(lastCoordinateOther))
      {
        segment.Coordinates.Reverse();
        BeginChildren.Add(segment);
        return true;
      }

      if (lastCoordinateSelf.Equals(firstCoordinateOther))
      {
        EndChildren.Add(segment);
        return true;
      }

      if (lastCoordinateSelf.Equals(lastCoordinateOther))
      {
        segment.Coordinates.Reverse();
        EndChildren.Add(segment);
        return true;
      }

      return false;
    }

    private const double _defaultMinDistance = 0.011;
    public static List<SegmentCoordinate> 
      AddCoordinatesInBetween(List<SegmentCoordinate> coordinates, double minDistance = _defaultMinDistance)
    {
      var result = new List<SegmentCoordinate>();

      for (int i = 0; i < coordinates.Count; i++)
      {
        var currentSegment = coordinates[i];

        // Always add the current segment
        // This will also include the last one
        result.Add(currentSegment);

        // Check if we have a following segment
        if (i + 1 < coordinates.Count)
        {
          var nextSegment = coordinates[i + 1];
          double distance = SegmentCoordinate.DistanceBetweenInKilometers(currentSegment, nextSegment);
          if (distance > minDistance)
          {
            int numCoordinatesToAdd = (int)(distance / minDistance);
            result.AddRange(CreateCoordinatesBetween(currentSegment, nextSegment, distance, numCoordinatesToAdd));
          }
        }
      }

      return result;
    }

    public static List<SegmentCoordinate> CreateCoordinatesBetween(SegmentCoordinate s1, SegmentCoordinate s2, double distance, int numCoordinatesToAdd)
    {
      double forwardLat = (s2.Lat - s1.Lat) / (numCoordinatesToAdd + 1);
      double forwardLon = (s2.Lng - s1.Lng) / (numCoordinatesToAdd + 1);

      double startLat = s1.Lat;
      double startLon = s1.Lng;

      var newCoordinates = new List<SegmentCoordinate>();
      // i = 0 would be our starting point
      for (int i = 1; i <= numCoordinatesToAdd; i++)
      {
        newCoordinates.Add(new SegmentCoordinate
        {
          Lat = startLat + forwardLat * i,
          Lng = startLon + forwardLon * i
        });
      }
      return newCoordinates;
    }

  }
}