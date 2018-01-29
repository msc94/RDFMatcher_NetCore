using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace RDFMatcher_NetCore.Utilities
{
  class Utils
  {
    public static int? ParseIntHandleEmpty(string s)
    {
      if (s.Length == 0)
        return null;
      if (!int.TryParse(s, out var i))
        return null;
      return i;
    }

    public static void Swap<T>(ref T lhs, ref T rhs)
    {
      var temp = lhs;
      lhs = rhs;
      rhs = temp;
    }

    public static double ParseDoubleInvariantCulture(string s)
    {
      return double.Parse(s, CultureInfo.InvariantCulture);
    }

    public static string DoubleToStringInvariantCulture(double d, string format)
    {
      return d.ToString(format, CultureInfo.InvariantCulture);
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
}
