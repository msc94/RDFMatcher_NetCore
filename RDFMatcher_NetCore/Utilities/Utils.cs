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
      if (s == "")
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

    public static float ParseFloatInvariantCulture(string s)
    {
      return float.Parse(s, CultureInfo.InvariantCulture);
    }

    public static string FloatToStringInvariantCulture(float f, string format)
    {
      return f.ToString(format, CultureInfo.InvariantCulture);
    }

    // https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
    public static float DistanceBetweenInKilometers(SegmentCoordinate s1, SegmentCoordinate s2)
    {
      float earthRadius = 6371.0f;

      float dLat = (s2.Lat - s1.Lat).ToRadians();
      float dLon = (s2.Lng - s1.Lng).ToRadians();

      float a = (float)
          (Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
          Math.Cos(s1.Lat) * Math.Cos(s2.Lat) *
          Math.Sin(dLon / 2) * Math.Sin(dLon / 2));

      float c = 2.0f * (float)Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
      float distance = earthRadius * c; // Distance in km

      return distance;
    }
  }
}
