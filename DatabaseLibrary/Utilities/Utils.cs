using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace DatabaseLibrary.Utilities
{
  public class Utils
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

    public static string RdfCoordinateInsertDecimal(string coordinateString)
    {
      var coordinateStringLength = coordinateString.Length;
      coordinateString = coordinateString.Insert(coordinateString.Length - 5, ".");
      return coordinateString;
    }
  }
}
