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
  }
}
