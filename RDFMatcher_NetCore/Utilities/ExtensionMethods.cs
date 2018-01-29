using System;
using System.Collections.Generic;

namespace RDFMatcher_NetCore
{
  public static class ExtensionMethods
  {
    public static double ToRadians(this double val)
    {
      return (Math.PI / 180.0) * val;
    }

    public static void AddIfNotNull(this List<int> list, int? i)
    {
      if (i != null)
        list.Add(i.Value);
    }

    public static void AddIfNotNullAndEven(this List<int> list, int? i)
    {
      if (i != null && i.Value % 2 == 0)
        list.Add(i.Value);
    }

    public static void AddIfNotNullAndNotEven(this List<int> list, int? i)
    {
      if (i != null && i.Value % 2 != 0)
        list.Add(i.Value);
    }
  }
}