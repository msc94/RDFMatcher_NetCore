using System.Collections.Generic;
using System.Linq;

namespace RDFMatcher_NetCore
{
  class RdfAddr
  {
    public int RoadLinkId;

    public int? LeftHnoStart;
    public int? LeftHnoEnd;
    public int? RightHnoStart;
    public int? RightHnoEnd;

    public int Scheme;

    // If we have swapped HNO's we need to reverse the segment data later
    public bool SwappedHno;

    public int? GetMinHno()
    {
      var matchingHnos = new List<int?>();

      matchingHnos.Add(LeftHnoStart);
      matchingHnos.Add(LeftHnoEnd);
      matchingHnos.Add(RightHnoStart);
      matchingHnos.Add(RightHnoEnd);

      return matchingHnos.Min();
    }

    public static int Compare(RdfAddr a1, RdfAddr a2)
    {
      var a1Min = a1.GetMinHno();
      var a2Min = a2.GetMinHno();
      if (a1Min == null)
        return 1;
      else if (a2Min == null)
        return -1;
      else
        return a1Min.Value - a2Min.Value;
    }
  }
}