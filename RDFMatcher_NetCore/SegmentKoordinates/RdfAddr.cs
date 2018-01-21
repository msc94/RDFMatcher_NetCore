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

    public int GetMinHno()
    {
      var matchingHnos = new List<int>();

      switch (Scheme)
      {
        case 3:
          matchingHnos.AddIfNotNull(LeftHnoStart);
          matchingHnos.AddIfNotNull(LeftHnoEnd);
          matchingHnos.AddIfNotNull(RightHnoStart);
          matchingHnos.AddIfNotNull(RightHnoEnd);
          break;
        case 2:
          matchingHnos.AddIfNotNullAndEven(LeftHnoStart);
          matchingHnos.AddIfNotNullAndEven(LeftHnoEnd);
          matchingHnos.AddIfNotNullAndEven(RightHnoStart);
          matchingHnos.AddIfNotNullAndEven(RightHnoEnd);
          break;
        case 1:
          matchingHnos.AddIfNotNullAndNotEven(LeftHnoStart);
          matchingHnos.AddIfNotNullAndNotEven(LeftHnoEnd);
          matchingHnos.AddIfNotNullAndNotEven(RightHnoStart);
          matchingHnos.AddIfNotNullAndNotEven(RightHnoEnd);
          break;
      }

      return matchingHnos.Min();
    }

    public static int Compare(RdfAddr a1, RdfAddr a2)
    {
      return a1.GetMinHno() - a2.GetMinHno();
    }
  }
}