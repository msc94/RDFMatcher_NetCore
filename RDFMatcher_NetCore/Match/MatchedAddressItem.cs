using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  class MatchedAddressItem
  {
    public object StreetZipId;
    public object BuildingId;
    public object RoadLinkId;

    public string Hno;
    public string HnoExtension;
    public Coordinates<string> Coordinates;
  }
}

