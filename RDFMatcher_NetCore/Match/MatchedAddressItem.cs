using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  class MatchedAddressItem
  {
    public object StreetZipId;
    public object BuildingId;
    public object RoadLinkId;

    public string Address;
    public Coordinates<string> Coordinates;
  }
}

