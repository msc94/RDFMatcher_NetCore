using DatabaseLibrary.Utilities;

namespace MatchAddress
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

