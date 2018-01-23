using RDFMatcher_NetCore.Utilities;
using System;
using System.Collections.Generic;
using System.Text;

namespace RDFMatcher_NetCore
{
  class RdfAddrItem
  {
    public int RoadLinkId;
    public string Zip;
    public string StreetBaseName;
    public string StreetType;
  }

  class RdfPointItem
  {
    public int RoadLinkId;
    public string Address;
    public Coordinates<string> Coordinates;
  }
}
