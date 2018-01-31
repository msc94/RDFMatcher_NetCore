using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseLibrary.DatabaseClasses
{
  public class RdfAddrItem
  {
    public int RoadLinkId;
    public string Zip;
    public string StreetBaseName;
    public string StreetType;
  }

  public class RdfPointItem
  {
    public int RoadLinkId;
    public string Address;
    public Coordinates<string> Coordinates;
  }
}
