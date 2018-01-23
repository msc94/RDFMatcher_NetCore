using System.Collections.Generic;

namespace DatabaseLibrary
{
  public class StreetAlias
  {
    public string Name;
    public string Type;
  }

  public class Street
  {
    public Zone Zone;

    public string Name;
    public string Type;

    public List<StreetAlias> Aliases = new List<StreetAlias>();
  }
}