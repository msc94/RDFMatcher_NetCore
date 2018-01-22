using System.Collections.Generic;

namespace DataImporter
{
  public class Zone
  {
    public Zone ParentZone = null;
    public int Level;

    public string CommunityKey = null;
    public string Name = null;

    public List<string> Aliases = new List<string>();
  }
}
