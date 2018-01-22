using System.Collections.Generic;

namespace DataImporter
{
  public class Street
  {
    public Zone Zone;

    public string Name;
    public List<string> Aliases = new List<string>();
  }
}