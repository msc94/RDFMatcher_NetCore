using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace DataImporter
{
  class Program
  {
    static void Main(string[] args)
    {
      var loadedEntries = NOR.LoadFile();
      Console.WriteLine(loadedEntries);
    }
  }
}
