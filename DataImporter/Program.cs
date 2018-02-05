using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using DataImporter.Countries;
using MySql.Data.MySqlClient;
using DatabaseLibrary.Utilities;

namespace DataImporter
{
  class Program
  {
    static void Main(string[] args)
    {
      // var loadedEntries = RUS.LoadFile();
      // Console.WriteLine(loadedEntries);
      DebuggerUtils.WaitForDebugger();
      Console.WriteLine("Hello World!");
      Console.ReadKey();
    }
  }
}
