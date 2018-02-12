using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using DataImporter.Countries;
using MySql.Data.MySqlClient;
using DatabaseLibrary.Utilities;
using DatabaseLibrary;

namespace DataImporter
{
  class Program
  {
    static void Main(string[] args)
    {
      var loadedEntries = RUS.LoadFile();
    }
  }
}
