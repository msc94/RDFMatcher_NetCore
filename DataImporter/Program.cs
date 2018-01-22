using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using DataImporter.Countries;
using MySql.Data.MySqlClient;

namespace DataImporter
{
  class Program
  {
    static void Main(string[] args)
    {
      //readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ABBREVIATIONS.csv")));
      //readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ALTERNATIVE_STREET_NAMES.csv")));
      //readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ALTERNATIVE_SUBURB_NAMES.csv")));
      //readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ALTERNATIVE_TOWN_CITY_NAMES.csv")));
      //readerTasks.Add(Task.Run(() => );
      //readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_GEO_DEM_ADDRESSES.csv")));
      //readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_RECORD_USAGES.csv")));

      var loadedEntries = NZ.LoadFile();
      Console.WriteLine(loadedEntries);
    }
  }
}
