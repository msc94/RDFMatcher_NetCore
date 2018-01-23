using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class DB
  {
    public static int NumberOfThreads;
    public static string ConnectionString;
    public static int InsertBufferSize = 50000;

    public static void CreateConnectionString(string username, string password, string databaseName)
    {
      ConnectionString =
        "Server=localhost;" +
        $"Uid={username};" +
        $"Pwd={password};" +
        $"Database={databaseName};" +
        "Connection Timeout=1000;" +
        "Command Timeout=1000;" +
        "Charset=utf8;";
    }
  }

  class Log
  {
    private static TextWriter _logWriter;

    public static void Init(string fileName)
    {
      _logWriter = TextWriter.Synchronized(new StreamWriter(fileName));
    }

    public static void WriteLine(string message)
    {
      _logWriter.WriteLine(message);
    }

    public static void Flush()
    {
      _logWriter.Flush();
    }
  }

  class Program
  {
    static void Main(string[] args)
    {
      // Add right encoding
      Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
      Console.OutputEncoding = System.Text.Encoding.UTF8;


      if (args.Length != 5)
      {
        Console.WriteLine("Wrong number of parameters");
        return;
      }

      DB.CreateConnectionString(args[0], args[1], args[2]);
      DB.NumberOfThreads = int.Parse(args[3]);

      switch (int.Parse(args[4]))
      {
        case 1:
          Log.Init(@"J:\MatchSzLog.txt");
          MatchSz.DoMatch();
          break;
        case 2:
          Log.Init(@"J:\MatchAddressLog.txt");
          MatchAddress.DoMatch();
          break;
        case 3:
          Log.Init(@"J:\StreetSegLog.txt");
          StreetSeg.DoStreetSeg();
          break;
        case 4:
          Log.Init(@"J:\StreetSegKooLog.txt");
          StreetSegKoo.DoStreetSegKoo();
          break;
        case 5:
          Log.Init(@"J:\ZipGeoLog.txt");
          ZipGeo.DoZipGeo();
          break;
      }

      Log.Flush();
    }
  }
}
