using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
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

    public static void CreateConnectionString(string username, string password, string databaseName)
    {
      ConnectionString =
        "server=localhost;" +
        $"uid={username};" +
        $"pwd={password};" +
        $"database={databaseName};" +
        "connection timeout=1000;" +
        "command timeout=1000;";
    }

  }

  class Program
  {
    static void Main(string[] args)
    {
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
          Match.DoMatch();
          break;
        case 2:
          StreetSeg.DoStreetSeg();
          break;
        case 3:
          StreetSegKoo.DoStreetSegKoo();
          break;
      }
    }
  }
}
