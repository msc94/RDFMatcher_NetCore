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
    public static string connectionString =
      "server=localhost;uid=root;pwd=bloodrayne;database=test;connection timeout=1000;command timeout=1000;";
  }

  class Program
  {
    static void Main(string[] args)
    {
      StreetSegKoo.DoStreetSegKoo();     
    }
  }
}
