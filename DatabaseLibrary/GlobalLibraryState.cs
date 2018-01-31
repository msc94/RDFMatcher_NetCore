using DatabaseLibrary.Utilities;
using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseLibrary
{
  public class GlobalLibraryState
  {
    public static string ConnectionString { get; private set; }

    public static string RdfAddrTable { get; private set; } = "rdf_addr";
    public static string RdfSegTable { get; private set; } = "rdf_seg";
    public static string RdfPointTable { get; private set; } = "rdf_point";

    public static void Init(string applicationName, string username, string password, string databaseName)
    {
      ConnectionString = CreateConnectionString("root", "bloodrayne", "nor");
      Log.Init(@"G:\SQL\logs\" + applicationName + ".txt");
    }

    private static string CreateConnectionString(string username, string password, string databaseName)
    {
      return "Server=localhost;" +
        $"Uid={username};" +
        $"Pwd={password};" +
        $"Database={databaseName};" +
        "Connection Timeout=1000;" +
        "Command Timeout=1000;" +
        "Charset=utf8;";
    }
  }
}
