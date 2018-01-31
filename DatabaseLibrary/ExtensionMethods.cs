using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseLibrary
{
  public static class ExtensionMethods
  {
    public static string GetString(this MySqlDataReader reader, string column)
    {
      return reader.GetString(reader.GetOrdinal(column));
    }

    public static int GetInt32(this MySqlDataReader reader, string column)
    {
      return reader.GetInt32(reader.GetOrdinal(column));
    }

    public static long GetInt64(this MySqlDataReader reader, string column)
    {
      return reader.GetInt64(reader.GetOrdinal(column));
    }
  }
}
