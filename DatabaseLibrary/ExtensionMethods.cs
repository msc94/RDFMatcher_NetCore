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
  }
}
