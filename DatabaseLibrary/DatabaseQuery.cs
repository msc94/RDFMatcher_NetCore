using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseLibrary
{
  class DatabaseQuery
  {
    readonly string _connectionString;

    public DatabaseQuery(string connectionString)
    {
      _connectionString = connectionString;
    }

  }
}
