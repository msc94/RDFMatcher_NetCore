using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DatabaseLibrary
{
  public class DatabaseHelper
  {
    public static int ExecuteNonQuery(string connectionString, string commandText, params object[] commandParameters)
    {
      using (var conn = new MySqlConnection(connectionString))
      {
        conn.Open();

        return ExecuteNonQuery(conn, commandText, commandParameters);
      }
    }

    public static int ExecuteNonQuery(MySqlConnection connection, string commandText, params object[] commandParameters)
    {
      //create a command and prepare it for execution
      MySqlCommand cmd = new MySqlCommand();
      cmd.Connection = connection;
      cmd.CommandText = commandText;
      cmd.CommandType = CommandType.Text;

      int i = 1;
      foreach (var p in commandParameters)
          cmd.Parameters.Add(new MySqlParameter($"@{i++}", p));

      int result = cmd.ExecuteNonQuery();
      cmd.Parameters.Clear();

      return result;
    }

    public static MySqlDataReader ExecuteReader(string connectionString, string commandText, params object[] commandParameters)
    {
      var conn = new MySqlConnection(connectionString);
        conn.Open();

        return ExecuteReader(conn, commandText, commandParameters);
    }

    public static MySqlDataReader ExecuteReader(MySqlConnection connection, string commandText, params object[] commandParameters)
    {
      //create a command and prepare it for execution
      MySqlCommand cmd = new MySqlCommand();
      cmd.Connection = connection;
      cmd.CommandText = commandText;
      cmd.CommandType = CommandType.Text;

      int i = 1;
      foreach (var p in commandParameters)
        cmd.Parameters.Add(new MySqlParameter($"@{i++}", p));

      //create a reader
      MySqlDataReader dr;
      dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);

      // detach the SqlParameters from the command object, so they can be used again.
      cmd.Parameters.Clear();

      return dr;
    }

    public static object ExecuteScalar(string connectionString, string commandText, params object[] commandParameters)
    {
      using (var conn = new MySqlConnection(connectionString))
      {
        conn.Open();

        return ExecuteScalar(conn, commandText, commandParameters);
      }
    }

    public static object ExecuteScalar(MySqlConnection connection, string commandText, params object[] commandParameters)
    {
      //create a command and prepare it for execution
      MySqlCommand cmd = new MySqlCommand();
      cmd.Connection = connection;
      cmd.CommandText = commandText;
      cmd.CommandType = CommandType.Text;

      int i = 1;
      foreach (var p in commandParameters)
        cmd.Parameters.Add(new MySqlParameter($"@{i++}", p));

      //execute the command & return the results
      object retval = cmd.ExecuteScalar();

      // detach the SqlParameters from the command object, so they can be used again.
      cmd.Parameters.Clear();
      return retval;
    }

  }
}
