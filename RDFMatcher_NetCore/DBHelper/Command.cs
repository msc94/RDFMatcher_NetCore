using System;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class Command
  {
    private MySqlConnection _connection;
    private MySqlCommand _command;

    private int _numParameters;

    public Command(string command)
    {
      _connection = new MySqlConnection(DB.ConnectionString);
      _connection.Open();

      _command = _connection.CreateCommand();
      _command.CommandText = command;

      // TODO: Does this work reliable?
      _numParameters = Regex.Matches(command, @"@[\d]+").Count;
      for (int i = 0; i < _numParameters; i++)
      {
        _command.Parameters.AddWithValue($"@{i + 1}", null);
      }
    }

    public MySqlDataReader ExecuteReader(params object[] parameters)
    {
      SetParameters(parameters);
      return _command.ExecuteReader();
    }

    public void ExecuteNonQuery(params object[] parameters)
    {
      SetParameters(parameters);
      _command.ExecuteNonQuery();
    }

    private void SetParameters(params object[] parameters)
    {
      if (parameters.Length != _numParameters)
      {
        throw new ArgumentException("Number of parameters does not match command parameters!", nameof(parameters));
      }

      for (int i = 0; i < parameters.Length; i++)
      {
        _command.Parameters[i].Value = parameters[i];
      }
    }
  }
}