using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Text;

namespace RDFMatcher_NetCore
{
  class CommandBuffer
  {
    private Dictionary<string, Command> _commandBuffer = new Dictionary<string, Command>();

    public MySqlDataReader ExececuteReader(string command, params object[] parameters)
    {
      CreateCommandIfNotExists(command);
      return _commandBuffer[command].ExecuteReader(parameters);
    }

    public void ExecuteNonQuery(string command, params object[] parameters)
    {
      CreateCommandIfNotExists(command); 
      _commandBuffer[command].ExecuteNonQuery(parameters);
    }

    private void CreateCommandIfNotExists(string command)
    {
      if (_commandBuffer.ContainsKey(command) == false)
      {
        _commandBuffer.Add(command, new Command(command));
      }
    }

  }
}
