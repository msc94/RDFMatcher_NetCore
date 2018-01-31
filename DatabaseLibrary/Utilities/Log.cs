using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DatabaseLibrary.Utilities
{
  public class Log
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
}
