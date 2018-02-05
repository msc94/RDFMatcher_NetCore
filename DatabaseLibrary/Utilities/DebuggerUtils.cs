using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace DatabaseLibrary.Utilities
{
  public class DebuggerUtils
  {
    public static void WaitForDebugger()
    {
      Console.WriteLine("Waiting for debugger to attach...");
      while (!Debugger.IsAttached)
      {
        Thread.Sleep(1000);
      }
      Console.WriteLine("Debugger attached");
      Debugger.Break();
    }
  }
}
