using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.DBHelper;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  class WorkerThread
  {
    private WorkerThreadProgress _workerThreadProgress;
    private BlockingCollection<Action> _workQueue;

    public WorkerThread(WorkerThreadProgress workerThreadProgress, BlockingCollection<Action> workQueue)
    {
      _workerThreadProgress = workerThreadProgress;
      _workQueue = workQueue;

      new Thread(WorkLoop).Start();
    }

    private void WorkLoop()
    {
      while (!_workQueue.IsCompleted)
      {
        Action item = null;
        try
        {
          item = _workQueue.Take();
        }
        catch (InvalidOperationException) { }

        if (item != null)
        {
          item();
          _workerThreadProgress.IncrementItemsDone();
        }
      }
    }
  }
}