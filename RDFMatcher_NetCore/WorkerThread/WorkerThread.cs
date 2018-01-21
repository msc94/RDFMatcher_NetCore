using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.DBHelper;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  abstract class WorkerThread<T>
  {
    protected Database _db = new Database();

    private WorkerThreadsProgress _workerThreadProgress;
    private BlockingCollection<T> _workQueue;

    public WorkerThread(WorkerThreadsProgress workerThreadProgress, BlockingCollection<T> workQueue)
    {
      _workerThreadProgress = workerThreadProgress;
      _workQueue = workQueue;

      new Thread(WorkLoop).Start();
    }

    private void WorkLoop()
    {
      while (!_workQueue.IsCompleted)
      {
        T item = default(T);
        try
        {
          item = _workQueue.Take();
        }
        catch (InvalidOperationException) { }

        if (item != null)
        {
          try
          {
            var result = Work(item);
            if (result == WorkResult.Successful)
            {
              _workerThreadProgress.IncrementItemsSuccessful();
            }
          }
          catch (Exception e)
          {
            Trace.TraceWarning("Worker thread: Work() failed on item:\n" +
              item.ToString() + "\n" +
              "with error:\n" + 
              e.Message);
          }

          _workerThreadProgress.IncrementItemsDone();
        }
      }
    }

    // Implement this in sub-classes to implement Logic.
    public abstract WorkResult Work(T item);
  }
}