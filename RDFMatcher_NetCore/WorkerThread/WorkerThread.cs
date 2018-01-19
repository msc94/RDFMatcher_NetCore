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

  // The Workerthread should have a Database instance that is unique for every thread
  // and give it to each item

  class WorkerThread
  {
    private WorkerThreadsProgress _workerThreadProgress;
    private BlockingCollection<Action> _workQueue;

    public WorkerThread(WorkerThreadsProgress workerThreadProgress, BlockingCollection<Action> workQueue)
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
          try
          {
            item();
          _workerThreadProgress.IncrementItemsSuccessful();
          }
          catch(Exception e)
          {

          }

          _workerThreadProgress.IncrementItemsDone();
        }
      }
    }
  }
}