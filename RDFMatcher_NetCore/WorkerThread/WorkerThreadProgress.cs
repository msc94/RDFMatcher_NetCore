using System;
using System.Threading;

namespace RDFMatcher_NetCore
{
  class WorkerThreadsProgress
  {
    private int _itemsDone = 0;
    private int _itemsSuccessful = 0;

    public int ItemsDone
    {
      get
      {
        return _itemsDone;
      }
    }

    public int ItemsSuccessful
    {
      get
      {
        return _itemsSuccessful;
      }
    }

    public void IncrementItemsDone()
    {
      Interlocked.Increment(ref _itemsDone);
    }

    internal void IncrementItemsSuccessful()
    {
      Interlocked.Increment(ref _itemsSuccessful);
    }
  }
}