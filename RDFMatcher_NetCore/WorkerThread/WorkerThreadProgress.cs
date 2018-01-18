using System.Threading;

namespace RDFMatcher_NetCore
{
  class WorkerThreadProgress
  {
    private int _itemsDone = 0;
    public int ItemsDone
    {
      get
      {
        return _itemsDone;
      }
    }

    public void IncrementItemsDone()
    {
      Interlocked.Increment(ref _itemsDone);
    }
  }
}