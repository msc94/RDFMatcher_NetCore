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

    private int _lastDoneItems = 0;
    public string ProgressString()
    {
      var done = ItemsDone;
      var successful = ItemsSuccessful;

      var matchedPercentage = (float)successful / done;
      var percentageString = matchedPercentage.ToString("0.00");

      var itemsPerSecond = done - _lastDoneItems;
      _lastDoneItems = done;

      return $"Done: {done}, Successful: {successful}, Percentage: {percentageString}, Items/s: {itemsPerSecond}";
    }

    public void PrintProgress()
    {
      Console.WriteLine(ProgressString());
    }
  }
}