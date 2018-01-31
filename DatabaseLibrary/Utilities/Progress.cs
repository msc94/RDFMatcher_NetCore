using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DatabaseLibrary.Utilities
{
  public class Progress
  {
    private int _itemsDone = 0;
    private int _itemsSuccessful = 0;

    public void IncrementItemsDone()
    {
      Interlocked.Increment(ref _itemsDone);
    }

    public void IncrementItemsSuccessful()
    {
      Interlocked.Increment(ref _itemsSuccessful);
    }

    private int _lastDoneItems = 0;
    public string ProgressString()
    {
      var done = _itemsDone;
      var successful = _itemsSuccessful;

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
