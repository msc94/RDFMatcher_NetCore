using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Countries;

namespace RDFMatcher_NetCore
{
  class MatchSzThread : WorkerThread<MatchSzItem>
  {
    public MatchSzThread(WorkerThreadsProgress workerThreadsProgress, BlockingCollection<MatchSzItem> workQueue)
      : base(workerThreadsProgress, workQueue)
    {
    }

    public override WorkResult Work(MatchSzItem item)
    {
      item.StreetType = NZ.ReplaceStreetType(item.StreetType);

      var addrItems = _db.GetMatchingRdfAddrItems(item.Zip, item.StreetName, item.StreetType);

      if (addrItems.Count == 0)
      {
        Log.WriteLine($"No match for {item.Zip}, {item.StreetName}, {item.StreetType}");
        return WorkResult.Failed;
      }

      foreach (var addrItem in addrItems)
      {
        _db.InsertMatchedSzItem(new MatchedSzItem
        {
          RoadLinkId = addrItem.RoadLinkId,
          StreetZipId = item.StreetZipId
        });
      }

      return WorkResult.Successful;
    }
  }
}

