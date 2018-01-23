using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class MatchAddressThread : WorkerThread<MatchAddressItem>
  {
    private const string insertCommandText = "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, AP_LAT, AP_LNG) VALUES(@1, @2, @3, @4, @5)";

    private InsertBuffer<MatchedAddressItem> _insertBuffer;

    public MatchAddressThread(WorkerThreadsProgress workerThreadsProgress, BlockingCollection<MatchAddressItem> workQueue)
      : base(workerThreadsProgress, workQueue)
    {
      _insertBuffer = new MatchInsertBuffer(_db);
    }

    public override WorkResult Work(MatchAddressItem item)
    {
      var pointsForAddress = _db.GetRdfPointsForAddress(item.Zip, item.StreetName, item.HouseNumber + item.HouseNumberExtension);
      var numMatches = pointsForAddress.Count;

      if (numMatches > 1)
        return WorkResult.TooManyMatches;
      else if (numMatches == 0)
        return WorkResult.NoMatch;

      foreach (var point in pointsForAddress)
      {
        _db.InsertMatchedItem(new MatchedAddressItem()
        {
          Address = item.HouseNumber +  item.HouseNumberExtension,
          RoadLinkId = point.RoadLinkId,
          BuildingId = item.BuildingId
        });
      }

      return WorkResult.Successful;
    }
    
    public void FlushInsertBuffer()
    {
      _insertBuffer.Flush();
    }
  }
}

