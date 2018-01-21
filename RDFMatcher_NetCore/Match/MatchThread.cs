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

    public override bool Work(MatchAddressItem item)
    {
      var pointsForAddress = _db.GetRdfPointsForAddress(item.Zip, item.StreetName, item.HouseNumber, item.HouseNumberExtension);
      var numMatches = pointsForAddress.Count;

      if (numMatches != 1)
        return false;

      foreach (var point in pointsForAddress)
      {
        var coordinates = point.Coordinates;
        _insertBuffer.Insert(new MatchedAddressItem()
        {
          Coordinates = coordinates,
          RoadLinkId = point.RoadLinkId,
          BuildingId = item.BuildingId
        });
      }

      return true;
    }
    
    public void FlushInsertBuffer()
    {
      _insertBuffer.Flush();
    }
  }
}

