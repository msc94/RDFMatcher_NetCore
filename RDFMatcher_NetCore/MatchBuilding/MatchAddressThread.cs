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
    public MatchAddressThread(WorkerThreadsProgress workerThreadsProgress, BlockingCollection<MatchAddressItem> workQueue)
      : base(workerThreadsProgress, workQueue)
    {
    }

    public override WorkResult Work(MatchAddressItem item)
    {
      var address = (item.HouseNumber + item.HouseNumberExtension).Trim('0');
      var matchedPoints = _db.GetRdfPointsForAddress(item.Zip, item.StreetName, address);
      var numMatches = matchedPoints.Count;

      if (numMatches == 0)
      {
        Log.WriteLine($"No match for {item.Zip}, {item.StreetName}, {address}");
        return WorkResult.Failed;
      }
      if (numMatches > 1)
      {
        Log.WriteLine($"More than one match for {item.Zip}, {item.StreetName}, {address}");
        return WorkResult.Failed;
      }

      var match = matchedPoints[0];
      var coordinates = match.Coordinates;

      coordinates.Lat = coordinates.Lat.Insert(2, ".");

      if (coordinates.Lng.Length == 6)
        coordinates.Lng = coordinates.Lng.Insert(1, ".");
      else
        coordinates.Lng = coordinates.Lng.Insert(2, ".");

      _db.InsertMatchedBuildingItem(new MatchedAddressItem
      {
        StreetZipId = item.StreetZipId,
        Address = address,
        RoadLinkId = match.RoadLinkId,
        BuildingId = item.BuildingId,
        Coordinates = coordinates
      });

      return WorkResult.Successful;
    }
  }
}

