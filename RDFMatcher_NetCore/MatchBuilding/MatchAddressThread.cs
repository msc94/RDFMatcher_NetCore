using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Countries;
using RDFMatcher_NetCore.Utilities;

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
      var address = (item.HouseNumber + item.HouseNumberExtension).TrimStart('0');
      // var streetType = NZ.ReplaceStreetType(item.StreetType);

      var matchedPoints = _db.GetRdfPointsForAddress(item.StreetZipId, address);
      var numMatches = matchedPoints.Count;

      if (numMatches == 0)
      {
        Log.WriteLine($"No match for {item.Zip}, {item.StreetName}, {item.StreetType}, {address}");
        return WorkResult.Failed;
      }
      if (numMatches > 1)
      {
        Log.WriteLine($"More than one match for {item.Zip}, {item.StreetName}, {item.StreetType}, {address}");
        return WorkResult.Failed;
      }

      var match = matchedPoints[0];
      var coordinates = match.Coordinates;

      coordinates.Lat = Utils.RdfCoordinateInsertDecimal(coordinates.Lat);
      coordinates.Lng = Utils.RdfCoordinateInsertDecimal(coordinates.Lng);

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

