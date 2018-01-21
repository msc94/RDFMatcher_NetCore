using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  class MatchAddressItem
  {
    public object StreetZipId;
    public object BuildingId;

    public string Zip;
    public string StreetName;
    public string HouseNumber;
    public string HouseNumberExtension;
  }

  class InsertItem
  {
    public object StreetZipId;
    public object BuildingId;
    public object RoadLinkId;

    public string Hno;
    public string HnoExtension;
    public Coordinates<string> Coordinates;
  }

  class MatchInsertBuffer : InsertBuffer<InsertItem>
  {
    public override void InsertItem(InsertItem item)
    {
      // TODO: Implement
      throw new NotImplementedException();
    }
  }

  class MatchAddressThread : WorkerThread<MatchAddressItem>
  {
    private const string insertCommandText = "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, AP_LAT, AP_LNG) VALUES(@1, @2, @3, @4, @5)";

    private InsertBuffer<InsertItem> _insertBuffer;

    public MatchAddressThread(WorkerThreadsProgress workerThreadsProgress, BlockingCollection<MatchAddressItem> workQueue)
      : base(workerThreadsProgress, workQueue)
    {
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
        _insertBuffer.Insert(new InsertItem()
        {
          Coordinates = coordinates,
          RoadLinkId = point.RoadLinkId,
          BuildingId = item.BuildingId
        });
      }

      return true;
    }
  }
}

