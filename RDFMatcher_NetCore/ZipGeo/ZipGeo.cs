using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Maps;
using Google.Maps.Geocoding;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore
{
  class ZipGeo
  {
    public static void DoZipGeo()
    {
      GoogleSigned.AssignAllServices(new GoogleSigned("AIzaSyCzOcXgCQ_1Nng6shWR9FRS2tRFBItyG0E"));

      var districtCodeReader = MySqlHelper.ExecuteReader(DB.ConnectionString, 
        "SELECT DISTINCT ZIP " +
        "FROM street_zip " +
        "WHERE ZIP NOT IN (SELECT DISTRICT_CODE FROM zip_koo);");

      var workQueue = new BlockingCollection<ZipGeoItem>();
      var workerThreadsProgress = new WorkerThreadsProgress();

      var workerThreads = new List<ZipGeoThread>();
      for (int i = 0; i < DB.NumberOfThreads; i++)
      {
        workerThreads.Add(new ZipGeoThread(workerThreadsProgress, workQueue));
      }

      while (districtCodeReader.Read())
      {
        var zip = districtCodeReader.GetString("ZIP");
        workQueue.Add(new ZipGeoItem
        {
          Zip = zip
        });
      }
      districtCodeReader.Close();

      workQueue.CompleteAdding();
      while (workQueue.IsCompleted == false)
      {
        workerThreadsProgress.PrintProgress();
        Log.Flush();
        Thread.Sleep(1000);
      }
    }
  }
}
