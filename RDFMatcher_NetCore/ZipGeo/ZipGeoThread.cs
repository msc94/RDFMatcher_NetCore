﻿using Google.Maps;
using Google.Maps.Geocoding;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDFMatcher_NetCore
{
  class ZipGeoThread : WorkerThread<ZipGeoItem>
  {
    public ZipGeoThread(WorkerThreadsProgress workerThreadProgress, BlockingCollection<ZipGeoItem> workQueue) 
      : base(workerThreadProgress, workQueue)
    {
    }

    public override WorkResult Work(ZipGeoItem item)
    {
      string zip = item.Zip;
      var coordinates = new List<Coordinates<float>>();

      var coordinateReader = MySqlHelper.ExecuteReader(DB.ConnectionString,
        "SELECT addr.LEFT_POSTAL_CODE, pt.LAT, pt.LNG " +
        "FROM nor_rdf_addr addr " +
        " LEFT JOIN nor_rdf_point pt USING (ROAD_LINK_ID) " +
        $"WHERE addr.LEFT_POSTAL_CODE = @1 " +
        " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", zip)
        });

      while (coordinateReader.Read())
      {
        string latString = coordinateReader.GetString("LAT").Insert(2, ".");
        string lonString = coordinateReader.GetString("LNG");

        if (lonString.Length == 6)
          lonString = lonString.Insert(1, ".");
        else
          lonString = lonString.Insert(2, ".");

        coordinates.Add(new Coordinates<float>
        {
          Lat = Utils.ParseFloatInvariantCulture(latString),
          Lng = Utils.ParseFloatInvariantCulture(lonString)
        });
      }

      coordinateReader.Close();

      bool matched = false;
      float midLat = -1.0f, midLng = -1.0f;

      if (coordinates.Count > 0)
      {
        midLat = coordinates.Sum(c => c.Lat) / coordinates.Count;
        midLng = coordinates.Sum(c => c.Lng) / coordinates.Count;
        matched = true;
      }
      else
      {
        Coordinates<float> googleCoordinate = MatchGoogle(zip);

        if (googleCoordinate != null)
        {
          midLat = googleCoordinate.Lat;
          midLng = googleCoordinate.Lng;
          matched = true;
        }
      }

      if (!matched)
      {
        Log.WriteLine($"Could not match {zip}");
        return WorkResult.Failed;
      }

      const string latFormat = "00.00000";
      const string lngFormat = "00.00000";

      MySqlHelper.ExecuteNonQuery(DB.ConnectionString,
        "INSERT INTO zip_koo VALUES(@1, @2, @3);",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", zip),
          new MySqlParameter("@2", Utils.FloatToStringInvariantCulture(midLat, latFormat)),
          new MySqlParameter("@3", Utils.FloatToStringInvariantCulture(midLng, lngFormat)),
        });

      return WorkResult.Successful;
    }

    private Coordinates<float> MatchGoogle(string zip)
    {
      var request = new GeocodingRequest();
      request.Address = $"{zip}, Norwegen";
      var response = new GeocodingService().GetResponse(request);

      if (response.Status == ServiceResponseStatus.Ok && response.Results.Count() == 1)
      {
        var firstResult = response.Results.First();
        var result = new Coordinates<float>
        {
          Lat = (float)firstResult.Geometry.Location.Latitude,
          Lng = (float)firstResult.Geometry.Location.Longitude
        };
        return result;
      }
      else
      {
        return null;
      }
    }
  }
}