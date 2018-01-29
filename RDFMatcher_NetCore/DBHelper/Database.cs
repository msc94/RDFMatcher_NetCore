﻿using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore.DBHelper
{
  class Database
  {
    // Settings
    private readonly string _rdfAddrTable = "NZ_RDF_ADDR";
    private readonly string _rdfSegTable = "NZ_RDF_SEG";
    private readonly string _rdfPointTable = "NZ_RDF_POINT";

    private readonly int _latDecimalPosition = 3;
    private readonly int _lngDecimalPosition = 3;

    private readonly string _connectionString;

    public Database(string connectionString)
    {
      _connectionString = connectionString;
    }

    public List<BuildingItem> GetBuildingsInStreetZip(object streetZipId)
    {
      var reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT ID, STREET_ZIP_ID, HNO, HNO_EXTENSION " +
        $"FROM building b " +
        "WHERE STREET_ZIP_ID = @1;",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", streetZipId)
        });

      var buildingList = new List<BuildingItem>();
      using (reader)
      {
        while (reader.Read())
        {
          var item = new BuildingItem
          {
            Id = reader.GetInt32("ID"),
            StreetZipId = reader.GetInt32("STREET_ZIP_ID"),
            HouseNumber = reader.GetString("HNO"),
            HouseNumberExtension = reader.GetString("HNO_EXTENSION")
          };
          buildingList.Add(item);
        }
      }
      return buildingList;
    }

    public void InsertMatchedBuildingItem(MatchedAddressItem item)
    {
      MySqlHelper.ExecuteNonQuery(DB.ConnectionString,
        "INSERT INTO match_building VALUES (@1, @2, @3, @4, @5)",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", item.RoadLinkId),
          new MySqlParameter("@2", item.Address),
          new MySqlParameter("@3", item.BuildingId),
          new MySqlParameter("@4", item.Coordinates.Lat),
          new MySqlParameter("@5", item.Coordinates.Lng),
        });
    }

    public void InsertMatchedSzItem(MatchedSzItem item)
    {
      MySqlHelper.ExecuteNonQuery(DB.ConnectionString,
        "INSERT INTO match_sz VALUES (@1, @2)",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", item.StreetZipId),
          new MySqlParameter("@2", item.RoadLinkId),
        });
    }

    public List<RdfAddrItem> GetMatchingRdfAddrItems(string zip, string streetName, string streetType)
    {
      var reader = MySqlHelper.ExecuteReader(DB.ConnectionString,
        "SELECT addr.ROAD_LINK_ID, addr.LEFT_POSTAL_CODE, addr.STREET_BASE_NAME, addr.STREET_TYPE  " +
        $"FROM {_rdfAddrTable} addr " +
        "WHERE " +
        " addr.LEFT_POSTAL_CODE = @1 AND " +
        " (addr.STREET_BASE_NAME = @2 AND addr.STREET_TYPE = @3);",
        new MySqlParameter[]
        {
                new MySqlParameter("@1", zip),
                new MySqlParameter("@2", streetName),
                new MySqlParameter("@3", streetType)
        });

      var rdfAddrItemList = new List<RdfAddrItem>();
      using (reader)
      {
        while (reader.Read())
        {
          RdfAddrItem item = new RdfAddrItem
          {
            RoadLinkId = reader.GetInt32("ROAD_LINK_ID"),
            StreetBaseName = reader.GetString("STREET_BASE_NAME"),
            StreetType = reader.GetString("STREET_TYPE"),
            Zip = reader.GetString("LEFT_POSTAL_CODE")
          };
          rdfAddrItemList.Add(item);
        }
      }
      return rdfAddrItemList;
    }

    // Be careful when this function returns more than one item!
    public List<RdfPointItem> GetRdfPointsForAddress(string zip, string streetName, string streetType, string address)
    {
      var reader = MySqlHelper.ExecuteReader(DB.ConnectionString,
        "SELECT pt.ROAD_LINK_ID, pt.ADDRESS, pt.LAT, pt.LNG " +
        $"FROM {_rdfAddrTable} addr " +
        $"  LEFT JOIN {_rdfPointTable} pt USING (ROAD_LINK_ID) " +
        "WHERE " +
        " addr.LEFT_POSTAL_CODE = @1 AND " +
        " (addr.STREET_BASE_NAME_UPPER = @2 AND addr.STREET_TYPE = @3) AND " +
        " pt.ADDRESS = @4 AND " +
        " pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL;",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", zip),
          new MySqlParameter("@2", streetName),
          new MySqlParameter("@3", streetType),
          new MySqlParameter("@4", address)
        });

      List<RdfPointItem> coordinateList = new List<RdfPointItem>();
      using (reader)
      {
        while (reader.Read())
        {
          var coordinates = new Coordinates<string>()
          {
            Lat = reader.GetString("LAT"),
            Lng = reader.GetString("LNG")
          };

          var rdfPointEntry = new RdfPointItem()
          {
            RoadLinkId = reader.GetInt32("ROAD_LINK_ID"),
            Address = reader.GetString("ADDRESS"),
            Coordinates = coordinates
          };

          coordinateList.Add(rdfPointEntry);
        }
      }
      return coordinateList;
    }

    public RdfAddr GetRdfAddr(int roadLinkID)
    {
      var reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT LEFT_HNO_START, LEFT_HNO_END, RIGHT_HNO_START, RIGHT_HNO_END " +
        $"FROM {_rdfAddrTable} addr " +
        "WHERE ROAD_LINK_ID = @1",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", roadLinkID)
        });

      var newItem = new RdfAddr();
      using (reader)
      {
        reader.Read();
        newItem.RoadLinkId = roadLinkID;
        newItem.LeftHnoStart = Utils.ParseIntHandleEmpty(reader.GetString("LEFT_HNO_START"));
        newItem.LeftHnoEnd = Utils.ParseIntHandleEmpty(reader.GetString("LEFT_HNO_END"));
        newItem.RightHnoStart = Utils.ParseIntHandleEmpty(reader.GetString("RIGHT_HNO_START"));
        newItem.RightHnoEnd = Utils.ParseIntHandleEmpty(reader.GetString("RIGHT_HNO_END"));
      }
      return newItem;
    }

    public List<int> GetMatchedRoadLinkIdsForStreetSeg(int streetSegId)
    {
      var reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT DISTINCT m.ROAD_LINK_ID " +
        "FROM match_sz m " +
        "  LEFT JOIN street_seg seg ON m.SZ_ID = seg.STREET_ZIP_ID " +
        "WHERE seg.ID = @1",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", streetSegId)
        });

      var roadLinkIds = new List<int>();
      using (reader)
      {
        while (reader.Read())
        {
          roadLinkIds.Add(reader.GetInt32("ROAD_LINK_ID"));
        }
      }
      return roadLinkIds;
    }

    public Segment GetRdfSeg(int roadLinkId)
    {
      var reader = MySqlHelper.ExecuteReader(_connectionString,
        "SELECT LAT, LON " +
        $"FROM {_rdfSegTable} seg " +
        "WHERE ROAD_LINK_ID = @1 " +
        "ORDER BY SEQ_NUM",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", roadLinkId)
        });

      var segment = new Segment();
      using (reader)
      {
        while (reader.Read())
        {
          string latString = reader.GetString("LAT");
          string lonString = reader.GetString("LON");

          // Add the decimal point to the lat/lon string
          latString = latString.Insert(_latDecimalPosition, ".");
          lonString = lonString.Insert(_lngDecimalPosition, ".");

          segment.Coordinates.Add(new SegmentCoordinate()
          {
            Lat = Utils.ParseDoubleInvariantCulture(latString),
            Lng = Utils.ParseDoubleInvariantCulture(lonString)
          });
        }
      }
      return segment;
    }

    public void InsertKoo(object segmentId, int ord, int pos, string lat, string lon)
    {
      MySqlHelper.ExecuteNonQuery(_connectionString,
        "INSERT INTO street_seg_koo (STREET_SEG_ID, ORD, POS, LAT, LNG) " +
        "VALUES (@1, @2, @3, @4, @5)",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", segmentId),
          new MySqlParameter("@2", ord),
          new MySqlParameter("@3", pos),
          new MySqlParameter("@4", lat),
          new MySqlParameter("@5", lon)
        });
    }

    public void UpdateSeg(object segmentId, double lat, double lon)
    {
      MySqlHelper.ExecuteNonQuery(_connectionString,
        "UPDATE street_seg " +
        "SET CENTER_LAT = @2, " +
        "CENTER_LNG = @3 " +
        "WHERE ID = @1",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", segmentId),
          new MySqlParameter("@2", lat),
          new MySqlParameter("@3", lon),
        });
    }

    public void InsertKooGroup(object segmentId)
    {
      MySqlHelper.ExecuteNonQuery(_connectionString,
        "INSERT INTO street_seg_koo_group " +
        "SELECT STREET_SEG_ID, GROUP_CONCAT(CONCAT(LAT, ',', LNG) SEPARATOR '@') " +
        "FROM street_seg_koo " +
        "WHERE STREET_SEG_ID = @1 " +
        "GROUP BY STREET_SEG_ID " +
        "ORDER BY ORD ",
        new MySqlParameter[]
        {
          new MySqlParameter("@1", segmentId)
        });
    }
  }
}
