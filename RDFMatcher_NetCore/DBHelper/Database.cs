using System;
using System.Collections.Generic;
using System.Text;
using RDFMatcher_NetCore.Utilities;

namespace RDFMatcher_NetCore.DBHelper
{
  class Database
  {
    // Settings
    private readonly string _rdfAddrTable = "POL_RDF_ADDR";
    private readonly string _rdfSegTable = "POL_RDF_SEG";
    private readonly string _rdfPointTable = "POL_RDF_POINT";


    private readonly int _latDecimalPosition = 2;

    internal void InsertMatchedItems(IEnumerable<MatchedAddressItem> items)
    {
      throw new NotImplementedException();
    }

    private readonly int _lngDecimalPosition = 2;

    private readonly CommandBuffer _commandBuffer = new CommandBuffer();
    public RdfAddr GetRdfAddr(int roadLinkID)
    {
      var reader = _commandBuffer.ExececuteReader(
        "SELECT LEFT_HNO_START, LEFT_HNO_END, RIGHT_HNO_START, RIGHT_HNO_END " +
        $"FROM {_rdfAddrTable} addr " +
        "WHERE ROAD_LINK_ID = @1",
        roadLinkID);

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
      var reader = _commandBuffer.ExececuteReader(
        "SELECT DISTINCT m.ROAD_LINK_ID " +
        "FROM match_test m " +
        "  LEFT JOIN street_seg seg ON m.STREET_ZIP_ID = seg.STREET_ZIP_ID " +
        "WHERE seg.ID = @1",
        streetSegId);

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

    public List<Segment> GetRdfSeg(int roadLinkId)
    {
      var reader = _commandBuffer.ExececuteReader(
        "SELECT LAT, LON " +
        $"FROM {_rdfSegTable} seg " +
        "WHERE ROAD_LINK_ID = @1 " +
        "ORDER BY SEQ_NUM", roadLinkId);

      var segments = new List<Segment>();
      using (reader)
      {
        while (reader.Read())
        {
          string latString = reader.GetString("LAT");
          string lonString = reader.GetString("LON");

          // Add the decimal point to the lat/lon string
          latString = latString.Insert(_latDecimalPosition, ".");
          lonString = lonString.Insert(_lngDecimalPosition, ".");

          segments.Add(new Segment()
          {
            LAT = Utils.ParseFloatInvariantCulture(latString),
            LON = Utils.ParseFloatInvariantCulture(lonString)
          });
        }
      }
      return segments;
    }

    public void InsertKoo(object segmentId, int ord, int pos, string lat, string lon)
    {
      _commandBuffer.ExecuteNonQuery(
        "INSERT INTO street_seg_koo (STREET_SEG_ID, ORD, POS, LAT, LNG) " +
        "VALUES (@1, @2, @3, @4, @5)",
        segmentId, ord, pos, lat, lon);
    }

    public void UpdateSeg(object segmentId, float lat, float lon)
    {
      _commandBuffer.ExecuteNonQuery(
        "UPDATE street_seg " +
        "SET CENTER_LAT = @2, " +
        "CENTER_LNG = @3 " +
        "WHERE ID = @1",
        segmentId, lat, lon);
    }

    public void InsertKooGroup(object segmentId)
    {
      _commandBuffer.ExecuteNonQuery(
        "INSERT INTO street_seg_koo_group " +
        "SELECT STREET_SEG_ID, GROUP_CONCAT(CONCAT(LAT, ',', LNG) SEPARATOR '@') " +
        "FROM street_seg_koo " +
        "WHERE STREET_SEG_ID = @1 " +
        "GROUP BY STREET_SEG_ID " +
        "ORDER BY ORD ",
        segmentId);
    }

    // Be careful when this function returns more than one item!
    public List<RdfPointItem> GetRdfPointsForAddress(string zip, string streetName, string houseNumber, string houseNumberExtension)
    {
      var reader = _commandBuffer.ExececuteReader(
        "SELECT addr.ROAD_LINK_ID, addr.LEFT_POSTAL_CODE, addr.STREET_BASE_NAME, pt.ADDRESS, pt.LAT, pt.LNG " +
        $"FROM {_rdfAddrTable} " +
        $" LEFT JOIN {_rdfPointTable} pt using (ROAD_LINK_ID) " +
        "WHERE " +
        " addr.LEFT_POSTAL_CODE = @1 AND " +
        " addr.STREET_BASE_NAME = @2 AND " +
        " pt.HNO = @3 AND " +
        " pt.HNO_EXTENSION = @4 " +
        " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL ",
        zip, streetName, houseNumber, houseNumberExtension);

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
            Zip = reader.GetString("LEFT_POSTAL_CODE"),
            StreetBaseName = reader.GetString("STREET_BASE_NAME"),
            Coordinates = coordinates
          };
          coordinateList.Add(rdfPointEntry);
        }
      }
      return coordinateList;
    }
  }
}
