using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DatabaseLibrary
{
  class DatabaseInsert
  {
    readonly string _connectionString;

    public DatabaseInsert(string connectionString)
    {
      _connectionString = connectionString;
    }

    public void InsertBuilding(Building building)
    {
      var streetZipID = FindOrCreateStreetZip(building.StreetZip);

      string commandText =
        "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, FOREIN_KEY) " +
        "VALUES (@1, @2, @3, @4);";
      MySqlHelper.ExecuteScalar(_connectionString, commandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", streetZipID),
          new MySqlParameter("@2", building.HouseNumber),
          new MySqlParameter("@3", building.HouseNumberExtension),
          new MySqlParameter("@4", building.ForeignKey)
        });
    }

    public ulong FindOrCreateStreetZip(StreetZip streetZip)
    {
      var streetID = FindOrCreateStreet(streetZip.Street);

      string commandText =
        "SELECT ID FROM street_zip " +
        "WHERE " +
        " ZIP = @1 AND" +
        " STREET_ID = @2";
      var streetZipID = MySqlHelper.ExecuteScalar(_connectionString, commandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", streetZip.Zip),
          new MySqlParameter("@2", streetID)
        });

      if (streetZipID == null)
      {
        commandText =
          "INSERT INTO street_zip (ZIP, STREET_ID) " +
          "VALUES (@1, @2); " +
          "SELECT LAST_INSERT_ID()";
        streetZipID = MySqlHelper.ExecuteScalar(_connectionString, commandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", streetZip.Zip),
          new MySqlParameter("@2", streetID)
        });
      }

      return Convert.ToUInt64(streetZipID);
    }

    public ulong FindOrCreateStreet(Street street)
    {
      var zoneID = FindOrCreateZone(street.Zone);

      string commandText = "SELECT ID FROM street " +
        "WHERE " +
        " NAME = @1 AND " +
        " TYPE = @2 AND " +
        " ZONE_ID = @3;";

      var streetID = MySqlHelper.ExecuteScalar(_connectionString,
        commandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", street.Name),
          new MySqlParameter("@2", street.Type),
          new MySqlParameter("@3", zoneID)
        });

      if (streetID == null)
        streetID = InsertStreet(street, zoneID);

      return Convert.ToUInt64(streetID);
    }

    private ulong InsertStreet(Street street, ulong zoneID)
    {
      object streetID;

      string commandText = "INSERT INTO street (NAME, ZONE_ID, TYPE) " +
      "VALUES (@1, @2, @3); " +
      "SELECT LAST_INSERT_ID()";

      streetID = MySqlHelper.ExecuteScalar(_connectionString, commandText,
      new MySqlParameter[]
      {
          new MySqlParameter("@1", street.Name),
          new MySqlParameter("@2", zoneID),
          new MySqlParameter("@3", street.Type)
      });

      // Insert aliases
      commandText = "INSERT INTO street (NAME, TYPE, IS_ALIAS, STREET_MASTER_ID) VALUES (@1, @2, 1, @3)";
      foreach (var alias in street.Aliases)
      {
        MySqlHelper.ExecuteNonQuery(_connectionString, commandText,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", alias.Name),
          new MySqlParameter("@2", alias.Type),
          new MySqlParameter("@3", streetID)
        });
      }

      return Convert.ToUInt64(streetID);
    }

    public ulong FindOrCreateZone(Zone zone)
    {
      ulong? parentID = null;
      if (zone.ParentZone != null)
        parentID = FindOrCreateZone(zone.ParentZone);

      // If we have a community key, compare with that, otherwise, use the name
      string zoneSearchString = "FALSE";
      if (zone.CommunityKey != null)
      {
        zoneSearchString = $"COMMUNITY_KEY = '{zone.CommunityKey.Replace("'", "''")}'";
      }
      else
      {
        zoneSearchString = $"ZONE_NAME = '{zone.Name.Replace("'", "''")}'";
      }

      string parentSearchString = "";
      if (parentID != null)
      {
        parentSearchString = $"AND LEVEL_{zone.Level - 1}_ZONE_ID = {parentID}";
      }


      string commandText = "SELECT ID " +
        "FROM zone " +
        $"WHERE {zoneSearchString} {parentSearchString}";
      var zoneID = MySqlHelper.ExecuteScalar(_connectionString, commandText);

      if (zoneID == null)
        zoneID = InsertZone(zone, parentID);

      return Convert.ToUInt64(zoneID);
    }

    private ulong InsertZone(Zone zone, ulong? parentID)
    {
      string commandText;
      object zoneID;

      if (parentID != null)
      {
        commandText = $"INSERT INTO zone (ZONE_NAME, LEVEL, LEVEL_{zone.Level - 1}_ZONE_ID, COMMUNITY_KEY) " +
          "VALUES (@1, @2, @3, @4); " +
          "SELECT LAST_INSERT_ID();";

        zoneID = MySqlHelper.ExecuteScalar(_connectionString, commandText,
        new MySqlParameter[]
        {
            new MySqlParameter("@1", zone.Name),
            new MySqlParameter("@2", zone.Level),
            new MySqlParameter("@3", parentID),
            new MySqlParameter("@4", zone.CommunityKey)
        });
      }
      else
      {
        commandText = $"INSERT INTO zone (ZONE_NAME, LEVEL, COMMUNITY_KEY) " +
          "VALUES (@1, @2, @3); " +
          "SELECT LAST_INSERT_ID();";

        zoneID = MySqlHelper.ExecuteScalar(_connectionString, commandText,
        new MySqlParameter[]
        {
            new MySqlParameter("@1", zone.Name),
            new MySqlParameter("@2", zone.Level),
            new MySqlParameter("@3", zone.CommunityKey)
        });
      }

      return Convert.ToUInt64(zoneID);

    }
  }
}
