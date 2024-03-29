﻿using DatabaseLibrary;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataImporter.Countries
{
  class RUS
  {
    const bool local = true;
    private const string connectionString =
      "server=" + (local ? "localhost" : "h2744269.stratoserver.net") + ";" +
      "uid=Marcel;" +
      "pwd=YyQzKeSSX0TlgsI4;" +
      "database=RUS;" +
      "connection timeout=10000;" +
      "command timeout=10000;" +
      "charset=utf8";

    internal static long LoadFile()
    {
      //InsertZones();
      //InsertStreets();
      //InsertZips();
      InsertBuildings();

      return 0;
    }

    private static (string number, string extension) SplitHouseNumber(string houseNumberString)
    {
      int extensionStart = 0;
      while (extensionStart < houseNumberString.Length && char.IsDigit(houseNumberString[extensionStart]))
      {
        extensionStart++;
      }

      string extension = houseNumberString.Substring(extensionStart);
      string number = houseNumberString.Substring(0, extensionStart);

      return (number, extension);
    }

    private static void InsertBuildings()
    {
      Console.WriteLine("Inserting buildings...");
      var taskList = new List<Task>();
      using (var reader = DatabaseHelper.ExecuteReader(connectionString,
        "SELECT GUID, HOUSE_NUMBER_EN, HOUSE_NUMBER, POSTAL_CODE, STREET_F_EN, STREET_S_EN, PLACE_F_EN, CITY_F_EN, AREA_F_EN, REGION_F_EN " +
        "FROM input " +
        "WHERE HOUSE_NUMBER_EN <> '' " +
        "AND GUID NOT IN (SELECT FOREIN_KEY FROM building);"))
      {
        while (reader.Read())
        {
          var guid = reader.GetString("GUID");
          var houseNumberString = reader.GetString("HOUSE_NUMBER_EN");
          var houseNumberRuString = reader.GetString("HOUSE_NUMBER");
          var zip = reader.GetString("POSTAL_CODE");
          var streetName = reader.GetString("STREET_F_EN");
          var streetType = reader.GetString("STREET_S_EN");

          var level1ZoneName = reader.GetString("REGION_F_EN");
          var level2ZoneName = reader.GetString("AREA_F_EN");
          var level3ZoneName = reader.GetString("CITY_F_EN");
          var level4ZoneName = reader.GetString("PLACE_F_EN");

          taskList.Add(Task.Run(() =>
          {
            using (var taskConnection = new MySqlConnection(connectionString))
            {
              taskConnection.Open();

              var (number, extension) = SplitHouseNumber(houseNumberString);
              var (numberRu, extensionRu) = SplitHouseNumber(houseNumberRuString);

              object level1ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1", level1ZoneName);
              object level2ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2", level1ZoneId, level2ZoneName);
              object level3ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 3 AND LEVEL_1_ZONE_ID = @1 AND LEVEL_2_ZONE_ID = @2 AND ZONE_NAME = @3", level1ZoneId, level2ZoneId, level3ZoneName);
              object level4ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 4 AND LEVEL_1_ZONE_ID = @1 AND LEVEL_2_ZONE_ID = @2 AND LEVEL_3_ZONE_ID = @3 AND ZONE_NAME = @4", level1ZoneId, level2ZoneId, level3ZoneId, level4ZoneName);

              object streetId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2 AND TYPE = @3 AND IS_ALIAS IS NULL", level4ZoneId, streetName, streetType);
              object streetZipId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM street_zip WHERE STREET_ID = @1 AND ZIP = @2", streetId, zip);

              long numberOfBuildings = (long)DatabaseHelper.ExecuteScalar(taskConnection, "SELECT COUNT(*) FROM building WHERE STREET_ZIP_ID = @1 AND HNO = @2 AND HNO_EXTENSION = @3;",
                streetZipId, number, extension);

              if (numberOfBuildings > 0)
                return;

              DatabaseHelper.ExecuteNonQuery(taskConnection, "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_RU, HNO_EXTENSION, HNO_EXTENSION_RU, FOREIN_KEY) VALUES(@1, @2, @3, @4, @5, @6)",
                streetZipId, number, numberRu, extension, extensionRu, guid);
            }
          }));
        }
      }

      var whenAll = Task.WhenAll(taskList);
      while (!whenAll.IsCompleted)
      {
        Thread.Sleep(5 * 1000);
        var removed = taskList.RemoveAll(t => t.IsCompleted);
        Console.WriteLine($"Removed {removed} tasks. {taskList.Count} left");
      }
    }

    private static void InsertStreets()
    {
      DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street;");

      Console.WriteLine("Inserting streets...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString,
        "SELECT DISTINCT STREET_F_EN, STREET_F, STREET_S_EN, STREET_S, PLACE_F_EN, CITY_F_EN, AREA_F_EN, REGION_F_EN FROM input;"))
      {
        while (reader.Read())
        {
          object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1",
            reader.GetString("REGION_F_EN"));

          object level2ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2",
            reader.GetString("AREA_F_EN"), level1ZoneId);

          object level3ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 3 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2 AND LEVEL_2_ZONE_ID = @3",
            reader.GetString("CITY_F_EN"), level1ZoneId, level2ZoneId);

          object level4ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 4 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2 AND LEVEL_2_ZONE_ID = @3 AND LEVEL_3_ZONE_ID = @4",
            reader.GetString("PLACE_F_EN"), level1ZoneId, level2ZoneId, level3ZoneId);

          object streetId = DatabaseHelper.ExecuteScalar(connectionString, "INSERT INTO street (NAME, TYPE, ZONE_ID) VALUES(@1, @2, @3); SELECT LAST_INSERT_ID();",
              reader.GetString("STREET_F_EN"), reader.GetString("STREET_S_EN"), level4ZoneId);

          DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO street (NAME, TYPE, IS_ALIAS, STREET_MASTER_ID) VALUES(@1, @2, 1, @3);",
            reader.GetString("STREET_F"), reader.GetString("STREET_S"), streetId);
        }
      }
    }

    private static void InsertZips()
    {
      DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street_zip;");

      Console.WriteLine("Inserting zips...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString,
        "SELECT DISTINCT POSTAL_CODE, STREET_F_EN, STREET_S_EN, PLACE_F_EN, CITY_F_EN, AREA_F_EN, REGION_F_EN FROM input;"))
      {
        while (reader.Read())
        {
          object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1",
            reader.GetString("REGION_F_EN"));

          object level2ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2",
            reader.GetString("AREA_F_EN"), level1ZoneId);

          object level3ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 3 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2 AND LEVEL_2_ZONE_ID = @3",
            reader.GetString("CITY_F_EN"), level1ZoneId, level2ZoneId);

          object level4ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 4 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2 AND LEVEL_2_ZONE_ID = @3 AND LEVEL_3_ZONE_ID = @4",
            reader.GetString("PLACE_F_EN"), level1ZoneId, level2ZoneId, level3ZoneId);

          object streetId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2 AND TYPE = @3",
              level4ZoneId, reader.GetString("STREET_F_EN"), reader.GetString("STREET_S_EN"));

          DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO street_zip (STREET_ID, ZIP) VALUES(@1, @2)",
              streetId, reader.GetString("POSTAL_CODE"));
        }
      }
    }

    private static void InsertZones()
    {
      DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE zone;");

      Console.WriteLine("Inserting level 1 zones...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT REGION_F_EN, REGION_F FROM input"))
      {
        while (reader.Read())
        {
          object zoneId = DatabaseHelper.ExecuteScalar(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL) VALUES(@1, 1); SELECT LAST_INSERT_ID();",
            reader.GetString("REGION_F_EN"));

          DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, IS_ALIAS, ZONE_MASTER_ID) VALUES(@1, 1, @2);",
            reader.GetString("REGION_F"), zoneId);
        }
      }

      Console.WriteLine("Inserting level 2 zones...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT AREA_F_EN, AREA_F, REGION_F_EN FROM input"))
      {
        while (reader.Read())
        {
          object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1",
            reader.GetString("REGION_F_EN"));

          object zoneId = DatabaseHelper.ExecuteScalar(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, LEVEL_1_ZONE_ID) VALUES(@1, 2, @2); SELECT LAST_INSERT_ID();",
            reader.GetString("AREA_F_EN"), level1ZoneId);

          DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, IS_ALIAS, ZONE_MASTER_ID) VALUES(@1, 1, @2);",
            reader.GetString("AREA_F"), zoneId);
        }
      }

      Console.WriteLine("Inserting level 3 zones...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT CITY_F_EN, CITY_F, AREA_F_EN, REGION_F_EN FROM input"))
      {
        while (reader.Read())
        {
          object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1",
            reader.GetString("REGION_F_EN"));

          object level2ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2",
            reader.GetString("AREA_F_EN"), level1ZoneId);

          object zoneId = DatabaseHelper.ExecuteScalar(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, LEVEL_1_ZONE_ID, LEVEL_2_ZONE_ID) VALUES(@1, 3, @2, @3); SELECT LAST_INSERT_ID();",
            reader.GetString("CITY_F_EN"), level1ZoneId, level2ZoneId);

          DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, IS_ALIAS, ZONE_MASTER_ID) VALUES(@1, 1, @2);",
            reader.GetString("CITY_F"), zoneId);
        }
      }

      Console.WriteLine("Inserting level 4 zones...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT PLACE_F_EN, PLACE_F, CITY_F_EN, AREA_F_EN, REGION_F_EN FROM input;"))
      {
        while (reader.Read())
        {
          object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1",
            reader.GetString("REGION_F_EN"));

          object level2ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2",
            reader.GetString("AREA_F_EN"), level1ZoneId);

          object level3ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 3 AND ZONE_NAME = @1 AND LEVEL_1_ZONE_ID = @2 AND LEVEL_2_ZONE_ID = @3",
            reader.GetString("CITY_F_EN"), level1ZoneId, level2ZoneId);

          object zoneId = DatabaseHelper.ExecuteScalar(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, LEVEL_1_ZONE_ID, LEVEL_2_ZONE_ID, LEVEL_3_ZONE_ID) VALUES(@1, 4, @2, @3, @4); SELECT LAST_INSERT_ID();",
            reader.GetString("PLACE_F_EN"), level1ZoneId, level2ZoneId, level3ZoneId);

          DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, IS_ALIAS, ZONE_MASTER_ID) VALUES(@1, 1, @2);",
            reader.GetString("PLACE_F"), zoneId);
        }
      }
    }
  }
}
