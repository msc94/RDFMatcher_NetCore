using DatabaseLibrary;
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
    private const string connectionString = local ?
      "server=localhost;" +
      "uid=Marcel;" +
      "pwd=YyQzKeSSX0TlgsI4;" +
      "database=RUS;" +
      "connection timeout=10000;" +
      "command timeout=10000;" +
      "charset=utf8"
      :
      "server=h2744269.stratoserver.net;" +
      "uid=Marcel;" +
      "pwd=YyQzKeSSX0TlgsI4;" +
      "database=RUS;" +
      "connection timeout=10000;" +
      "command timeout=10000;" +
      "charset=utf8";

    internal static long LoadFile()
    {
      // InsertZones();
      // InsertStreets();
      // InsertZips();
      InsertBuildings();

      return 0;
    }

    private static void SplitHouseNumber(ref string houseNumber, out string extension)
    {
      int extensionStart = 0;
      while (extensionStart < houseNumber.Length && char.IsDigit(houseNumber[extensionStart]))
      {
        extensionStart++;
      }

      extension = houseNumber.Substring(extensionStart);
      houseNumber = houseNumber.Substring(0, extensionStart);
    }

    private static void InsertBuildings()
    {
      // DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE building;");

      Console.WriteLine("Inserting buildings...");
      var taskList = new List<Task>();
      using (var reader = DatabaseHelper.ExecuteReader(connectionString,
        "SELECT GUID, HOUSE_NUMBER_EN, HOUSE_NUMBER, POSTAL_CODE, STREET_F_EN, STREET_S_EN, PLACE_F_EN, CITY_F_EN, AREA_F_EN, REGION_F_EN " +
        "FROM input " +
        "WHERE HOUSE_NUMBER_EN <> '' " +
        "AND GUID NOT IN (SELECT FOREIN_KEY FROM building)"))
      {
        int current = 0;

        while (reader.Read())
        {
          string guid = reader.GetString("GUID");
          string houseNumber = reader.GetString("HOUSE_NUMBER_EN");
          string houseNumberRu = reader.GetString("HOUSE_NUMBER");
          string zip = reader.GetString("POSTAL_CODE");
          string streetName = reader.GetString("STREET_F_EN");
          string streetType = reader.GetString("STREET_S_EN");

          string level1ZoneName = reader.GetString("REGION_F_EN");
          string level2ZoneName = reader.GetString("AREA_F_EN");
          string level3ZoneName = reader.GetString("CITY_F_EN");
          string level4ZoneName = reader.GetString("PLACE_F_EN");

          taskList.Add(Task.Run(() =>
          {
            var taskConnection = new MySqlConnection(connectionString);
            taskConnection.Open();

            SplitHouseNumber(ref houseNumber, out string houseNumberExtension);
            SplitHouseNumber(ref houseNumberRu, out string houseNumberExtensionRu);

            object level1ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 1 AND ZONE_NAME = @1", level1ZoneName);
            object level2ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2", level1ZoneId, level2ZoneName);
            object level3ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 3 AND LEVEL_1_ZONE_ID = @1 AND LEVEL_2_ZONE_ID = @2 AND ZONE_NAME = @3", level1ZoneId, level2ZoneId, level3ZoneName);
            object level4ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 4 AND LEVEL_1_ZONE_ID = @1 AND LEVEL_2_ZONE_ID = @2 AND LEVEL_3_ZONE_ID = @3 AND ZONE_NAME = @4", level1ZoneId, level2ZoneId, level3ZoneId, level4ZoneName);

            object streetId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2 AND TYPE = @3 AND IS_ALIAS IS NULL", level4ZoneId, streetName, streetType);
            object streetZipId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM street_zip WHERE STREET_ID = @1 AND ZIP = @2", streetId, zip);

            long numberOfBuildings = (long) DatabaseHelper.ExecuteScalar(taskConnection, "SELECT COUNT(*) FROM building WHERE STREET_ZIP_ID = @1 AND HNO = @2 AND HNO_EXTENSION = @3;", 
              streetZipId, houseNumber, houseNumberExtension);
            if (numberOfBuildings == 0)
            {
              DatabaseHelper.ExecuteNonQuery(taskConnection, "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_RU, HNO_EXTENSION, HNO_EXTENSION_RU, FOREIN_KEY) VALUES(@1, @2, @3, @4, @5, @6)",
                streetZipId, houseNumber, houseNumberRu, houseNumberExtension, houseNumberExtensionRu, guid);
            }

            taskConnection.Close();

            Interlocked.Increment(ref current);
            if (current % 100000 == 0)
              Console.WriteLine(current);
          }));


          while (taskList.Count > 1000000)
          {
            Console.Write("Waiting... ");
            Thread.Sleep(30 * 1000);
            var removed = taskList.RemoveAll(t => t.IsCompleted);
            Console.WriteLine($"Removed {removed} tasks.");
          }
        }
      }

      Task.WaitAll(taskList.ToArray());
    }

    private static void InsertStreets()
    {
      DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street;");

      Console.WriteLine("Inserting streets...");
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT STREET_F_EN, STREET_F, STREET_S_EN, STREET_S, PLACE_F_EN, CITY_F_EN, AREA_F_EN, REGION_F_EN FROM input"))
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
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT POSTAL_CODE, STREET_F_EN, STREET_S_EN, PLACE_F_EN, CITY_F_EN, AREA_F_EN, REGION_F_EN FROM input"))
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
