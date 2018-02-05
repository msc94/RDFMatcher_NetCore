using DatabaseLibrary;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DataImporter.Countries
{
  internal class NOR
  {
    const string connectionString =
      "server=localhost;" +
      "uid=root;" +
      "pwd=bloodrayne;" +
      "database=nor;" +
      "connection timeout=1000;" +
      "command timeout=1000;" +
      "CharSet=utf8";

    internal static int LoadFile()
    {
      //DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE building;");
      //DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street_zip;");
      //DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street;");
      //DatabaseHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE zone;");

      //Console.WriteLine("Inserting zones...");
      //using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT COMMUNE, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, FOREIN_KEY) VALUES(@1, 1, @2)",
      //        reader.GetString("COMMUNE"), reader.GetString("COMMUNE_NUMBER"));
      //  }
      //}

      //using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT CITY, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
      //      reader.GetString("COMMUNE_NUMBER"));

      //    DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, LEVEL_1_ZONE_ID) VALUES(@1, 2, @2)",
      //      reader.GetString("CITY"), level1ZoneId);
      //  }
      //}

      //Console.WriteLine("Inserting streets...");
      //using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT STREET_NAME, CITY, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
      //        reader.GetString("COMMUNE_NUMBER"));

      //    object level2ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2",
      //        level1ZoneId, reader.GetString("CITY"));


      //    DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO street (NAME, ZONE_ID) VALUES(@1, @2)",
      //        reader.GetString("STREET_NAME"), level2ZoneId);
      //  }
      //}

      //Console.WriteLine("Inserting zips...");
      //using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT ZIP, STREET_NAME, CITY, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    object level1ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
      //        reader.GetString("COMMUNE_NUMBER"));

      //    object level2ZoneId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2",
      //        level1ZoneId, reader.GetString("CITY"));

      //    object streetId = DatabaseHelper.ExecuteScalar(connectionString, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2",
      //        level2ZoneId, reader.GetString("STREET_NAME"));

      //    DatabaseHelper.ExecuteNonQuery(connectionString, "INSERT INTO street_zip (STREET_ID, ZIP) VALUES(@1, @2)",
      //        streetId, reader.GetString("ZIP"));
      //  }
      //}

      Console.WriteLine("Inserting buildings...");
      var taskList = new List<Task>();
      using (var reader = DatabaseHelper.ExecuteReader(connectionString, "SELECT DISTINCT ID, HOUSE_NUMBER, HOUSE_NUMBER_EXTENSION, ZIP, STREET_NAME, CITY, COMMUNE_NUMBER FROM input"))
      {
        int current = 0;

        while (reader.Read())
        {
          string id = reader.GetString("ID");
          string houseNumber = reader.GetString("HOUSE_NUMBER");
          string houseNumberExtension = reader.GetString("HOUSE_NUMBER_EXTENSION");
          string zip = reader.GetString("ZIP");
          string streetName = reader.GetString("STREET_NAME");
          string city = reader.GetString("CITY");
          string communeNumber = reader.GetString("COMMUNE_NUMBER");

          taskList.Add(Task.Run(() =>
          {
            var taskConnection = new MySqlConnection(connectionString);
            taskConnection.Open();

            object level1ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1", communeNumber);
            object level2ZoneId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2", level1ZoneId, city);
            object streetId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2", level2ZoneId, streetName);
            object streetZipId = DatabaseHelper.ExecuteScalar(taskConnection, "SELECT ID FROM street_zip WHERE STREET_ID = @1 AND ZIP = @2", streetId, zip);

            DatabaseHelper.ExecuteNonQuery(taskConnection, "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, FOREIN_KEY) VALUES(@1, @2, @3, @4)",
              streetZipId, houseNumber, houseNumberExtension, id);

            taskConnection.Close();

            Interlocked.Increment(ref current);
            if (current % 1000 == 0)
              Console.WriteLine(current);
          }));
        }
      }

      Task.WaitAll(taskList.ToArray());

      return 0;
    }
  }
}