using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataImporter_Test
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
      //MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE building;");
      //MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street_zip;");
      //MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street;");
      //MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE zone;");

      //Console.WriteLine("Inserting zones...");
      //using (var reader = MySqlHelper.ExecuteReader(connectionString, "SELECT DISTINCT COMMUNE, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    MySqlHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, FOREIN_KEY) VALUES(@1, 1, @2)",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", reader.GetString("COMMUNE")),
      //        new MySqlParameter("@2", reader.GetString("COMMUNE_NUMBER"))
      //      });
      //  }
      //}

      //using (var reader = MySqlHelper.ExecuteReader(connectionString, "SELECT DISTINCT CITY, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    object level1ZoneId = MySqlHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", reader.GetString("COMMUNE_NUMBER"))
      //      });

      //    MySqlHelper.ExecuteNonQuery(connectionString, "INSERT INTO zone (ZONE_NAME, LEVEL, LEVEL_1_ZONE_ID) VALUES(@1, 2, @2)",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", reader.GetString("CITY")),
      //        new MySqlParameter("@2", level1ZoneId)
      //      });
      //  }
      //}

      //Console.WriteLine("Inserting streets...");
      //using (var reader = MySqlHelper.ExecuteReader(connectionString, "SELECT DISTINCT STREET_NAME, CITY, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    object level1ZoneId = MySqlHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", reader.GetString("COMMUNE_NUMBER"))
      //      });

      //    object level2ZoneId = MySqlHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", level1ZoneId),
      //        new MySqlParameter("@2", reader.GetString("CITY"))
      //      });


      //    MySqlHelper.ExecuteNonQuery(connectionString, "INSERT INTO street (NAME, ZONE_ID) VALUES(@1, @2)",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", reader.GetString("STREET_NAME")),
      //        new MySqlParameter("@2", level2ZoneId)
      //      });
      //  }
      //}

      //Console.WriteLine("Inserting zips...");
      //using (var reader = MySqlHelper.ExecuteReader(connectionString, "SELECT DISTINCT ZIP, STREET_NAME, CITY, COMMUNE_NUMBER FROM input"))
      //{
      //  while (reader.Read())
      //  {
      //    object level1ZoneId = MySqlHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", reader.GetString("COMMUNE_NUMBER"))
      //      });

      //    object level2ZoneId = MySqlHelper.ExecuteScalar(connectionString, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", level1ZoneId),
      //        new MySqlParameter("@2", reader.GetString("CITY"))
      //      });

      //    object streetId = MySqlHelper.ExecuteScalar(connectionString, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", level2ZoneId),
      //        new MySqlParameter("@2", reader.GetString("STREET_NAME"))
      //      });

      //    MySqlHelper.ExecuteNonQuery(connectionString, "INSERT INTO street_zip (STREET_ID, ZIP) VALUES(@1, @2)",
      //      new MySqlParameter[]
      //      {
      //        new MySqlParameter("@1", streetId),
      //        new MySqlParameter("@2", reader.GetString("ZIP"))
      //      });
      //  }
      //}

      Console.WriteLine("Inserting buildings...");

      var taskList = new List<Task>();

      using (var reader = MySqlHelper.ExecuteReader(connectionString, "SELECT DISTINCT ID, HOUSE_NUMBER, HOUSE_NUMBER_EXTENSION, ZIP, STREET_NAME, CITY, COMMUNE_NUMBER FROM input"))
      {
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
            MySqlConnection connection = new MySqlConnection(connectionString);
            connection.Open();

            object level1ZoneId = MySqlHelper.ExecuteScalar(connection, "SELECT ID FROM zone WHERE LEVEL = 1 AND FOREIN_KEY = @1",
              new MySqlParameter[]
              {
              new MySqlParameter("@1", communeNumber)
              });

            object level2ZoneId = MySqlHelper.ExecuteScalar(connection, "SELECT ID FROM zone WHERE LEVEL = 2 AND LEVEL_1_ZONE_ID = @1 AND ZONE_NAME = @2",
              new MySqlParameter[]
              {
              new MySqlParameter("@1", level1ZoneId),
              new MySqlParameter("@2", city)
              });

            object streetId = MySqlHelper.ExecuteScalar(connection, "SELECT ID FROM street WHERE ZONE_ID = @1 AND NAME = @2",
              new MySqlParameter[]
              {
              new MySqlParameter("@1", level2ZoneId),
              new MySqlParameter("@2", streetName)
              });

            object streetZipId = MySqlHelper.ExecuteScalar(connection, "SELECT ID FROM street_zip WHERE STREET_ID = @1 AND ZIP = @2",
              new MySqlParameter[]
              {
              new MySqlParameter("@1", streetId),
              new MySqlParameter("@2", zip)
              });

            MySqlHelper.ExecuteNonQuery(connection, "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, FOREIN_KEY) VALUES(@1, @2, @3, @4)",
              new MySqlParameter[]
              {
              new MySqlParameter("@1", streetZipId),
              new MySqlParameter("@2", houseNumber),
              new MySqlParameter("@3", houseNumberExtension),
              new MySqlParameter("@4", id),
              });

            connection.Close();
          }));
        }
      }

      Task.WaitAll(taskList.ToArray());

      return 0;
    }
  }
}