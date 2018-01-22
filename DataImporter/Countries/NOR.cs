using MySql.Data.MySqlClient;
using System;

namespace DataImporter
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
      int currentNumber = 0;

      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE building;");
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street_zip;");
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street;");
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE zone;");

      using (var reader = MySqlHelper.ExecuteReader(connectionString, "SELECT * FROM input"))
      {
        while (reader.Read())
        {
          Building building = new Building
          {
            ForeignKey = reader.GetString("ID"),
            HouseNumber = reader.GetString("HOUSE_NUMBER"),
            HouseNumberExtension = reader.GetString("HOUSE_NUMBER_EXTENSION"),
            StreetZip = new StreetZip
            {
              Zip = reader.GetString("ZIP"),
              Street = new Street
              {
                Name = reader.GetString("STREET_NAME")
              }
            }
          };

          var commune = new Zone
          {
            Level = 1,
            CommunityKey = reader.GetString("COMMUNE_NUMBER"),
            Name = reader.GetString("COMMUNE")
          };

          building.StreetZip.Street.Zone = new Zone
          {
            Level = 2,
            ParentZone = commune,
            Name = reader.GetString("CITY")
          };

          new Database(connectionString).InsertBuilding(building);
          currentNumber++;
          if (currentNumber % 1000 == 0)
          {
            Console.WriteLine(currentNumber);
          }
        }
      }
      return currentNumber;
    }
  }
}