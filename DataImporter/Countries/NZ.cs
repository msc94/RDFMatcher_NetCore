using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

/*
+		[0]	{[ACTION_INDICATOR, 0]}	System.Collections.Generic.KeyValuePair<string, int>
+		[1]	{[DELIVERY_POINT_ID, 1]}	System.Collections.Generic.KeyValuePair<string, int>
+		[2]	{[ADDRESS_TYPE, 2]}	System.Collections.Generic.KeyValuePair<string, int>
+		[3]	{[STREET_NUMBER, 3]}	System.Collections.Generic.KeyValuePair<string, int>
+		[4]	{[STREET_ALPHA, 4]}	System.Collections.Generic.KeyValuePair<string, int>
+		[5]	{[UNIT_TYPE, 5]}	System.Collections.Generic.KeyValuePair<string, int>
+		[6]	{[UNIT_IDENTIFIER, 6]}	System.Collections.Generic.KeyValuePair<string, int>
+		[7]	{[FLOOR, 7]}	System.Collections.Generic.KeyValuePair<string, int>
+		[8]	{[BUILDING_NAME, 8]}	System.Collections.Generic.KeyValuePair<string, int>
+		[9]	{[STREET_ALIAS_ID, 9]}	System.Collections.Generic.KeyValuePair<string, int>
+		[10]	{[STREET_NAME, 10]}	System.Collections.Generic.KeyValuePair<string, int>
+		[11]	{[STREET_TYPE, 11]}	System.Collections.Generic.KeyValuePair<string, int>
+		[12]	{[STREET_DIRECTION, 12]}	System.Collections.Generic.KeyValuePair<string, int>
+		[13]	{[DELIVERY_SERVICE_TYPE, 13]}	System.Collections.Generic.KeyValuePair<string, int>
+		[14]	{[BOX_BAG_NUMBER, 14]}	System.Collections.Generic.KeyValuePair<string, int>
+		[15]	{[BOX_BAG_LOBBY_NAME, 15]}	System.Collections.Generic.KeyValuePair<string, int>
+		[16]	{[SUBURB_ALIAS_ID, 16]}	System.Collections.Generic.KeyValuePair<string, int>
+		[17]	{[SUBURB_NAME, 17]}	System.Collections.Generic.KeyValuePair<string, int>
+		[18]	{[TOWN_CITY_MAILTOWN_ALIAS_ID, 18]}	System.Collections.Generic.KeyValuePair<string, int>
+		[19]	{[TOWN_CITY_MAILTOWN, 19]}	System.Collections.Generic.KeyValuePair<string, int>
+		[20]	{[POSTCODE, 20]}	System.Collections.Generic.KeyValuePair<string, int>
+		[21]	{[RD_NUMBER, 21]}	System.Collections.Generic.KeyValuePair<string, int>
+		[22]	{[OLD_POSTCODE, 22]}	System.Collections.Generic.KeyValuePair<string, int>
+		[23]	{[RECORD_USAGE_ID, 23]}	System.Collections.Generic.KeyValuePair<string, int>
*/

namespace DataImporter.Countries
{
  class NZ
  {
    const string connectionString =
    "server=localhost;" +
    "uid=root;" +
    "pwd=bloodrayne;" +
    "database=nz;" +
    "connection timeout=1000;" +
    "command timeout=1000;" +
    "CharSet=utf8";

    public static List<string> GetEntry(Dictionary<string, List<string>> dic, string key)
    {
      if (!dic.ContainsKey(key))
        return new List<string>();

      return dic[key];
    }

    public static Dictionary<string, List<string>> ReadAliases(string path, bool readThree = false)
    {
      Dictionary<string, List<string>> result = new Dictionary<string, List<string>>();

      using (var reader = new StreamReader(path, Encoding.UTF8))
      {
        // Skip header
        string header = reader.ReadLine();

        string line;
        while ((line = reader.ReadLine()) != null)
        {
          if (!line.Contains("|"))
            continue;

          string[] values = line.Split('|');
          string key = values[1];
          string value = values[2];

          if (readThree)
            value += values[3];

          if (!result.ContainsKey(key))
            result[key] = new List<string>();

          result[key].Add(value);
        }
      }

      return result;
    }

    public static int LoadFile()
    {
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE building;");
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street_zip;");
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE street;");
      MySqlHelper.ExecuteNonQuery(connectionString, "TRUNCATE TABLE zone;");


      var streetAliasesDic = ReadAliases(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ALTERNATIVE_STREET_NAMES.csv", true);
      var suburbAliasesDic = ReadAliases(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ALTERNATIVE_SUBURB_NAMES.csv");
      var townAliasesDic = ReadAliases(@"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_ALTERNATIVE_TOWN_CITY_NAMES.csv");

      string path = @"G:\SQL\NZ\PAF2_V2017Q3V01\PAF2_V2017Q3V01_DELIVERY_ADDRESSES.csv";

      int currentNumber = 0;
      using (var reader = new StreamReader(path, Encoding.UTF8))
      {
        string header = reader.ReadLine();
        string[] fields = header.Split('|');

        var fieldIndexes = new Dictionary<string, int>();
        for (int i = 0; i < fields.Length; i++)
        {
          fieldIndexes[fields[i]] = i;
        }

        string line;
        while ((line = reader.ReadLine()) != null)
        {
          if (!line.Contains("|"))
            continue;

          string[] values = line.Split('|');

          var streetAliases = GetEntry(streetAliasesDic, values[fieldIndexes["STREET_ALIAS_ID"]]);
          var suburbAliases = GetEntry(suburbAliasesDic, values[fieldIndexes["SUBURB_ALIAS_ID"]]);
          var townAliases = GetEntry(townAliasesDic, values[fieldIndexes["TOWN_CITY_MAILTOWN_ALIAS_ID"]]);

          Building building = new Building
          {
            ForeignKey = values[fieldIndexes["DELIVERY_POINT_ID"]],
            HouseNumber = values[fieldIndexes["STREET_NUMBER"]],
            HouseNumberExtension = values[fieldIndexes["STREET_ALPHA"]],
            StreetZip = new StreetZip
            {
              Zip = values[fieldIndexes["POSTCODE"]],
              Street = new Street
              {
                Name = values[fieldIndexes["STREET_NAME"]] + values[fieldIndexes["STREET_TYPE"]],
                Aliases = streetAliases
              }
            }
          };

          var townCityZone = new Zone
          {
            Level = 1,
            Name = values[fieldIndexes["TOWN_CITY_MAILTOWN"]],
            Aliases = townAliases
          };

          string suburbName = values[fieldIndexes["SUBURB_NAME"]];
          if (suburbName != "")
          {
            building.StreetZip.Street.Zone = new Zone
            {
              Level = 2,
              ParentZone = townCityZone,
              Name = suburbName,
              Aliases = townAliases
            };
          }
          else
          {
            building.StreetZip.Street.Zone = townCityZone;
          }
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
