using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;
using Microsoft.Maps.MapControl.WPF;
using MySql.Data.MySqlClient;

namespace DataValidator
{
  struct BuildingEntry
  {
    public int Id;
    public string Hno;
    public string HnoExtension;

    public string StreetName;

    public string PlaceName;
    public string Zip;

    public float ApLat, ApLng;
  }

  class ValidateBuilding : IRunMode
  {

    private List<BuildingEntry> _entries = new List<BuildingEntry>();
    private Random _rand = new Random();
    private BuildingEntry _currentEntry;

    public void LoadEntries()
    {
      const string getEntry =
        "SELECT b.ID, b.HNO, b.HNO_EXTENSION, b.AP_LAT, b.AP_LNG, sz.ZIP, s.NAME, z1.ZONE_NAME " +
        "FROM building b " +
        " LEFT JOIN street_zip sz ON sz.ID = b.STREET_ZIP_ID " +
        " LEFT JOIN street s on s.ID = sz.STREET_ID " +
        " LEFT JOIN zone z1 on s.ZONE_ID = z1.ID ";

      Task.Run(() =>
      {
        var reader = MySqlHelper.ExecuteReader(MainWindow.connectionString, getEntry);
        while (reader.Read())
        {
          string apLat = reader.GetString("AP_LAT").Replace('.', ',');
          string apLng = reader.GetString("AP_LNG").Replace('.', ',');

          _entries.Add(new BuildingEntry()
          {
            Id = reader.GetInt32("ID"),

            Hno = reader.GetString("HNO"),
            HnoExtension = reader.GetString("HNO_EXTENSION"),

            ApLat = float.Parse(apLat),
            ApLng = float.Parse(apLng),

            Zip = reader.GetString("ZIP"),
            StreetName = reader.GetString("Name"),
            PlaceName = reader.GetString("ZONE_NAME")
          });
        }
      }); 

    }

    public void NextEntry()
    {
      _currentEntry = _entries[_rand.Next(_entries.Count)];
    }

    public void FillMap(Map map, Label streetLabel)
    {
      streetLabel.Content = _currentEntry.Id + ": " + _currentEntry.StreetName + " " + _currentEntry.Hno + _currentEntry.HnoExtension + " " + _currentEntry.Zip + " " + _currentEntry.PlaceName;

      var location = new Location(_currentEntry.ApLat, _currentEntry.ApLng);
      var pp = new Pushpin { Location = location };
      map.Children.Add(pp);

      var locationRect = new LocationRect(location, 0.01, 0.01);
      map.SetView(locationRect);
    }
  }
}
