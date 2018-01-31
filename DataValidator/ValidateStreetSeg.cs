using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;
using System.Windows.Media;
using Microsoft.Maps.MapControl.WPF;
using MySql.Data.MySqlClient;

namespace DataValidator
{
  

  class Building
  {
    public string HouseNumber;
    public Coordinate Coordinate;
  }

  class Entry
  {
    public int Id;
    public int StreetZipId;

    public string HouseNumberStart;
    public string HouseNumberEnd;

    public string Name;
    public string Zip;

    public Coordinate Center;
    public List<Coordinate> Coordinates = new List<Coordinate>();
    public List<Building> Buildings = new List<Building>();
  }

  class ValidateStreetSeg : IRunMode
  {
    private List<Entry> _entries = new List<Entry>();
    private Random _rand = new Random();
    private Entry _currentEntry;

    private readonly bool _showMatchedHouses;

    public ValidateStreetSeg(bool showMatchedHouses)
    {
      _showMatchedHouses = showMatchedHouses;
    }

    private void ReadBuildings(Entry item)
    {
      const string getBuildings =
        "SELECT b.HNO, b.AP_LAT, b.AP_LNG " +
        "FROM building b " +
        "WHERE b.STREET_ZIP_ID = @1 AND " +
        " b.AP_LAT IS NOT NULL AND b.AP_LNG IS NOT NULL;";

      var reader = MySqlHelper.ExecuteReader(MainWindow.ConnectionString, getBuildings,
        new MySqlParameter[]
        {
          new MySqlParameter("@1", item.StreetZipId)
        });


      using (reader)
      {
        while (reader.Read())
        {
          item.Buildings.Add(
            new Building
            {
              Coordinate = new Coordinate
              {
                Lat = Utils.ParseDoubleInvariantCulture(reader.GetString("AP_LAT")),
                Lng = Utils.ParseDoubleInvariantCulture(reader.GetString("AP_LNG"))
              },
              HouseNumber = reader.GetString("HNO").TrimStart('0')
            });
        }
      }
    }

    private static void SplitCoordinates(Entry item, string coordinatesString)
    {
      var coordinatesSplit = coordinatesString.Split('@');
      foreach (var coordinate in coordinatesSplit)
      {
        var latLng = coordinate.Split(',');

        Debug.Assert(latLng.Length == 2);

        var lat = Utils.ParseDoubleInvariantCulture(latLng[0]);
        var lng = Utils.ParseDoubleInvariantCulture(latLng[1]);

        item.Coordinates.Add(new Coordinate { Lat = lat, Lng = lng });
      }
    }

    public async void LoadEntries(Label statusLabel)
    {
      const string getEntry =
        "SELECT ss.ID as ss_ID, ss.HN_START, ss.HN_END, ss.CENTER_LAT, ss.CENTER_LNG, s.NAME, sz.ID as sz_ID, sz.ZIP, COORDINATES " +
        "FROM street_seg ss " +
        "  LEFT JOIN street_zip sz ON sz.ID = ss.STREET_ZIP_ID " +
        "  LEFT JOIN street s on s.ID = sz.STREET_ID " +
        "  LEFT JOIN street_seg_koo_group sskg ON ss.ID = sskg.STREET_SEG_ID " +
        "WHERE COORDINATES IS NOT NULL AND ss.ID = 201;";

      await Task.Run(() =>
      {
        _entries.Clear();

        var reader = MySqlHelper.ExecuteReader(MainWindow.ConnectionString, getEntry);
        while (reader.Read())
        {
          Entry item = new Entry
          {
            Id = reader.GetInt32("ss_ID"),
            StreetZipId = reader.GetInt32("sz_ID"),

            HouseNumberStart = reader.GetString("HN_START"),
            HouseNumberEnd = reader.GetString("HN_END"),

            Name = reader.GetString("NAME"),
            Zip = reader.GetString("ZIP"),

            Center = new Coordinate
            {
              Lat = reader.GetDouble("CENTER_LAT"),
              Lng = reader.GetDouble("CENTER_LNG")
            }
          };

          var coordinatesString = reader.GetString("COORDINATES");
          SplitCoordinates(item, coordinatesString);

          ReadBuildings(item);

          _entries.Add(item);
        }

        reader.Close();
      });
      statusLabel.Content = $"Loaded {_entries.Count} entries";
    }

    private const int _minCoordinates = 5;
    public void NextEntry()
    {
      do
      {
        _currentEntry = _entries[_rand.Next(_entries.Count)];
      } while (_currentEntry.Coordinates.Count < _minCoordinates);
    }

    public void FillMap(Map map, Label streetLabel)
    {
      streetLabel.Content = _currentEntry.Id + ": " + _currentEntry.Zip + " " + _currentEntry.Name + " " + _currentEntry.HouseNumberStart + " - " + _currentEntry.HouseNumberEnd;

      map.Children.Clear();

      // Fill Street Segment
      var streetSegmentLocations = _currentEntry.Coordinates.Select((c) => new Location(c.Lat, c.Lng)).ToList();
      int ppNumber = 0;
      foreach (var location in streetSegmentLocations)
      {
        var pp = new Pushpin { Location = location, Content = ppNumber++.ToString() };
        pp.Background = new SolidColorBrush(Color.FromArgb(255, 255, 0, 0));
        map.Children.Add(pp);
      }

      // Fill buildings
      if (_showMatchedHouses)
      {
        foreach (var building in _currentEntry.Buildings)
        {
          var location = new Location(building.Coordinate.Lat, building.Coordinate.Lng);
          var pp = new Pushpin { Location = location, Content = building.HouseNumber };
          pp.Background = new SolidColorBrush(Color.FromArgb(255, 0, 255, 0));
          map.Children.Add(pp);
        }
      }

      // Show Center
      var center = _currentEntry.Center;
      var centerPushpin = new Pushpin { Location = new Location(center.Lat, center.Lng) };
      centerPushpin.Background = new SolidColorBrush(Color.FromArgb(255, 0, 0, 255));
      map.Children.Add(centerPushpin);

      var locationRect = new LocationRect(streetSegmentLocations);
      map.SetView(locationRect);
    }
  }
}
