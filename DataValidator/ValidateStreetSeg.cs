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
  struct Entry
  {
    public int id;
    public string hnStart;
    public string hnEnd;
    public string name;
    public string coordinates;
    public string zip;
  }

  class ValidateStreetSeg : IRunMode
  {

    private List<Entry> _entries = new List<Entry>();
    private Random _rand = new Random();
    private Entry _currentEntry;

    public void LoadEntries()
    {
      const string getEntry =
        "SELECT ss.ID, HN_START, HN_END, s.NAME, sz.ZIP, COORDINATES " +
        "FROM street_seg ss " +
        "  LEFT JOIN street_zip sz ON sz.ID = ss.STREET_ZIP_ID " +
        "  LEFT JOIN street s on s.ID = sz.STREET_ID " +
        "  LEFT JOIN street_seg_koo_group sskg ON ss.ID = sskg.STREET_SEG_ID " +
        "WHERE COORDINATES IS NOT NULL";

      Task.Run(() =>
      {
        _entries.Clear();
        var reader = MySqlHelper.ExecuteReader(MainWindow.connectionString, getEntry);
        while (reader.Read())
        {
          _entries.Add(new Entry
          {
            id = reader.GetInt32("ID"),
            hnStart = reader.GetString("HN_START"),
            hnEnd = reader.GetString("HN_END"),
            name = reader.GetString("NAME"),
            zip = reader.GetString("ZIP"),
            coordinates = reader.GetString("COORDINATES")
          });
        }
      });
    }

    public void NextEntry()
    {
      do
      {
        _currentEntry = _entries[_rand.Next(_entries.Count)];
      } while (_currentEntry.coordinates.Split('@').Length < 2);
    }

    public void FillMap(Map map, Label streetLabel)
    {
      streetLabel.Content = _currentEntry.id + ": " + _currentEntry.zip + " " + _currentEntry.name + " " + _currentEntry.hnStart + " - " + _currentEntry.hnEnd;
      var newLocations = new List<Location>();
      var coordinates = _currentEntry.coordinates.Split('@');
      foreach (var coordinate in coordinates)
      {
        var latLng = coordinate.Split(',');

        Debug.Assert(latLng.Length == 2);

        var lat = double.Parse(latLng[0].Replace('.', ','));
        var lng = double.Parse(latLng[1].Replace('.', ','));

        newLocations.Add(new Location(lat, lng));
      }

      map.Children.Clear();

      int ppNumber = 0;
      foreach (var location in newLocations)
      {
        var pp = new Pushpin { Location = location, Content = ppNumber++.ToString() };
        map.Children.Add(pp);
      }

      var locationRect = new LocationRect(newLocations);
      map.SetView(locationRect);
    }
  }
}
