using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing.Drawing2D;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Maps.MapControl.WPF;
using MySql.Data.MySqlClient;

namespace DataValidator
{
  /// <summary>
  /// Interaction logic for MainWindow.xaml
  /// </summary>
  public partial class MainWindow : Window
  {
    public MainWindow()
    {
      InitializeComponent();
    }

    struct Entry
    {
      public int id;
      public string hnStart;
      public string hnEnd;
      public string name;
      public string coordinates;
    }

    private List<Entry> _entries = new List<Entry>();

    private void LoadBtn_OnClick(object sender, RoutedEventArgs e)
    {
      string connectionString;

      bool local = true;
      if (local)
      {
        connectionString =
          "server=localhost;" +
          "uid=root;" +
          "pwd=bloodrayne;" +
          "database=test;" +
          "connection timeout=1000;" +
          "command timeout=1000;";
      }
      else
      {
        connectionString =
          "server=h2744269.stratoserver.net;" +
          "uid=Marcel;" +
          "pwd=YyQzKeSSX0TlgsI4;" +
          "database=NLD;" +
          "connection timeout=1000;" +
          "command timeout=1000;";
      }

      const string getEntry =
        "SELECT ss.ID, HN_START, HN_END, s.NAME, COORDINATES " +
        "FROM street_seg ss " +
        "  LEFT JOIN street_zip sz ON sz.ID = ss.STREET_ZIP_ID " +
        "  LEFT JOIN street s on s.ID = sz.STREET_ID " +
        "  LEFT JOIN street_seg_koo_group sskg ON ss.ID = sskg.STREET_SEG_ID " +
        "WHERE COORDINATES IS NOT NULL";

      var reader = MySqlHelper.ExecuteReader(connectionString, getEntry);
      while (reader.Read())
      {
        _entries.Add(new Entry
        {
          id = reader.GetInt32("ID"),
          hnStart = reader.GetString("HN_START"),
          hnEnd = reader.GetString("HN_END"),
          name = reader.GetString("NAME"),
          coordinates = reader.GetString("COORDINATES")
        });
      }
    }

    private Random _rand = new Random();
    private Entry _currentEntry;
    private void NextBtn_OnClick_OnClick(object sender, RoutedEventArgs e)
    {
      do
      {
        _currentEntry = _entries[_rand.Next(_entries.Count)];
      } while (_currentEntry.coordinates.Split('@').Length < 10);

      // _currentEntry = _entries.Find(entry => entry.id == 2994);

      StreetLabel.Content = _currentEntry.id + ":" + _currentEntry.name + " " + _currentEntry.hnStart + " - " + _currentEntry.hnEnd;

      FillMap();
    }

    private void ReloadBtn_OnClick(object sender, RoutedEventArgs e)
    {
      FillMap();
    }

    private void FillMap()
    {
      var newLocations = new List<Location>();
      var coordinates = _currentEntry.coordinates.Split('@');
      foreach (var coordinate in coordinates)
      {
        var latLng = coordinate.Split(',');

        Debug.Assert(latLng.Length == 2);

        var lat = float.Parse(latLng[0].Replace('.', ','));
        var lng = float.Parse(latLng[1].Replace('.', ','));

        newLocations.Add(new Location(lat, lng));
      }

      Map.Children.Clear();

      int ppNumber = 0;
      foreach (var location in newLocations)
      {
        var pp = new Pushpin {Location = location, Content = ppNumber++.ToString()};
        Map.Children.Add(pp);
      }

      var locationRect = new LocationRect(newLocations);
      Map.SetView(locationRect);
    }
  }
}
