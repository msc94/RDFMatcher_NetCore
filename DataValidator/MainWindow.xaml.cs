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

    public static string connectionString;

    private IRunMode _runMode = new ValidateBuilding();

    public MainWindow()
    {
      InitializeComponent();

      bool local = true;
      if (local)
      {
        connectionString =
          "server=localhost;" +
          "uid=root;" +
          "pwd=bloodrayne;" +
          "database=nz;" +
          "connection timeout=1000;" +
          "command timeout=1000;";
      }
      else
      {
        connectionString =
          "server=h2744269.stratoserver.net;" +
          "uid=Marcel;" +
          "pwd=YyQzKeSSX0TlgsI4;" +
          "database=POL;" +
          "connection timeout=1000;" +
          "command timeout=1000;";
      }

    }


    private void LoadBtn_OnClick(object sender, RoutedEventArgs e)
    {
      _runMode.LoadEntries();
    }


    private void NextBtn_OnClick_OnClick(object sender, RoutedEventArgs e)
    {
      _runMode.NextEntry();
      _runMode.FillMap(Map, StreetLabel);
    }

    private void ReloadBtn_OnClick(object sender, RoutedEventArgs e)
    {
      _runMode.FillMap(Map, StreetLabel);
    }

    private void RunmodeCb_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
      var selectedValue = RunmodeCb.SelectedValue;
      if (selectedValue == null)
        return;

      var selectedValueString = selectedValue.ToString();
      if (selectedValueString == "Building")
      {
        _runMode = new ValidateBuilding();
      }
      else if (selectedValueString == "StreetSeg")
      {
        _runMode = new ValidateStreetSeg();
      }
    }
  }
}
