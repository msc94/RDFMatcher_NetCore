using System.Windows.Controls;
using Microsoft.Maps.MapControl.WPF;

namespace DataValidator
{
  interface IRunMode
  {
    void FillMap(Map map, Label streetLabel);
    void LoadEntries();
    void NextEntry();
  }
}