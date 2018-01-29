using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataValidator
{
  class Utils
  {
    public static double ParseDoubleInvariantCulture(string s)
    {
      return double.Parse(s, CultureInfo.InvariantCulture);
    }
  }
}
