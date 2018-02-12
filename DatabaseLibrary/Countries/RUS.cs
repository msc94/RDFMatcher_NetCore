using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseLibrary.Countries
{
  public class RUS
  {
    public static string ReplaceStreetType(string streetType)
    {
      switch (streetType)
      {
        case "ул":
          return "улица";
        case "мкр":
          return "";
        case "пер":
          return "переулок";
        case "пр-кт":
          return "проспект";
      }
      return streetType;
    }
  }
}
