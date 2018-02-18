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
        case "пл":
          return "площадь";
        case "наб":
          return "набережная";
      }
      return streetType;
    }

    // 4-я Северная
    // 2-я Северная
    // 5-й Институтский
    public static string ReplaceStreetName(string streetName)
    {
      string[] streetNameParts = streetName.Split(' ');
      int numParts = streetNameParts.Length;

      if (numParts < 2)
        return streetName;

      string lastPart = streetNameParts[numParts - 1];
      if (!lastPart.Contains("-я") &&
        !lastPart.Contains("-й"))
        return streetName;

      string[] newParts = new string[numParts];

      newParts[0] = lastPart;
      for (int i = 0; i < numParts - 1; i++)
        newParts[i + 1] = streetNameParts[i];

      return String.Join(" ", newParts);
    }
  }
}
