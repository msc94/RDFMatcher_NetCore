using System;
using System.Collections.Generic;
using System.Text;

namespace RDFMatcher_NetCore.Countries
{
  class NZ
  {
    public static string ReplaceStreetType(string streetType)
    {
      switch (streetType)
      {
        case "ROAD":
          return "Rd";
        case "AVENUE":
          return "Ave";
        case "STREET":
          return "St";
        case "LANE":
          return "Ln";
        case "DRIVE":
          return "Dr";
        case "CRESCENT":
          return "Cres";
        case "GROVE":
          return "Grv";
        case "PLACE":
          return "Pl";

        case "HEIGHTS":
          return "Hts";
        case "PARADE":
          return "Pde";
        case "COURT":
          return "Cor";
        case "CLOSE":
          return "Clos";
        case "TERRACE":
          return "Ter";
        case "HIGHWAY":
          return "Hwy";
        case "VALLEY":
          return "Vale";

        case "SQUARE":
          return "Sq";
        case "STRAND":
          return "Sta";
        case "MOUNT":
          return "Mt";
        case "GREEN":
          return "Grn";
        case "VIEW":
          return "Vw";
        case "POINT":
          return "Pt";
        case "ESPLANADA":
          return "Espl";
        case "GARDENS":
          return "Gdns";
        case "BOULEVARD":
          return "Blvd";
      }

      return streetType;
    }
  }
}
