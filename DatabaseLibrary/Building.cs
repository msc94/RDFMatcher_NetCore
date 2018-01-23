using System.Data;

namespace DatabaseLibrary
{
  class Building
  {
    public StreetZip StreetZip;

    public string HouseNumber;
    public string HouseNumberExtension;

    public string ForeignKey;

    void ReadFromRecord(IDataRecord record)
    {

    }
  }
}
