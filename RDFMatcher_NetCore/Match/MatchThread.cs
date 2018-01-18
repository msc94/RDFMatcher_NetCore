using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFMatcher_NetCore
{
  class MatchProgress
  {
    public int matched = 0;
    public int done = 0;
  }

  class MatchItem
  {
    public object street_zip_id;
    public string zip;
    public string street_name;
  }

  class InsertItem : IInsertBufferItem
  {
    public object street_zip_id;
    public string hno;
    public string hno_extension;
    public object ap_lat;
    public object ap_lng;

    public void Insert()
    {

    }
  }

  class MatchThread
  {
    private const string queryCommandText = "SELECT addr.ROAD_LINK_ID, pt.ADDRESS, pt.LAT, pt.LNG " +
                                            "FROM POL_RDF_ADDR addr " +
                                            " LEFT JOIN POL_RDF_POINT pt using (ROAD_LINK_ID) " +
                                            "WHERE addr.LEFT_POSTAL_CODE = @1 AND addr.STREET_BASE_NAME = @2 " +
                                            " AND pt.LAT IS NOT NULL AND pt.LNG IS NOT NULL ";

    private const string insertCommandText = "INSERT INTO building (STREET_ZIP_ID, HNO, HNO_EXTENSION, AP_LAT, AP_LNG) VALUES(@1, @2, @3, @4, @5)";

    private readonly MatchProgress _matchProgress;
    private readonly BlockingCollection<MatchItem> _workQueue;


    private readonly MySqlConnection _addrQueryConnection;
    private readonly MySqlConnection _insertConnection;

    private readonly MySqlCommand _addrQueryCommand;
    private readonly MySqlCommand _insertCommand;

    public MatchThread(MatchProgress matchProgress, BlockingCollection<MatchItem> workQueue)
    {
      _matchProgress = matchProgress;
      _workQueue = workQueue;

      _addrQueryConnection = new MySqlConnection(DB.ConnectionString);
      _addrQueryConnection.Open();

      _addrQueryCommand = _addrQueryConnection.CreateCommand();
      _addrQueryCommand.CommandText = queryCommandText;
      _addrQueryCommand.Prepare();

      _addrQueryCommand.Parameters.AddWithValue("@1", null);
      _addrQueryCommand.Parameters.AddWithValue("@2", null);

      _insertConnection = new MySqlConnection(DB.ConnectionString);
      _insertConnection.Open();

      _insertCommand = _insertConnection.CreateCommand();
      _insertCommand.CommandText = insertCommandText;
      _insertCommand.Prepare();

      _insertCommand.Parameters.AddWithValue("@1", null);
      _insertCommand.Parameters.AddWithValue("@2", null);
      _insertCommand.Parameters.AddWithValue("@3", null);
      _insertCommand.Parameters.AddWithValue("@4", null);
      _insertCommand.Parameters.AddWithValue("@5", null);

      new Thread(workLoop).Start();
    }

    private void workLoop()
    {
      while (!_workQueue.IsCompleted)
      {
        MatchItem item = null;
        try
        {
          item = _workQueue.Take();
        }
        catch (InvalidOperationException) { }

        if (item != null)
        {
          Match(item);
        }
      }
    }

    private void Match(MatchItem item)
    {
      var zip = item.zip;
      var street_name = item.street_name;

      _addrQueryCommand.Parameters[0].Value = zip;
      _addrQueryCommand.Parameters[1].Value = street_name;
      var addrReader = _addrQueryCommand.ExecuteReader();

      var numMatches = 0;

      while (addrReader.Read())
      {
        numMatches++;

        //MySqlHelper.ExecuteNonQuery(DB.ConnectionString,
        //  $"INSERT INTO MATCH_TEST VALUES({addrReader.GetInt32("ROAD_LINK_ID")}, {item.street_zip_id})");
        //split_address(addrReader.GetString("ADDRESS"), out var hno, out var hno_extension);
        //var newItem = new InsertItem
        //{
        //  street_zip_id = item.street_zip_id,
        //  hno = hno,
        //  hno_extension = hno_extension,
        //  ap_lat = addrReader.GetString("LAT").Insert(2, "."),
        //  ap_lng = addrReader.GetString("LNG").Insert(2, ".")
        //};
        //_insertBuffer.Add(newItem);
      }

      addrReader.Close();

      if (numMatches > 0)
      {
        Interlocked.Increment(ref _matchProgress.matched);
      }

      if (numMatches == 0)
      {
        // Console.WriteLine($"No match for {item.street_name}, {item.zip}");
      }


      Interlocked.Increment(ref _matchProgress.done);
    }

    private void SplitAddress(string address, out string hno, out string hno_extension)
    {
      for (int i = 0; i < address.Length; i++)
      {
        if (char.IsLetter(address[i])
          || address[i] == '/'
          || address[i] == '?')
        {
          hno = address.Substring(0, i);
          hno_extension = address.Substring(i);
          return;
        }
      }
      hno = address;
      hno_extension = "";
    }
  }
}

