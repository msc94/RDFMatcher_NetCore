using System;
using System.Collections.Generic;
using RDFMatcher_NetCore.DBHelper;

namespace RDFMatcher_NetCore
{
  class MatchInsertBuffer : InsertBuffer<MatchedAddressItem>
  {
    private Database _db;

    public MatchInsertBuffer(Database db)
    {
      _db = db;
    }

    public override void FlushItems(IEnumerable<MatchedAddressItem> items)
    {
      _db.InsertMatchedItems(items);
    }
  }
}

