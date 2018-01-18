using RDFMatcher_NetCore.DBHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace RDFMatcher_NetCore
{
  class InsertBuffer
  {
    // TODO: Remove global variable access
    private readonly int _maxInsertBufferSize = DB.InsertBufferSize;
    
    private readonly List<IInsertBufferItem> _insertBuffer = new List<IInsertBufferItem>();

    public void Insert(IInsertBufferItem item)
    {
      _insertBuffer.Add(item);

      if (_insertBuffer.Count > _maxInsertBufferSize)
      {
        Flush();
      }
    }

    public void Flush()
    {
      foreach (var insertItem in _insertBuffer)
      {
        insertItem.Insert();
      }
      _insertBuffer.Clear();
    }
  }
}
