using RDFMatcher_NetCore.DBHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace RDFMatcher_NetCore
{
  abstract class InsertBuffer<T>
  {
    // TODO: Remove global variable access
    private readonly int _maxInsertBufferSize = DB.InsertBufferSize;

    private readonly List<T> _insertBuffer = new List<T>();

    public void Insert(T item)
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
        Insert(insertItem);
      }
      _insertBuffer.Clear();
    }

    // Implement in subclasses
    public abstract void InsertItem(T item);
  }
}
