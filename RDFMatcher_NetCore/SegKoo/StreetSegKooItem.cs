namespace RDFMatcher_NetCore
{
  class StreetSegKooItem : IWorkerThreadItem
  {
    public object segId;
    public int hnStart;
    public int hnEnd;
    public int scheme;

    public void Work()
    {
      throw new System.NotImplementedException();
    }
  }
}