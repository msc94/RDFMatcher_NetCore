using System;

namespace RDFMatcher_NetCore
{
  class Segment : IEquatable<Segment>
  {
    public float LAT;
    public float LON;

    public bool Equals(Segment other)
    {
      const float epsilonDistance = 0.00001f;
      return Math.Abs(LAT - other.LAT) < epsilonDistance &&
             Math.Abs(LON - other.LON) < epsilonDistance;
    }
  }
}