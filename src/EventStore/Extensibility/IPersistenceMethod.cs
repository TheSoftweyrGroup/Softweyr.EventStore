namespace Softweyr.EventStore
{
    using System;
    using System.Collections.Generic;

    public interface IPersistenceMethod : IDisposable
    {
        IPersistenceSession NewSession();
        // void Snapshot(Guid id, int version, object snapshotEvent);
        IEnumerable<Guid> GetAllIds();
        EventStream GetById(Guid id);
    }
}