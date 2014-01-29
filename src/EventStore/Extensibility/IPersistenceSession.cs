namespace Softweyr.EventStore
{
    using System;
    using System.Collections.Generic;

    public interface IPersistenceSession : IDisposable
    {
        void Save(EventStream value);
        IEnumerable<Guid> GetAllIds();
        EventStream GetById(Guid id);
    }
}