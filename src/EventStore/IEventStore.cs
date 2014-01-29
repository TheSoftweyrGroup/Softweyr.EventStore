namespace Softweyr.EventStore
{
    using System;
    using System.Collections.Generic;

    public interface IEventStore
    {
        EventStoreSession NewSession();
        EventStream GetById(Guid id);
        IEnumerable<EventStream> GetAll();
    }
}