namespace Softweyr.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    public class EventStore : IEventStore
    {
        private readonly IPersistenceMethod persistenceMethod;

        public EventStoreSession CurrentSession
        {
            get
            {
                return this.currentStream.Value;
            }

            set
            {
                this.currentStream.Value = value;
            }
        }

        private ThreadLocal<EventStoreSession> currentStream = new ThreadLocal<EventStoreSession>();

        public EventStore(IPersistenceMethod persistenceMethod)
        {
            this.persistenceMethod = persistenceMethod;
        }

        public EventStoreSession NewSession()
        {
            var newStream = new EventStoreSession(this, this.persistenceMethod.NewSession());
            this.CurrentSession = newStream;
            return newStream;
        }

        public EventStream GetById(Guid id)
        {
            if (this.CurrentSession != null)
            {
                return this.CurrentSession.GetById(id);
            }

            return this.persistenceMethod.GetById(id);
        }

        public IEnumerable<EventStream> GetAll()
        {
            return this.persistenceMethod.GetAllIds().Select(eventStreamId => this.persistenceMethod.GetById(eventStreamId));
        }

        internal EventStream GetByIdFromPersistenceStore(Guid id, EventStoreSession session)
        {
            return this.persistenceMethod.GetById(id);
        }
    }
}