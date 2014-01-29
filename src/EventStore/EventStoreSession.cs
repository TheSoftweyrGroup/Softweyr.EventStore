namespace Softweyr.EventStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class EventStoreSession : IDisposable
    {
        private readonly IPersistenceSession persistenceSession;

        private readonly IHyrdationMethod hydrationMethod;

        private bool complete;

        private readonly object eventStreamsLock = new object();

        private readonly Dictionary<Guid, EventStream> eventStreams = new Dictionary<Guid, EventStream>();

        private readonly EventStore eventStore;

        private bool disposing = false;

        public ExpandoObject Context { get; private set; }

        public EventStoreSession(EventStore eventStore, IPersistenceSession persistenceSession)
        {
            this.Context = new ExpandoObject();
            this.eventStore = eventStore;
            this.persistenceSession = persistenceSession;
        }

        public void Complete()
        {
            this.complete = true;
        }

        public void AddEvent(Guid eventStreamId, object @event)
        {
            if (disposing)
            {
                throw new ObjectDisposedException("Cannot AddEvent after session has been disposed.");
            }

            var eventStream = this.GetById(eventStreamId);
            eventStream.AddEvent(@event);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            this.disposing = true;
            if (this.complete)
            {
                using (
                    var ts = new TransactionScope(
                        TransactionScopeOption.Required,
                        new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }))
                {
                    foreach (var value in this.eventStreams.Values)
                    {
                        // Persist the event stream.
                        this.persistenceSession.Save(value);
                    }

                    ts.Complete();
                }
            }

            this.persistenceSession.Dispose();
            this.eventStore.CurrentSession = null;
        }

        public EventStream GetById(Guid id)
        {
            if (disposing)
            {
                throw new ObjectDisposedException("Cannot GetById after session has been disposed.");
            }

            lock (this.eventStreams)
            {
                if (!this.eventStreams.ContainsKey(id))
                {
                    this.eventStreams.Add(id, this.eventStore.GetByIdFromPersistenceStore(id, this));
                }
            }

            return this.eventStreams[id];
        }

        public void AddSnapshot(Guid eventStreamId, object snapshot)
        {
            if (disposing)
            {
                throw new ObjectDisposedException("Cannot AddEvent after session has been disposed.");
            }

            var eventStream = this.GetById(eventStreamId);
            eventStream.AddSnapshot(snapshot);
        }
    }
}