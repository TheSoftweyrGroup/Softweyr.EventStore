namespace Softweyr.EventStore.Persistence.InMemory
{
    using System;
    using System.Collections.Generic;

    public class InMemoryPersistenceMethod : IPersistenceMethod
    {
        private readonly Dictionary<Guid, EventStream> eventStreams = new Dictionary<Guid, EventStream>();

        private readonly Dictionary<Guid, AggregateSnapshot> snapshots = new Dictionary<Guid, AggregateSnapshot>();

        private IPersistenceSession currentSession = null; 

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            // Do nothing.
        }

        public IPersistenceSession NewSession()
        {
            this.currentSession = new InMemoryPersistenceSession(this.eventStreams, this.snapshots);
            return this.currentSession;
        }

        public EventStream GetById(Guid id)
        {
            if (this.currentSession != null)
            {
                return this.currentSession.GetById(id);
            }

            using (var session = new InMemoryPersistenceSession(this.eventStreams, this.snapshots))
            {
                return session.GetById(id);
            }
        }

        public void Save(EventStream eventStream)
        {
            if (this.currentSession == null)
            {
                throw new NotSupportedException();
            }

            this.currentSession.Save(eventStream);
        }

        /*
        public void Snapshot(Guid id, int version, object snapshotEvent)
        {
            this.snapshots.Add(id, new InMemoryPersistenceMethod.AggregateSnapshot(id, version, snapshotEvent));
            this.eventStreams.Remove(id);
            this.eventStreams.Add(id, new EventStream(id, version, new List<object> { this.snapshots[id].SnapshotEvent }));
        } */

        public IEnumerable<Guid> GetAllIds()
        {
            if (this.currentSession != null)
            {
                return this.currentSession.GetAllIds();
            }

            using (var session = new InMemoryPersistenceSession(this.eventStreams, this.snapshots))
            {
                return session.GetAllIds();
            }
        }

        public class AggregateSnapshot
        {
            public Guid Id { get; private set; }

            public int Version { get; private set; }

            public object SnapshotEvent { get; private set; }

            public AggregateSnapshot(Guid id, int version, object snapshotEvent)
            {
                Id = id;
                Version = version;
                SnapshotEvent = snapshotEvent;
            }
        }
    }
}