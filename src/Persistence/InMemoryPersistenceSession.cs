namespace Softweyr.EventStore.Persistence.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class InMemoryPersistenceSession : IPersistenceSession
    {
        private readonly Dictionary<Guid, EventStream> eventStreams;

        private readonly Dictionary<Guid, InMemoryPersistenceMethod.AggregateSnapshot> snapshots;

        public InMemoryPersistenceSession(Dictionary<Guid, EventStream> eventStreams, Dictionary<Guid, InMemoryPersistenceMethod.AggregateSnapshot> snapshots)
        {
            this.eventStreams = eventStreams;
            this.snapshots = snapshots;
        }

        public EventStream GetById(Guid id)
        {
            if (this.eventStreams.ContainsKey(id))
            {
                return this.eventStreams[id];
            }

            return new EventStream(id, 0, 0, new List<object>());
        }

        public void Save(EventStream eventStream)
        {
            /*
            if (this.eventStreams.ContainsKey(id))
            {
                this.eventStreams[id] = new EventStream(id, expectedVersion, this.eventStreams[id].Events.Union(events).ToArray());
                return;
            }

            this.eventStreams.Add(id, new EventStream(id, expectedVersion, events)); */
        }

        public IEnumerable<Guid> GetAllIds()
        {
            return this.eventStreams.Keys.ToArray();
        }

        public void Snapshot(Guid id, int version, object snapshotEvent)
        {
            // TODO: Need to refactor to fit this scenario.
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            // Do nothing
        }
    }
}