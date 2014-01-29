namespace Softweyr.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class EventStream
    {
        public Guid Id { get; private set; }

        public int SnapshotVersion { get; private set; }

        public int CommittedVersion { get; private set; }
        
        public int UncommittedVersion { get; private set; }

        public IEnumerable<object> Events
        {
            get
            {
                lock (this.innerEventsLock)
                {
                    return this.innerEvents.ToArray();
                }
            }
        }

        private readonly HashSet<object> eventsSet;

        private readonly object innerEventsLock = new object();

        private readonly List<object> innerEvents;

        public EventStream(Guid id, int committedVersion, int snapshotVersion, IEnumerable<object> events)
        {
            this.Id = id;
            this.CommittedVersion = committedVersion;
            this.SnapshotVersion = snapshotVersion;
            this.UncommittedVersion = committedVersion;
            this.innerEvents = @events.ToList();
            this.eventsSet = new HashSet<object>(@events);
        }

        internal void AddEvent(object @event)
        {
            lock (this.innerEventsLock)
            {
                // Use the event set to check for dups as it is faster than the list .Contains.
                if (this.eventsSet.Contains(@event))
                {
                    throw new Exception("Can't add the same event twice.");
                }

                this.innerEvents.Add(@event);
                this.eventsSet.Add(@event);
                this.UncommittedVersion++;
            }
        }

        internal void AddSnapshot(object snapshot)
        {
            lock (this.innerEventsLock)
            {
                // Use the event set to check for dups as it is faster than the list .Contains.
                if (this.eventsSet.Contains(snapshot))
                {
                    throw new Exception("Can't add the same event twice.");
                }

                this.innerEvents.Add(snapshot);
                this.eventsSet.Add(snapshot);
                this.UncommittedVersion++;
                this.SnapshotVersion = this.UncommittedVersion;
            }
        }
    }
}