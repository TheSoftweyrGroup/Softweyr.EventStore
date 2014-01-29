namespace Softweyr.EventStore.Persistence.SqlServer2008
{
    using System;

    public abstract class SerializedData : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SerializedData"/> class.
        /// </summary>
        /// <param name="typeId">
        /// The type id.
        /// </param>
        protected SerializedData(Guid typeId)
        {
            TypeId = typeId;
        }

        public Guid TypeId { get; private set; }

        public abstract void Dispose();
    }
}