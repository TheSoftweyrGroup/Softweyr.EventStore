namespace Softweyr.EventStore.Serialization.BinaryFormatter
{
    using System;
    using System.IO;

    using Softweyr.EventStore.Persistence.SqlServer2008;

    public class BinarySerializedData : SerializedData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SerializedData"/> class.
        /// </summary>
        /// <param name="typeId">
        /// The type id.
        /// </param>
        /// <param name="stream">
        /// The stream.
        /// </param>
        public BinarySerializedData(Guid typeId, Stream stream)
            : base(typeId)
        {
            this.Stream = stream;
        }

        public Stream Stream { get; private set; }

        public override void Dispose()
        {
            this.Stream.Close();
            this.Stream.Dispose();
        }
    }
}