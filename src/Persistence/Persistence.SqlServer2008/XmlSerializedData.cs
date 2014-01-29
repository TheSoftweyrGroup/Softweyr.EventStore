namespace Softweyr.EventStore.Serialization.XmlDataContract
{
    using System;
    using System.Xml;

    using Softweyr.EventStore.Persistence.SqlServer2008;

    public class XmlSerializedData : SerializedData
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
        public XmlSerializedData(Guid typeId, XmlReader reader)
            : base(typeId)
        {
            this.Reader = reader;
        }

        public XmlReader Reader { get; private set; }

        public override void Dispose()
        {
            this.Reader.Close();
        }
    }
}