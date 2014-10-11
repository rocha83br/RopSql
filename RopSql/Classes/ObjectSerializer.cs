using System;
using System.Linq;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections.Generic;
using System.Collections;

namespace System.Data.RopSql
{
    public class ObjectSerializer
    {
        #region Public Methods

		public static string SerializeText(object sourceObject)
        {
            MemoryStream memStream = new MemoryStream();
            XmlSerializer xmlSerializer = new XmlSerializer(sourceObject.GetType());

            xmlSerializer.Serialize(memStream, sourceObject);

            memStream.Flush();
            memStream.Seek(0, SeekOrigin.Begin);

            return Convert.ToBase64String(memStream.ToArray());
        }

        public static object DeserializeText(string serialObject, Type objectType)
        {
            object result = null;

            if (!string.IsNullOrEmpty(serialObject) && (objectType != null))
            {
                XmlSerializer xmlSerializer = new XmlSerializer(objectType);
                MemoryStream memStream = new MemoryStream(Convert.FromBase64String(serialObject));

                result = xmlSerializer.Deserialize(memStream);
            }

            return result;
        }
		
		public static byte[] SerializeBinary(object sourceObject)
        {
            BinaryFormatter binSerializer = new BinaryFormatter();
            MemoryStream memStream = new MemoryStream();

            binSerializer.Serialize(memStream, sourceObject);

            memStream.Flush();
            memStream.Seek(0, SeekOrigin.Begin);

            return memStream.ToArray();
        }

        public static object DeserializeBinary(byte[] serialObject)
        {
            object result = null;

            if (serialObject != null)
            {
                BinaryFormatter binSerializer = new BinaryFormatter();
                MemoryStream memStream = new MemoryStream(serialObject);

                result = binSerializer.Deserialize(memStream);
            }

            return result;
        }
		
        #endregion
	}
}
