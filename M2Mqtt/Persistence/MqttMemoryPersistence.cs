using System.Collections;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace uPLibrary.Networking.M2Mqtt.Persistence
{
    public class MqttMemoryPersistence : IMqttClientPersistence
    {
        private Hashtable inflightMessages;

        public MqttMemoryPersistence()
        {
            inflightMessages = new Hashtable();
        }
        public void Add(string key, MqttMsgContext ctx)
        {
            inflightMessages.Add(key, ctx);
        }

        public  void Remove(string key)
        {
            inflightMessages.Remove(key);
        }

        public ICollection GetAll()
        {
            return inflightMessages.Values;
        }

        public void Clear()
        {
            throw new System.NotImplementedException();
        }
    }
}
