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
            inflightMessages.Clear();
        }

        public bool Contains(string key)
        {
#if (MF_FRAMEWORK_VERSION_V4_2 || MF_FRAMEWORK_VERSION_V4_3 || COMPACT_FRAMEWORK)
            return inflightMessages.Contains(key);
#else
            return inflightMessages.ContainsKey(key);
#endif
        }
    }
}
