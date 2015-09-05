
using System.Collections;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace uPLibrary.Networking.M2Mqtt.Persistence
{
    public interface IMqttClientPersistence
    {
        void Add(string key, MqttMsgContext ctx);
        void Remove(string key);
        ICollection GetAll();
        void Clear();
        bool Contains(string key);
    }
}
