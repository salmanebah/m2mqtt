
using System;
using System.Collections;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Text;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace uPLibrary.Networking.M2Mqtt.Persistence
{
    public class MqttDatabasePersistence : IMqttClientPersistence
    {
        private string connectionString;

        private static string createStm = @"CREATE TABLE IF NOT EXISTS inflightMessages (
		       key TEXT PRIMARY KEY,
			   state INTEGER check (state >= 1 and state <= 6),
			   flow INTEGER check (flow = 0 or flow = 1),
			   attempt INTEGER ,
			   timestamp INTEGER,
		       type INTEGER check (type >= 3 and type <= 7),
		       dupflag INTEGER check (dupflag = 0 or dupflag = 1),
		       qoslevel INTEGER check (qoslevel >= 0 and qoslevel <= 2),
		       retain INTEGER check (retain = 0 or retain = 1),
		       messageid INTEGER,
		       message BLOB,
		       topic TEXT)";

        private static string insertStm =
            "INSERT INTO inflightMessages(key, state, flow, attempt, timestamp, type, dupflag, qoslevel, retain,messageid, message, topic) " +
            "VALUES (@key, @state, @flow, @attempt, @timestamp, @type, @dupflag, @qoslevel, @retain, @messageid, @message, @topic)";

        private static string delKeyStm =
            "DELETE FROM inflightMessages WHERE key = @key";

        private static string delAllStm =
            "DELETE FROM inflightMessage";

        private static string containStm =
            "SELECT count(*) FROM inflightMessages WHERE key = @key";

        private static string getAllStm =
            "SELECT * FROM InflightMessages";

        public MqttDatabasePersistence(string saveDirectory, string clientId, string brokerHostName)
        {
            connectionString = "URI=file:" + saveDirectory + Path.DirectorySeparatorChar + brokerHostName + clientId + ".db";
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = createStm;
                    cmd.ExecuteNonQuery();
                }
                conn.Close();

            }
        }

        public void Add(string key, MqttMsgContext ctx)
        {
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = insertStm;
                    cmd.Parameters.AddWithValue("@key", key);
                    cmd.Parameters.AddWithValue("@state", (int)ctx.State);
                    cmd.Parameters.AddWithValue("@flow", (int)ctx.Flow);
                    cmd.Parameters.AddWithValue("@attempt", ctx.Attempt);
                    cmd.Parameters.AddWithValue("@timestamp", ctx.Timestamp);
                    cmd.Parameters.AddWithValue("@type", (int)ctx.Message.Type);
                    cmd.Parameters.AddWithValue("@dupflag", Convert.ToInt32(ctx.Message.DupFlag));
                    cmd.Parameters.AddWithValue("@qoslevel", (int)ctx.Message.QosLevel);
                    cmd.Parameters.AddWithValue("@retain", Convert.ToInt32(ctx.Message.Retain));
                    cmd.Parameters.AddWithValue("@messageid", ctx.Message.MessageId);
                    // if the message is of type publish, save the message content and the topic else save n/a 
                    byte[] msg = (ctx.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                                   ? ((MqttMsgPublish) ctx.Message).Message
                                   : Encoding.UTF8.GetBytes("n/a");
                    cmd.Parameters.AddWithValue("@message", msg);                                        
                    string topic =  (ctx.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                                      ? ((MqttMsgPublish) ctx.Message).Topic
                                      : "n/a";
                    cmd.Parameters.AddWithValue("@topic", topic);

                    cmd.ExecuteNonQuery();
                }
                conn.Close();
            }
        }

        public void Remove(string key)
        {
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = delKeyStm;
                    cmd.Parameters.AddWithValue("@key", key);
                    cmd.ExecuteNonQuery();
                }
                conn.Close();
            }
        }

        public ICollection GetAll()
        {
            IList ctxs = new ArrayList();
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = getAllStm;
                    using (SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            ctxs.Add(RowToContext(reader));
                        }
                    }

                }
                conn.Close();
            }
            return ctxs;
        }

        public void Clear()
        {
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = delAllStm;
                    cmd.ExecuteNonQuery();
                }
                conn.Close();
            }
        }

        public bool Contains(string key)
        {
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                bool haveKey;
                conn.Open();
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = containStm;
                    cmd.Parameters.AddWithValue("@key", key);
                    haveKey = cmd.ExecuteScalar() != null;
                }
                conn.Close();
                return haveKey;
            }
        }

        private static MqttMsgContext RowToContext(IDataRecord row)
        {
            byte type = row.GetByte(5);
            MqttMsgBase message = null;
            switch (type)
            {
                case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:
                    MqttMsgPublish pubmsg = new MqttMsgPublish();
                    RowToMsgBase(pubmsg, row);
                    long len = row.GetBytes(10, 0, null, 0, 1);
                    row.GetBytes(10, 0, pubmsg.Message, 0, (int) len);
                    pubmsg.Topic = row.GetString(11);
                    message = pubmsg;
                    break;

                case MqttMsgBase.MQTT_MSG_PUBREC_TYPE:
                    MqttMsgPubrec pubrecmsg = new MqttMsgPubrec();
                    RowToMsgBase(pubrecmsg, row);
                    message = pubrecmsg;
                    break;

                case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:
                    MqttMsgPubrel pubrelmsg = new MqttMsgPubrel();
                    RowToMsgBase(pubrelmsg, row);
                    message = pubrelmsg;
                    break;

                case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:
                    MqttMsgPubcomp pubcompcmsg = new MqttMsgPubcomp();
                    RowToMsgBase(pubcompcmsg, row);
                    message = pubcompcmsg;
                    break;

                default:
                    // TODO: not suppose to happen, throw more precise exception
                    throw new Exception("Unexpected message type in persistence");
            }
            return new MqttMsgContext
            {
                State = (MqttMsgState) row.GetInt32(1),
                Flow = (MqttMsgFlow) row.GetInt32(2),
                Attempt = row.GetInt32(3),
                Timestamp = row.GetInt32(4),
                Message = message

            };
        }

        private static void RowToMsgBase(MqttMsgBase msg, IDataRecord row)
        {
            msg.Type = row.GetByte(5);
            msg.DupFlag = row.GetBoolean(6);
            msg.QosLevel = row.GetByte(7);
            msg.Retain = row.GetBoolean(8);
            msg.MessageId = (ushort) row.GetInt16(9);
        }
    }
}