
using System;
using System.Collections;
using System.Data;
using System.Data.SQLite;
using System.Text;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace uPLibrary.Networking.M2Mqtt.Persistence
{
    public class MqttDatabasePersistence : IMqttClientPersistence
    {
        private string connectionString;

        private static string createStm = @"CREATE TABLE IF NOT EXISTS inflightMessages (
		       key INTEGER PRIMARY KEY,
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
            connectionString = saveDirectory + @"\" + brokerHostName + clientId + ".db";
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
                    cmd.Parameters["@key"].Value = key;
                    cmd.Parameters["@state"].Value = ctx.State;
                    cmd.Parameters["@flow"].Value = ctx.Flow;
                    cmd.Parameters["@attempt"].Value = ctx.Attempt;
                    cmd.Parameters["@timestamp"].Value = ctx.Timestamp;
                    cmd.Parameters["@type"].Value = ctx.Message.Type;
                    cmd.Parameters["@dupflag"].Value = ctx.Message.DupFlag;
                    cmd.Parameters["@qoslevel"].Value = ctx.Message.QosLevel;
                    cmd.Parameters["@retain"].Value = ctx.Message.Retain;
                    cmd.Parameters["@messageid"].Value = ctx.Message.MessageId;
                    // if the message is of type publish, save the message content and the topic else save n/a 
                    cmd.Parameters["@message"].Value =
                        (ctx.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                            ? ((MqttMsgPublish) ctx.Message).Message
                            : Encoding.UTF8.GetBytes("n/a");
                    cmd.Parameters["@topic"].Value =
                        (ctx.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                            ? ((MqttMsgPublish) ctx.Message).Topic
                            : "n/a";
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
                    cmd.Parameters["@key"].Value = key;
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
                    cmd.Parameters["@key"].Value = key;
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