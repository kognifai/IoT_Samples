using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using System.IO;
using System.Diagnostics;
using Kognifai.Serialization;
using log4net;
using log4net.Config;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using Kognifai.Mqtt;
#if NET_FRAMEWORK40
#else
using Google.Protobuf;
#endif

namespace M2MqttExampleClient.Net40
{
    public class Client
    {
        private static readonly ILog SysLog = LogManager.GetLogger(typeof(Client));
        private static MqttClient client;
        private static bool clientExit = false;
        //private static MqttClientTcpOptions options;
        private static uint state = 0;
        private static bool alarmOnOff = false;
        private static bool toggleCosSin = false;
        private static string clientId;
        private static string server;
        private static bool timeSeriesOnOff = false;
        private static Thread timeSeriesThread;
        public static void Main(string[] args)
        {
            string externalId = "MqttTestTimeSeries01";
            int numberOfPackets = 360;

            if (args.Length < 2)
            {
                Console.WriteLine("Usage: <clientId> <server>");
                return;
            }

            if (args.Length == 3 || args.Length == 4)
            {
                if (!int.TryParse(args[2], out numberOfPackets))
                {
                    Console.WriteLine("Invalid argument numberOfPackets: " + args[2]);
                    Console.WriteLine("Usage: <clientId> <server> [numberOfPackets] [externalId]");
                    return;
                }
            }

            if (args.Length == 4)
            {
                externalId = args[3];
            }

            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.ConfigureAndWatch(logRepository, new FileInfo("log4net.config"));
            clientId = args[0];
            server = args[1];
            //uPLibrary.Networking.M2Mqtt.Utility.Trace.TraceLevel = uPLibrary.Networking.M2Mqtt.Utility.TraceLevel.Queuing;
            //uPLibrary.Networking.M2Mqtt.Utility.Trace.TraceListener = (f, a) => SysLog.Info(System.String.Format(f, a));
            RunClientAsync(clientId,server,numberOfPackets,externalId);
        }

        private static void ClientTraceListener(string format, params object[] args)
        {
        }


        private static Task Delay(int milliseconds)        // Asynchronous NON-BLOCKING method
        {
            var tcs = new TaskCompletionSource<object>();
            new Timer(_ => tcs.SetResult(null)).Change(milliseconds, -1);
            return tcs.Task;
        }

        private static void ConnectionClosed(object sender, EventArgs e)
        {
            Console.WriteLine("### Disconnected from server ###");
            if (clientExit)
                return;

            Console.WriteLine("### Reconnecting to the server ###");
            do
            {
                var task = Delay(5000);
                task.Wait();
                try
                {
                    client.Connect(clientId);
                    Console.WriteLine("### Connected to the server ###");
                }
                catch
                {
                    Console.WriteLine("### Reconnecting failed, retrying in 5s ###");
                }
            } while (!client.IsConnected && !clientExit);
        }
        private static void RunClientAsync(string clientId, string server, double numberOfPackets, string externalId)
        {

            /* create the client instance */
            client = new MqttClient(server);

            /* register eventhandler for message received event */
            client.MqttMsgPublishReceived += ApplicationMessageReceived;

            client.ConnectionClosed += ConnectionClosed;

            /* Add an eventhandler for trace events from MQTT core */
            /*
            MqttNetTrace.TraceMessagePublished += (s, e) =>
            {
                TraceMessagePublished(e);
            };*/
            Console.WriteLine("### Connecting to the server ###");
            do
            {
                try
                {
                    client.Connect(clientId,"","",false,60);
                }
                catch (Exception)
                {
                    Console.WriteLine("### Connecting to the server failed retrying in 5s ###");
                }
                if (!client.IsConnected)
                {
                    var task = Delay(5000);
                    task.Wait();
                }
            } while (!client.IsConnected);

            while (true)
            {
                try
                {
                    /* now send some data */
                    //MqttApplicationMessage message = null;
                    Console.WriteLine("MQTT Client ready");
                    Console.WriteLine("1 = Send time series messages");
                    Console.WriteLine("2 = send alarm messages");
                    Console.WriteLine("3 = toggle alarm on/off");
                    Console.WriteLine("4 = send a state changed message");
                    Console.WriteLine("5 = send a sample set message");
                    Console.WriteLine("6 = toggle time series on/off");
                    Console.WriteLine("7 = send a container");
                    Console.WriteLine("Q = quit");
                    var pressedKey = Console.ReadKey(true);
                    if (pressedKey.Key == ConsoleKey.D1)
                    {
                        double i;
                        for (i = 0.0; i < 14400.0; i++)
                        {
                            DateTimeOffset time = DateTimeOffset.Now.AddYears(0);
                            
                            TimeseriesDoublesReplicationMessage tds = new TimeseriesDoublesReplicationMessage("MqttTestTimeSeries01", time, Math.Sin(Math.PI / 180 * i));
                            var messageWrpper = tds.ToMessageWrapper();
                            client.Publish(Topics.CloudBound, messageWrpper.ToByteArray(), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE,false);
                            //var task = Delay(12);
                            //task.Wait();
                            Console.Write("#");
                        }
                    }
                    if (pressedKey.Key == ConsoleKey.D2)
                    {
                        var alarm = new AlarmReplicationMessage
                        {
                            ExternalId = "MqttTestAlarm1"
                        };

                        AlarmEvent aEv = new AlarmEvent(
                            DateTimeOffset.Now.AddSeconds(-3),
                            AlarmLevelType.InfoLevel,
                            AlarmStateType.NormalState,
                            "Info level, Normal state");
                        alarm.AlarmEvents.Add(aEv);

                        aEv = new AlarmEvent(
                            DateTimeOffset.Now.AddSeconds(-2),
                            AlarmLevelType.WarningLevel,
                            AlarmStateType.NormalState,
                            "Warning level, Normal state");
                        alarm.AlarmEvents.Add(aEv);

                        aEv = new AlarmEvent(
                            DateTimeOffset.Now.AddSeconds(-1),
                            AlarmLevelType.AlarmLevel,
                            AlarmStateType.NormalState,
                            "Alarm level, Normal state");
                        alarm.AlarmEvents.Add(aEv);

                        aEv = new AlarmEvent(
                            DateTime.Now,   
                            AlarmLevelType.EmergencyLevel,
                            AlarmStateType.NormalState,
                            "Emergency level, Normal state");
                        alarm.AlarmEvents.Add(aEv);
                        var messageWrapper = alarm.ToMessageWrapper();
                        client.Publish(Topics.CloudBound, messageWrapper.ToByteArray(), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, false);
                    }
                    if (pressedKey.Key == ConsoleKey.D3)
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            Kognifai.Serialization.AlarmStateType alarmState;
                            alarmOnOff = !alarmOnOff;

                            if (alarmOnOff)
                                alarmState = AlarmStateType.AlarmState;
                            else
                                alarmState = AlarmStateType.NormalState;

                            var alarm = new AlarmReplicationMessage
                            {
                                ExternalId = "MqttTestAlarm01"
                            };
                            AlarmEvent aEv = new AlarmEvent(
                                DateTime.UtcNow,
                                AlarmLevelType.EmergencyLevel,
                                alarmState,
                                "Info level, Normal state");
                            alarm.AlarmEvents.Add(aEv);
                            var messageWrapper = alarm.ToMessageWrapper();
                            client.Publish(Topics.CloudBound, messageWrapper.ToByteArray(), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, false);
                            var delay = Delay(10);
                            delay.Wait();
                        }
                    }
                    if (pressedKey.Key == ConsoleKey.D4)
                    {
                        //for (int i = 0; i < 100; i++)
                        {
                            if (state == 5)
                                state = 0;
                            state++;
                            var stateChanged = new StateChangeReplicationMessage
                            {
                                ExternalId = "MqttTestStateChangeEvent01",
                            };
                            var sEv = new StateChange(
                                DateTime.UtcNow,
                                "Donald Duck",
                                true,
                                state);
                            stateChanged.StateChanges.Add(sEv);
                            var messageWrapper = stateChanged.ToMessageWrapper();
                            client.Publish(Topics.CloudBound, messageWrapper.ToByteArray(), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, false);
                            var delay = Delay(10);
                            delay.Wait();
                        }
                    }
                    if (pressedKey.Key == ConsoleKey.D5)
                    {
                        toggleCosSin = !toggleCosSin;
                        /* create a sample set */
                        List<double> samples = new List<double>();
                        double i;
                        for (i = 0.0; i < 360.0; i++)
                        {
                            int count = 0;
                            double value;
                            if (toggleCosSin)
                                value = Math.Sin(Math.PI / 180 * i);
                            else
                                value = Math.Cos(Math.PI / 180 * i);
                            samples.Add(value);
                            count++;
                        }

                        DataframeColumn dataframeColumn = new DataframeColumn(samples);
                        DataframeEvent dataFrame = new DataframeEvent(DateTimeOffset.UtcNow, dataframeColumn);

                        DataframeReplicationMessage samplesetReplicationMessage = new DataframeReplicationMessage("MqttTestSampleSet01", dataFrame);
                        var messageWrapper = samplesetReplicationMessage.ToMessageWrapper();
                        client.Publish(Topics.CloudBound, messageWrapper.ToByteArray(), MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, false);
                    }
                    if (pressedKey.Key == ConsoleKey.D6)
                    {
                        timeSeriesOnOff = !timeSeriesOnOff;
                        if (timeSeriesOnOff)
                        {
                            timeSeriesThread = new Thread(() => TimeSeries(numberOfPackets, externalId));
                            timeSeriesThread.Start();
                        }
                        else
                        {
                            timeSeriesThread?.Join();
                        }
                    }

                    if (pressedKey.Key == ConsoleKey.D7)
                    {
                        bool compress = false;
                        bool done = false;
                        do
                        {
                            int i;
                            MessageArray array = new MessageArray();
                            if (compress)
                                done = true;
                            for (i = 0; i < 10; i++)
                            {
                                DateTimeOffset time = DateTimeOffset.UtcNow;

                                TimeseriesDoublesReplicationMessage tds = new TimeseriesDoublesReplicationMessage("MqttTestTimeSeries01", time, i + 1);
                                array.Messages.Add(tds.ToMessageWrapper());
                                var delay = Delay(10);
                                delay.Wait();
                            }

                            bool manOverride = true;
                            var stateChanged = new StateChangeReplicationMessage
                            {
                                ExternalId = "MqttTestStateChangeEvent01",
                            };
                            for (i = 0; i < 10; i++)
                            {
                                manOverride = !manOverride;
                                var sEv = new StateChange(
                                    DateTime.UtcNow,
                                    "Donald Duck",
                                    manOverride,
                                    (uint)i + 1);
                                stateChanged.StateChanges.Add(sEv);
                                var delay = Delay(10);
                                delay.Wait();
                            }
                            array.Messages.Add(stateChanged.ToMessageWrapper());

                            for (i = 0; i < 10; i++)
                            {
                                List<double> samples = new List<double>();
                                for (int j = 0; j < 10; j++)
                                {
                                    samples.Add(j);
                                }
                                DataframeColumn dataframeColumn = new DataframeColumn(samples);
                                DataframeEvent dataFrame = new DataframeEvent(DateTimeOffset.UtcNow, dataframeColumn);
                                DataframeReplicationMessage samplesetReplicationMessage = new DataframeReplicationMessage("MqttTestSampleSet01", dataFrame);
                                array.Messages.Add(samplesetReplicationMessage.ToMessageWrapper());
                                var delay = Delay(10);
                                delay.Wait();
                            }
                            alarmOnOff = false;
                            for (i = 0; i < 10; i++)
                            {
                                Kognifai.Serialization.AlarmStateType alarmState;
                                alarmOnOff = !alarmOnOff;
                                alarmState = alarmOnOff ? AlarmStateType.AlarmState : AlarmStateType.NormalState;

                                var alarm = new AlarmReplicationMessage
                                {
                                    ExternalId = "MqttTestAlarm01",
                                };
                                AlarmEvent aEv = new AlarmEvent(
                                    DateTime.UtcNow,
                                    AlarmLevelType.EmergencyLevel,
                                    alarmState,
                                    "Info level, Normal state");
                                alarm.AlarmEvents.Add(aEv);
                                array.Messages.Add(alarm.ToMessageWrapper());
                                var delay = Delay(10);
                                delay.Wait();
                            }
                            MessageArrayContainer container = new MessageArrayContainer("",array,compress);
                            client.Publish(Topics.CloudBoundContainer, container.ToByteArray(), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, false);
                            compress = !compress;
                        } while (!done);
                    }

                    if (pressedKey.Key == ConsoleKey.Q)
                    {
                        clientExit = true;
                        try
                        {
                            if (client.IsConnected)
                            {
                                Console.WriteLine("### Disconnecting from server ###");
                                client.Disconnect();
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("### Disconnect from server failed ### " + e.Message );
                        }
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e.Message);
                }
             }
        }
        private static void ApplicationMessageReceived(object sender, MqttMsgPublishEventArgs message)
        {
            Console.WriteLine("### RECEIVED APPLICATION MESSAGE ###");
            Console.WriteLine($"+ Topic = {message.Topic}");
            Console.WriteLine($"+ Payload = {Encoding.UTF8.GetString(message.Message)}");
            Console.WriteLine($"+ QoS = {message.QosLevel}");
            Console.WriteLine($"+ Retain = {message.Retain}");
            Console.WriteLine();
        }
        private static void TimeSeries(double numberOfPackets, string externalId)
        {
            bool forever = numberOfPackets == 0 ? true : false;
            Console.WriteLine("Sending timeseries data.......");
            double i = 0;
            var stopwatch = Stopwatch.StartNew();
            while ((forever || i < numberOfPackets) && timeSeriesOnOff)
            {
                DateTimeOffset time = DateTimeOffset.Now.AddYears(0);

                TimeseriesDoublesReplicationMessage tds = new TimeseriesDoublesReplicationMessage(externalId, time, Math.Sin(Math.PI / 180 * i));
                var messageWrpper = tds.ToMessageWrapper();
                client.Publish(Topics.CloudBound, messageWrpper.ToByteArray(), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, false);
                i++;
            }
            stopwatch.Stop();
            Console.WriteLine($"Sent { i } messages within {stopwatch.ElapsedMilliseconds} ms ({stopwatch.ElapsedMilliseconds / (float)i} ms / message).");
            timeSeriesOnOff = false;
        }
    }
}
