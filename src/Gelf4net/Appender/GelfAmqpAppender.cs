using log4net.Appender;
using log4net.Util;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.v0_9_1;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace gelf4net.Appender
{
    public class GelfAmqpAppender : AppenderSkeleton
    {
        private IConnection connection;
        private IModel model;

        public GelfAmqpAppender()
        {
            Encoding = Encoding.UTF8;
        }

        protected ConnectionFactory ConnectionFactory { get; set; }
        public string RemoteAddress { get; set; }
        public int RemotePort { get; set; }
        public string RemoteQueue { get; set; }
        public string VirtualHost { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public Encoding Encoding { get; set; }

        public override void ActivateOptions()
        {
            base.ActivateOptions();

            InitializeConnectionFactory();
        }

        protected virtual void InitializeConnectionFactory()
        {
            ConnectionFactory = new ConnectionFactory()
            {
                Protocol = Protocols.FromEnvironment(),
                HostName = RemoteAddress,
                Port = RemotePort,
                VirtualHost = VirtualHost,
                UserName = Username,
                Password = Password
            };
        }

        protected override void Append(log4net.Core.LoggingEvent loggingEvent)
        {
            EnsureConnectionIsOpen();

            byte[] message = RenderLoggingEvent(loggingEvent).GzipMessage(Encoding);
            model.BasicPublish("sendExchange", "key", null, message);
        }

        public void EnsureConnectionIsOpen()
        {
            if (model != null) return;
            OpenConnection();
        }

        private void OpenConnection()
        {
            connection = ConnectionFactory.CreateConnection();
            connection.ConnectionShutdown += ConnectionShutdown;
            model = connection.CreateModel();

            model.ExchangeDeclare("sendExchange", ExchangeType.Direct);
            model.QueueDeclare(RemoteQueue, true, false, false, null);
            model.QueueBind(RemoteQueue, "sendExchange", "key");
        }

        void ConnectionShutdown(IConnection shutingDownConnection, ShutdownEventArgs reason)
        {
            SafeShutdownForConnection();
            SafeShutDownForModel();
        }

        private void SafeShutDownForModel()
        {
            if (model == null) return;
            model.Close(Constants.ReplySuccess, "gelf rabbit appender shutting down!");
            model.Dispose();
            model = null;
        }

        private void SafeShutdownForConnection()
        {
            if (connection == null) return;
            connection.ConnectionShutdown -= ConnectionShutdown;
            connection.AutoClose = true;
            connection = null;
        }
    }
}
