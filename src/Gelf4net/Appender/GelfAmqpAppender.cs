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
            RemoteAddress = "127.0.0.1";
            RemotePort = 5672;
            RemoteExchange = "sendExchange";
            RemoteExchangeKey = "key";
            RemoteExchangeType = ExchangeType.Direct;
            RemoteExchangeDurable = true;
            RemoteExchangeAutoDelete = false;
            RemoteQueue = "TestQueue";
            RemoteQueueDurable = true;
            RemoteQueueExclusive = false;
            RemoteQueueAutoDelete = false;
            VirtualHost = "/";
            Username = "guest";
            Password = "guest";
            Gzip = true;
        }

        protected ConnectionFactory ConnectionFactory { get; set; }
        public string RemoteAddress { get; set; }
        public int RemotePort { get; set; }
        public string RemoteExchange { get; set; }
        public string RemoteExchangeKey { get; set; }
        public string RemoteExchangeType { get; set; }
        public bool RemoteExchangeDurable { get; set; }
        public bool RemoteExchangeAutoDelete { get; set; }
        public string RemoteQueue { get; set; }
        public bool RemoteQueueDurable { get; set; }
        public bool RemoteQueueExclusive { get; set; }
        public bool RemoteQueueAutoDelete { get; set; }
        public string VirtualHost { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public Encoding Encoding { get; set; }
        public bool Gzip { get; set; }

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

            byte[] message;

            if (Gzip == false)
            {
                message = Encoding.GetBytes(RenderLoggingEvent(loggingEvent));
            }
            else
            {
                message = RenderLoggingEvent(loggingEvent).GzipMessage(Encoding);
            }

            byte[] messageBodyBytes = message;
            try
            {
                model.BasicPublish("sendExchange", "key", null, messageBodyBytes);
            }
            catch (Exception e)
            {
                // Try to reconnect
                OpenConnection();
                model.BasicPublish("sendExchange", "key", null, messageBodyBytes);
            }
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

            model.ExchangeDeclare(RemoteExchange, RemoteExchangeType, RemoteExchangeDurable, RemoteExchangeAutoDelete, null);
            model.QueueDeclare(RemoteQueue, RemoteQueueDurable, RemoteQueueExclusive, RemoteQueueAutoDelete, null);
            model.QueueBind(RemoteQueue, RemoteExchange, RemoteExchangeKey);
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
