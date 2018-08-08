using System;
using System.Threading.Tasks;
using PipServices.Commons.Config;
using PipServices.Commons.Data;
using PipServices.Commons.Errors;
using PipServices.Commons.Refer;
using PipServices.Commons.Run;
using PipServices.Components.Auth;
using PipServices.Components.Connect;
using StackExchange.Redis;

namespace PipServices.Redis.Lock
{
    public class RedisLock : PipServices.Components.Lock.Lock,
        IConfigurable, IReferenceable, IOpenable
    {
        private ConnectionResolver _connectionResolver = new ConnectionResolver();
        private CredentialResolver _credentialResolver = new CredentialResolver();

        private string _lock = IdGenerator.NextLong();
        private int _connectTimeout = 30000;
        private int _retryTimeout = 3000;
        private int _retries = 3;
        private ConnectionMultiplexer _client = null;
        private IDatabase _database = null;

        public RedisLock()
        {
        }

        // Make the method virtual
        public new void Configure(ConfigParams config)
        {
            base.Configure(config);

            _connectionResolver.Configure(config);
            _credentialResolver.Configure(config);

            _connectTimeout = config.GetAsIntegerWithDefault("options.connect_timeout", _connectTimeout);
            _retryTimeout = config.GetAsIntegerWithDefault("options.timeout", _retryTimeout);
            _retries = config.GetAsIntegerWithDefault("options.retries", _retries);
        }

        public void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _credentialResolver.SetReferences(references);
        }

        public bool IsOpen()
        {
            return _client != null;
        }

        public async Task OpenAsync(string correlationId)
        {
            var connection = await _connectionResolver.ResolveAsync(correlationId);
            if (connection == null)
                throw new ConfigException(correlationId, "NO_CONNECTION", "Connection is not configured");

            ConfigurationOptions options;
            var uri = connection.Uri;
            if (!string.IsNullOrEmpty(uri))
            {
                options = ConfigurationOptions.Parse(uri);
            }
            else
            {
                var host = connection.Host ?? "localhost";
                var port = connection.Port != 0 ? connection.Port : 6379;
                options = new ConfigurationOptions();
                options.EndPoints.Add(host, port);
            }

            var credential = await _credentialResolver.LookupAsync(correlationId);
            if (credential != null && !string.IsNullOrEmpty(credential.Password))
            {
                options.Password = credential.Password;
            }

            options.ConnectTimeout = _connectTimeout;
            options.ResponseTimeout = _retryTimeout;
            options.ConnectRetry = _retries;

            _client = await ConnectionMultiplexer.ConnectAsync(options);
            _database = _client.GetDatabase();
        }

        public async Task CloseAsync(string correlationId)
        {
            if (_client != null)
            {
                await _client.CloseAsync();
                _client = null;
                _database = null;
            }
        }

        private void CheckOpened(string correlationId)
        {
            if (!IsOpen())
                throw new InvalidStateException(correlationId, "NOT_OPENED", "Connection is not opened");
        }

        public override bool TryAcquireLock(string correlationId, string key, long ttl)
        {
            CheckOpened(correlationId);

            return _database.StringSetAsync(key, _lock, TimeSpan.FromMilliseconds(ttl), When.NotExists).Result;
        }

        public override void ReleaseLock(string correlationId, string key)
        {
            CheckOpened(correlationId);

            var transaction = _database.CreateTransaction();
            transaction.AddCondition(Condition.StringEqual(key, _lock));
            transaction.KeyDeleteAsync(key);
            transaction.Execute();
        }
    }
}
