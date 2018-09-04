using System;
using System.Threading.Tasks;

using PipServices.Commons.Config;
using PipServices.Commons.Convert;
using PipServices.Commons.Errors;
using PipServices.Commons.Refer;
using PipServices.Components.Auth;
using PipServices.Components.Cache;
using PipServices.Components.Connect;

using StackExchange.Redis;

namespace PipServices.Redis.Cache
{
    public class RedisCache: AbstractCache
    {
        private ConnectionResolver _connectionResolver = new ConnectionResolver();
        private CredentialResolver _credentialResolver = new CredentialResolver();

        private int _connectTimeout = 30000;
        private int _retryTimeout = 3000;
        private int _retries = 3;
        private ConnectionMultiplexer _client = null;
        private IDatabase _database = null;

        public RedisCache()
        {
        }

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            _connectionResolver.Configure(config);
            _credentialResolver.Configure(config);

            _connectTimeout = config.GetAsIntegerWithDefault("options.connect_timeout", _connectTimeout);
            _retryTimeout = config.GetAsIntegerWithDefault("options.timeout", _retryTimeout);
            _retries = config.GetAsIntegerWithDefault("options.retries", _retries);
        }

        public override void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _credentialResolver.SetReferences(references);
        }

        public override bool IsOpen()
        {
            return _client != null;
        }

        public override async Task OpenAsync(string correlationId)
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

        public override async Task CloseAsync(string correlationId)
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

        public override async Task<T> RetrieveAsync<T>(string correlationId, string key)
        {
            CheckOpened(correlationId);

            var json = await _database.StringGetAsync(key);
            var value = JsonConverter.FromJson<T>(json);

            return value;
        }

        public override async Task<T> StoreAsync<T>(string correlationId, string key, T value, long timeout)
        {
            CheckOpened(correlationId);

            timeout = timeout > 0 ? timeout : Timeout;

            var json = JsonConverter.ToJson(value);
            var result = await _database.StringSetAsync(key, json, TimeSpan.FromMilliseconds(timeout));

            return result ? value : default(T);
        }

        public override async Task RemoveAsync(string correlationId, string key)
        {
            CheckOpened(correlationId);

            await _database.KeyDeleteAsync(key);
        }
    }
}
