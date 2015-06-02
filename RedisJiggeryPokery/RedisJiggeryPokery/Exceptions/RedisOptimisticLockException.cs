using System;

namespace RedisJiggeryPokery.Exceptions
{
    public class RedisOptimisticLockException : Exception
    {
        public RedisOptimisticLockException(string message) : base(message)
        {
        }

         public RedisOptimisticLockException(string message, Exception innerException)
             : base(message, innerException)
        {
        }

         internal static RedisOptimisticLockException GenerateBasicException(string message)
        {
            var currentException = new RedisOptimisticLockException(message);

            return currentException;
        }

        internal static RedisOptimisticLockException GenerateBasicException(string key, string payload, string message = null)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (payload == null) throw new ArgumentNullException("payload");

            const string messageTemplate = "Item is locked, please try again later. Key : {0} | Value : {1}";

            var errorMessage = string.Format(messageTemplate, key, payload);

            if (message != null)
            {
                errorMessage += errorMessage;
            }

            return GenerateBasicException(errorMessage);
        }
    }
}