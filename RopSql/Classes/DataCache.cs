using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace System.Data.RopSql
{
    public static class DataCache
    {
        #region Declarations

        private static Dictionary<object, object> cacheItems = null;

        #endregion

        #region Public Methods

        public static object Get(object cacheKey)
        {
            object result = null;

            if (cacheItems[cacheKey] != null)
                return cacheItems[cacheKey];

            return result;
        }

        public static void Put(object cacheKey, object cacheItem)
        {
            if (cacheItem != null)
            {
                if (cacheItems == null)
                    cacheItems = new Dictionary<object, object>();

                if (cacheItems[cacheKey] == null)
                    cacheItems.Add(cacheKey, cacheItem);
            }
        }

        public static void Del(object cacheKey)
        {
            if (cacheKey != null)
                cacheItems.Remove(cacheKey);
        }

        public static void Clear()
        {
            cacheItems = null;
        }

        #endregion
    }
}
