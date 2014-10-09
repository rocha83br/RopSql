using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace System.Data.RopSql
{
    public static class DataCache
    {
        private static Dictionary<long, List<object>> cacheItems = null;

        public static List<object> List(long cacheId)
        {
            List<object> result = null;

            if (cacheItems[cacheId] != null)
                return cacheItems[cacheId];

            return result;
        }

        public static object Get(long cacheId, object cacheItem)
        {
            object result = null;

            if (cacheItems != null)
                result = cacheItems[cacheId].FirstOrDefault(item => item.Equals(cacheItem));

            return result;
        }

        public static void Put(long cacheId, object cacheItem)
        {
            if (cacheItem != null)
            {
                if (cacheItems == null)
                    cacheItems = new Dictionary<long, List<object>>();

                if (cacheItems[cacheId] == null)
                    cacheItems.Add(cacheId, new List<object>());

                cacheItems[cacheId].Add(cacheItem);
            }
        }
    }
}
