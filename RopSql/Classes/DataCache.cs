using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json;

namespace System.Data.RopSql
{
    public static class DataCache
    {
        #region Declarations

        private static Dictionary<KeyValuePair<int, string>, object> cacheItems = 
            new Dictionary<KeyValuePair<int, string>, object>();

        #endregion

        #region Public Methods

        public static object Get(object cacheKey)
        {
            object result = null;

            if (cacheKey != null)
            {
                var serialKey = JsonConvert.SerializeObject(cacheKey);
                var serialCacheKey = new KeyValuePair<int, string>(cacheKey.GetType().GetHashCode(), serialKey);
                if (cacheItems.ContainsKey(serialCacheKey))
                    result = cacheItems[serialCacheKey];
            }

            var listResult = result as IList;

            if ((listResult != null) && (listResult.Count == 1))
                result = ((IList)result)[0];
            
            return result;
        }

        public static void Put(object cacheKey, object cacheItem)
        {
            var parallelParam = new ParallelParam()
            {
                Param1 = cacheKey,
                Param2 = cacheItem
            };

            var parallelDelegate = new ParameterizedThreadStart(put);

            Parallelizer.StartNewProcess(parallelDelegate, parallelParam);
        }

        private static void put(object param)
        {
            ParallelParam parallelParam = param as ParallelParam;
            object cacheKey = parallelParam.Param1;
            object cacheItem = parallelParam.Param2;

            if ((cacheKey != null) && (cacheItem != null))
            {
                var serialKey = JsonConvert.SerializeObject(cacheKey);
                var serialCacheKey = new KeyValuePair<int, string>(cacheKey.GetType().GetHashCode(), serialKey);
                if (!cacheItems.ContainsKey(serialCacheKey))
                    cacheItems.Add(serialCacheKey, cacheItem);

                updateCacheTree(cacheKey.GetType().GetHashCode(), cacheItem);
            }
        }

        public static void Del(object cacheKey)
        {
            if (cacheKey != null)
            {
                var serialKey = JsonConvert.SerializeObject(cacheKey);
                var serialCacheKey = new KeyValuePair<int, string>(cacheKey.GetType().GetHashCode(), serialKey);
                if (cacheItems.ContainsKey(serialCacheKey))
                    cacheItems.Remove(serialCacheKey);
            }
        }

        public static void Clear()
        {
            cacheItems = null;
        }

        #endregion

        #region Helper Methods

        private static void updateCacheTree(int typeKeyCode, object cacheItem)
        {
            if (!(cacheItem is IList))
            {
                var typeCacheItems = cacheItems.Where(itm => itm.Key.Key.Equals(typeKeyCode)).ToList();
                var itemKeyId = EntityReflector.GetKeyColumn(cacheItem, false).GetValue(cacheItem, null);

                foreach (var typeCacheItem in typeCacheItems)
                {
                    var listValue = typeCacheItem.Value as IList;

                    if (listValue != null)
                        for (int count = 0; count < ((IList)typeCacheItem.Value).Count; count++)
                        {
                            if (EntityReflector.MatchKeys(cacheItem, listValue[count]))
                            {
                                listValue[count] = cacheItem;
                                break;
                            }
                        }
                }
            }
        }

        #endregion
    }
}
