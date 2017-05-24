using System;
using System.Collections.Generic;

namespace System.Data.RopSql.Interfaces
{
    public interface IDataAdapter
    {
        int Create(object entity, bool persistComposition);
        int Edit(object entity, object filterEntity, bool persistComposition);
        int Delete(object filterEntity);
        List<T> List<T>(T filterEntity, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, int recordLimit, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, Dictionary<string, double[]> rangeValues, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, string groupAttributes, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, string orderAttributes, bool orderDescending, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, bool orderDescending, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition) where T : class;
        List<T> List<T>(T filterEntity, string showAttributes, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition) where T : class;
        T Get<T>(T filterEntity, bool loadComposition) where T : class;
        int GetMax<T>();
        int Count<T>();
        void StartTransaction();
        void CommitTransaction();
        void CancelTransaction();
    }
}
