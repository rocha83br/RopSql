using System;
using System.Collections.Generic;

namespace System.Data.RopSql.Interfaces
{
    public interface IDataAdapter
    {
        int Create<T>(T entity);
        int Edit<T>(T entity, T filterEntity);
        int Delete<T>(T filterEntity);
        List<T> List<T>(T filterEntity);
        List<T> List<T>(T filterEntity, int recordLimit);
        List<T> List<T>(T filterEntity, string groupAttributes);
        List<T> List<T>(T filterEntity, string orderAttributes, bool orderDescending);
        List<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, bool orderDescending);
        List<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending);
        List<T> List<T>(T filterEntity, string showAttributes, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending);
        T Get<T>(T filterEntity);
        void StartTransaction();
        void CommitTransaction();
        void CancelTransaction();
    }
}
