using System;
using System.Collections;
using System.Collections.Generic;

namespace System.Data.RopSql.Interfaces
{
    public interface IPersistence
    {
        int Create(object entity, Type entityType, bool persistComposition);
        int Edit(object entity, object entityFilter, Type entityType, bool persistComposition);
        int Delete(object filterEntity, Type entityType);
        object View(object filterEntity, Type entityType, List<int> primaryKeyFilters, bool loadComposition);
        IList List(object filterEntity, Type entityType, List<int> primaryKeyFilters, int registryLimit, string showAttributes, string groupAttributes, string orderAttributes, bool onlyListableAttributes, bool getExclusion, bool orderDescending, bool uniqueQuery, bool loadComposition);
        void DefineSearchFilter(object entity, string filter);
        void StartTransaction();
        void CommitTransaction();
        void CancelTransaction();
    }
}
