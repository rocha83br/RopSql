using System;
using System.Text;
using System.Reflection;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data.RopSql.Interfaces;

namespace System.Data.RopSql
{
    public class RopSqlDataAdapter : Interfaces.IDataAdapter, IDisposable
    {
        #region Declarations

            IPersistence persistence = null;
            string dataLayerPath = string.Empty;

        #endregion

        #region Construtors

            public RopSqlDataAdapter(bool keepDBConnection = false)
            {
                if ((InstanceCache.Persistence == null) || keepDBConnection)
                    InstanceCache.Persistence = new DataPersistence(keepDBConnection);
                
                persistence = (IPersistence)InstanceCache.Persistence;
            }

        #endregion

        #region Public Methods

            public virtual int Create(object entity, bool persistComposition)
            {
                return persistence.Create(entity, entity.GetType(), persistComposition);
            }

            public virtual int Edit(object entity, object filterEntity, bool persistComposition)
            {
                return persistence.Edit(entity, filterEntity, entity.GetType(), persistComposition);
            }

            public virtual int Delete(object filterEntity)
            {
                return persistence.Delete(filterEntity, filterEntity.GetType());
            }

            public virtual T Get<T>(T filterEntity, bool loadComposition) where T : class
            {
                return persistence.Get<T>(filterEntity, null, loadComposition) as T;
            }

            public virtual IList<T> List<T>(T filterEntity, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public IList<T> List<T>(object procParamsEntity, bool loadComposition) where T : class
            {
                return persistence.List<T>(procParamsEntity, procParamsEntity.GetType(), loadComposition);
            }

            public IList<T> List<T>(T filterEntity, bool onlyListables, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, null, string.Empty, string.Empty, onlyListables, false, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, int recordLimit, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, recordLimit, string.Empty, null, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, List<int> primaryKeyFilters, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, primaryKeyFilters, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, List<int> primaryKeyFilters, bool getExclusion, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, primaryKeyFilters, 0, string.Empty, null, string.Empty, string.Empty, false, getExclusion, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, Dictionary<string, double[]> rangeValues, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, rangeValues, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, bool onlyListables, int recordLimit, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, recordLimit, string.Empty, null, string.Empty, string.Empty, onlyListables, false, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, string groupAttributes, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, null, groupAttributes, string.Empty, false, false, false, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, string orderAttributes, bool orderDescending, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, null, string.Empty, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, bool orderDescending, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, null, string.Empty, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, 0, string.Empty, null, groupAttributes, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public IList<T> List<T>(T filterEntity, string showAttributes, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition) where T : class
            {
                return persistence.List<T>(filterEntity, null, recordLimit, showAttributes, null, groupAttributes, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public int GetMax<T>()
            {
                return persistence.GetMax<T>();
            }

            public IList<T> Count<T>(T filterEntity, string groupAttributes)
            {
                return persistence.Count<T>(filterEntity, groupAttributes);
            }

            public int Count<T>()
            {
                return persistence.Count<T>();
            }

            public void DefineSearchFilter(object entity, string filter)
            {
                persistence.DefineSearchFilter(entity, filter);
            }

            public void StartTransaction()
            {
                if (InstanceCache.Persistence == null)
                    InstanceCache.Persistence = new DataPersistence(true);
                
                persistence = (IPersistence)InstanceCache.Persistence;

                persistence.StartTransaction();
            }

            public void CommitTransaction()
            {
                persistence.CommitTransaction();
            }

            public void CancelTransaction()
            {
                persistence.CancelTransaction();
            }

            public void Dispose()
            {
                this.Dispose(false);
            }

            protected virtual void Dispose(bool managed)
            {
                persistence.Dispose();

                if (!managed)
                    GC.ReRegisterForFinalize(this);
                else
                    GC.Collect(GC.GetGeneration(this), GCCollectionMode.Default);
            }

        #endregion
    }

    static class InstanceCache
    {
        public static object Persistence = null;
    }
}
