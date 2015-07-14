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

            public RopSqlDataAdapter()
            {
                if (InstanceCache.Persistence == null)
                    InstanceCache.Persistence = new DataPersistence();
                
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

            public virtual object Get(object filterEntity, bool loadComposition)
            {
                return persistence.Get(filterEntity, filterEntity.GetType(), null, loadComposition);
            }

            public virtual List<T> List<T>(T filterEntity, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, bool onlyListables, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, null, string.Empty, string.Empty, onlyListables, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, int recordLimit, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, recordLimit, string.Empty, null, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, List<int> primaryKeyFilters, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), primaryKeyFilters, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, List<int> primaryKeyFilters, bool getExclusion, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), primaryKeyFilters, 0, string.Empty, null, string.Empty, string.Empty, false, getExclusion, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, Dictionary<string, double[]> rangeValues, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, rangeValues, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, bool onlyListables, int recordLimit, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, recordLimit, string.Empty, null, string.Empty, string.Empty, onlyListables, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, string groupAttributes, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, null, groupAttributes, string.Empty, false, false, false, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, string orderAttributes, bool orderDescending, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, null, string.Empty, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, bool orderDescending, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, null, string.Empty, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, 0, string.Empty, null, groupAttributes, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public List<T> List<T>(T filterEntity, string showAttributes, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition)
            {
                return persistence.List<T>(filterEntity, filterEntity.GetType(), null, recordLimit, showAttributes, null, groupAttributes, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public void DefineSearchFilter(object entity, string filter)
            {
                persistence.DefineSearchFilter(entity, filter);
            }

            public void StartTransaction()
            {
                if (InstanceCache.Persistence == null)
                {
                    //persistence = (IPersistence)Activator.CreateInstanceFrom(dataLayerPath,
                    //                                                         "System.Data.RopSql.DataPersistence", new object[] { true }).Unwrap();
                    
                    InstanceCache.Persistence = new DataPersistence();
                    
                }
                
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
                persistence = null;
                GC.ReRegisterForFinalize(this);
            }

        #endregion
    }

    static class InstanceCache
    {
        public static object Persistence = null;
    }
}
