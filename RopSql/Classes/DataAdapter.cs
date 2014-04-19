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
    public class RopSqlDataAdapter
    {
        #region Declarations

            IPersistence persistence = null;
            string dataLayerPath = string.Empty;

        #endregion

        #region Construtors

            public RopSqlDataAdapter()
            {
                if (InstanceCache.Persistence == null)
                {
                    //dataLayerPath = string.Concat(ConfigurationManager.AppSettings["RopSqlBinPath"], "RopSql.dll");
                    
                    //persistence = (IPersistence)Activator.CreateInstanceFrom(dataLayerPath,
                    //                                                         "System.Data.RopSql.DataPersistence").Unwrap();
                    
                    InstanceCache.Persistence = new DataPersistence();
                }
                else
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

            public virtual IList List(object filterEntity, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }
                            
            public IList List(object filterEntity, bool onlyListables, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, 0, string.Empty, string.Empty, string.Empty, onlyListables, false, false, false, loadComposition);
            }

            public IList List(object filterEntity, int recordLimit, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, recordLimit, string.Empty, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public IList List(object filterEntity, List<int> primaryKeyFilters, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), primaryKeyFilters, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, loadComposition);
            }

            public IList List(object filterEntity, List<int> primaryKeyFilters, bool getExclusion, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), primaryKeyFilters, 0, string.Empty, string.Empty, string.Empty, false, getExclusion, false, false, loadComposition);
            }

            public IList List(object filterEntity, bool onlyListables, int recordLimit, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, recordLimit, string.Empty, string.Empty, string.Empty, onlyListables, false, false, false, loadComposition);
            }

            public IList List(object filterEntity, string groupAttributes, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, 0, string.Empty, groupAttributes, string.Empty, false, false, false, false, loadComposition);
            }

            public IList List(object filterEntity, string orderAttributes, bool orderDescending, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, 0, string.Empty, string.Empty, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public IList List(object filterEntity, string groupAttributes, string orderAttributes, bool orderDescending, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, 0, string.Empty, string.Empty, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public IList List(object filterEntity, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, 0, string.Empty, groupAttributes, orderAttributes, false, false, orderDescending, false, loadComposition);
            }

            public IList List(object filterEntity, string showAttributes, string groupAttributes, string orderAttributes, int recordLimit, bool orderDescending, bool loadComposition)
            {
                return persistence.List(filterEntity, filterEntity.GetType(), null, recordLimit, showAttributes, groupAttributes, orderAttributes, false, false, orderDescending, false, loadComposition);
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
                else
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

        #endregion
    }

    static class InstanceCache
    {
        public static object Persistence = null;
    }
}
