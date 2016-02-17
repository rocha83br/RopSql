using System;
using System.Xml;
using System.Text;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Configuration;
using System.Threading;
using System.Data.RopSql;
using System.Data.RopSql.Interfaces;
using System.Data.RopSql.Resources;
using System.Data.RopSql.DataAnnotations;
using System.Data.RopSql.Exceptions;

namespace System.Data.RopSql
{
    public class DataPersistence : DataBaseODBCConnection, IPersistence
    {
        #region Declarations

        private readonly string[] emptyArray = new string[0];
        bool keepConnection = false;

        #endregion

        #region Constructors

        public DataPersistence() { }

        public DataPersistence(bool keepDBConnected)
        {
            keepConnection = keepDBConnected;
            if (keepConnection) base.connect();
        }

        #endregion

        #region Public Methods

        public int Create(object entity, Type entityType, bool persistComposition)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters;
            int lastInsertedId = 0;

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(entity, entityType),
                                                  entityType,
                                                  (int)PersistenceAction.Create,
                                                  null, null, false,
                                                  emptyArray, null,
                                                  out commandParameters);

                lastInsertedId = base.executeCommand(sqlInstruction, commandParameters);

                // Atualizacao do Cache
                var tableAttrib = getTableAttrib(entity);
                if (tableAttrib.IsCacheable)
                    DataCache.Del(entity, true);

                // Persistencia assincrona da composicao

                if (persistComposition)
                {
                    var entityColumnKey = EntityReflector.GetKeyColumn(entity, false);

                    if (entityColumnKey != null)
                        entityColumnKey.SetValue(entity, lastInsertedId, null);

                    ParallelParam parallelParam = new ParallelParam()
                    {
                        Param1 = entity,
                        Param2 = entityType,
                        Param3 = (int)PersistenceAction.Create,
                        Param4 = commandParameters
                    };

                    var parallelDelegate = new ParameterizedThreadStart(parseCompositionAsync);

                    Parallelizer.StartNewProcess(parallelDelegate, parallelParam);
                    //parseCompositionAsync(parallelParam);
                }
                else
                {
                    if (!keepConnection) base.disconnect();

                    // Atualizacao do Cache

                    if (tableAttrib.IsCacheable)
                        DataCache.Del(entity, true);
                }
            }

            return lastInsertedId;
        }

        public int Edit(object entity, object filterEntity, Type entityType, bool persistComposition)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters = null;
            int recordsAffected = 0;
            List<string> childEntityCommands = new List<string>();

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(entity, entityType),
                                                     entityType, (int)PersistenceAction.Edit, filterEntity,
                                                     null, false, emptyArray, null, out commandParameters);

                recordsAffected = executeCommand(sqlInstruction, commandParameters);
            }

            // Atualizacao do Cache
            var tableAttrib = getTableAttrib(entity);
            if (tableAttrib.IsCacheable)
                DataCache.Del(entity, true);

            // Persistencia assincrona da composicao

            if (persistComposition)
            {
                ParallelParam parallelParam = new ParallelParam()
                {
                    Param1 = entity,
                    Param2 = entityType,
                    Param3 = (int)PersistenceAction.Edit,
                    Param4 = commandParameters,
                    Param5 = filterEntity
                };

                var parallelDelegate = new ParameterizedThreadStart(parseCompositionAsync);

                Parallelizer.StartNewProcess(parallelDelegate, parallelParam);
                //parseCompositionAsync(parallelParam);
            }
            else
            {
                if (!keepConnection) base.disconnect();

                // Atualizacao do Cache

                if (tableAttrib.IsCacheable)
                    DataCache.Del(filterEntity, true);
            }

            return recordsAffected;
        }

        public int Delete(object filterEntity, Type entityType)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters;
            int recordAffected = 0;

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(filterEntity, entityType),
                                             entityType,
                                             (int)PersistenceAction.Delete,
                                             Convert.ChangeType(filterEntity, entityType),
                                             null, false, emptyArray,
                                             null, out commandParameters);

                recordAffected = executeCommand(sqlInstruction, commandParameters);
            }

            if (!keepConnection) base.disconnect();

            if (getTableAttrib(filterEntity).IsCacheable)
                DataCache.Del(filterEntity, true);

            return recordAffected;
        }

        public object Get(object filterEntity, Type entityType, List<int> primaryKeyFilters, bool loadComposition)
        {
            object result = null;

            if (DataCache.Get(filterEntity) != null)
                result = DataCache.Get(filterEntity);
            else
            {
                var queryList = List(filterEntity, entityType, primaryKeyFilters, 0,
                                string.Empty, null, string.Empty, string.Empty,
                                false, false, false, true, loadComposition);

                if (queryList.Count > 0) result = queryList[0];
            }

            return result;
        }

        public List<T> List<T>(object filterEntity, Type entityType, List<int> primaryKeyFilters, int recordLimit, string showAttributes, Dictionary<string, double[]> rangeValues, string groupAttributes, string orderAttributes, bool onlyListableAttributes, bool getExclusion, bool orderDescending, bool uniqueQuery, bool loadComposition)
        {
            // Verificando cache

            IList result = null;

            if (DataCache.Get(filterEntity) != null)
            {
                result = DataCache.Get(filterEntity) as IList;
                if (result == null)
                {
                    result = new List<T>();
                    result.Add((T)DataCache.Get(filterEntity));
                }
            }
            else
            {
                result = List(filterEntity, entityType, primaryKeyFilters, recordLimit, showAttributes, rangeValues, groupAttributes, orderAttributes, onlyListableAttributes, getExclusion, orderDescending, uniqueQuery, loadComposition);

                if (getTableAttrib(filterEntity).IsCacheable)
                    DataCache.Put(filterEntity, result);
            }

            return result as List<T>;
        }

        public IList List(object filterEntity, Type entityType, List<int> primaryKeyFilters, int recordLimit, string showAttributes, Dictionary<string, double[]> rangeValues, string groupAttributes, string orderAttributes, bool onlyListableAttributes, bool getExclusion, bool orderDescending, bool uniqueQuery, bool loadComposition)
        {
            string sqlInstruction = string.Empty;
            string[] displayAttributes = new string[0];
            string[] groupingAttributes = new string[0];
            string[] ordinationAttributes = new string[0];
            Dictionary<object, object> attributeColumnRelation = null;
            Dictionary<object, object> commandParameters;
            string columnList = string.Empty;
            int persistenceAction = uniqueQuery ? (int)PersistenceAction.View
                                                : (int)PersistenceAction.List;

            var entityProps = entityType.GetProperties();

            Type dynamicListType = typeof(List<>).MakeGenericType(new Type[] { entityType });
            object returnList = Activator.CreateInstance(dynamicListType, true);

            if (onlyListableAttributes)
                validateListableAttributes(entityType, showAttributes, out displayAttributes);

            // Montando instrução de consulta

            sqlInstruction = parseEntity(Convert.ChangeType(filterEntity, entityType),
                                         entityType,
                                         persistenceAction,
                                         Convert.ChangeType(filterEntity, entityType),
                                         primaryKeyFilters, getExclusion, displayAttributes,
                                         rangeValues, out commandParameters);

            sqlInstruction = string.Format(sqlInstruction, recordLimit > 0 ? string.Format(SQLANSIRepository.DataPersistence_Action_LimitResult_MySQL, recordLimit) : string.Empty, "{0}", "{1}");

            if (!string.IsNullOrEmpty(groupAttributes))
            {
                string complementaryColumnList = string.Empty;

                groupingAttributes = groupAttributes.Split(',');

                for (int cont = 0; cont < groupingAttributes.Length; cont++)
                    groupingAttributes[cont] = groupingAttributes[cont].Trim();

                attributeColumnRelation = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, entityProps, persistenceAction, null, out commandParameters);

                foreach (var rel in attributeColumnRelation)
                    if (Array.IndexOf(groupingAttributes, rel.Key) > -1)
                        columnList += string.Format("{0}, ", ((KeyValuePair<string, object>)rel.Value).Key);
                    else
                        if ((!rel.Key.Equals("Class")) && (!rel.Key.Equals("DataTable")))
                            complementaryColumnList += string.Format("{0}, ", ((KeyValuePair<string, object>)rel.Value).Key);

                if (!String.IsNullOrEmpty(columnList) && Convert.ToInt32(columnList) > 2)
                    columnList = columnList.Substring(0, columnList.Length - 2);
                if (!String.IsNullOrEmpty(complementaryColumnList) && Convert.ToInt32(complementaryColumnList) > 2)
                    complementaryColumnList = complementaryColumnList.Substring(0, complementaryColumnList.Length - 2);

                sqlInstruction = string.Format(sqlInstruction,
                                              string.Format(SQLANSIRepository.DataPersistence_Action_Group,
                                                            columnList, ", ", complementaryColumnList),
                                                            "{0}");
            }
            else
                sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

            if (!string.IsNullOrEmpty(orderAttributes))
            {
                ordinationAttributes = orderAttributes.Split(',');
                attributeColumnRelation = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, entityProps, persistenceAction, null, out commandParameters);

                for (int contAtrib = 0; contAtrib < ordinationAttributes.Length; contAtrib++)
                {
                    ordinationAttributes[contAtrib] = ordinationAttributes[contAtrib].Trim();

                    var attribToOrder = attributeColumnRelation.FirstOrDefault(rca => ordinationAttributes[contAtrib].Equals(rca.Key));
                    var columnToOrder = ((KeyValuePair<object, object>)attribToOrder.Value).Key;

                    if (!(columnToOrder is RelationalColumn))
                        columnList = string.Concat(columnList, columnToOrder, ", ");
                    else
                        columnList = string.Concat(columnList, string.Format("{0}.{1}", ((RelationalColumn)columnToOrder).TableName.ToLower(),
                                                                                        ((RelationalColumn)columnToOrder).ColumnName), ", ");
                }

                columnList = columnList.Substring(0, columnList.Length - 2);

                sqlInstruction = string.Format(sqlInstruction,
                                               string.Format(SQLANSIRepository.DataPersistence_Action_OrderResult,
                                                             columnList,
                                                             orderDescending ? "DESC" : "ASC"));
            }
            else
                sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

            if (keepConnection || base.connect())
            {
                // Tratando retorno do banco

                XmlDocument queryReturn = base.executeQuery(sqlInstruction);

                returnList = parseDatabaseReturn(queryReturn, filterEntity.GetType());
            }

            if (!keepConnection) base.disconnect();

            // Efetuando carga da composição quando existente (Eager Loading)

            if (loadComposition && (((IList)returnList).Count > 0))
                for (int inC = 0; inC < ((IList)returnList).Count; inC++)
                    fillComposition(((IList)returnList)[inC], ((IList)returnList)[inC].GetType());

            return (IList)returnList;
        }

        public List<T> List<T>(object procParamsEntity, Type entityType, bool loadComposition)
        {
            // Verificando cache

            IList result = null;

            if (DataCache.Get(procParamsEntity) != null)
            {
                result = DataCache.Get(procParamsEntity) as IList;
                if (result == null)
                {
                    result = new List<T>();
                    result.Add((T)DataCache.Get(procParamsEntity));
                }
            }
            else
            {
                var procAttribs = getProcAttrib(procParamsEntity);

                result = List(procParamsEntity, entityType, typeof(T), procAttribs.ProcedureName, loadComposition);

                if (procAttribs.IsCacheable)
                    DataCache.Put(procParamsEntity, result);
            }

            return result as List<T>;
        }

        public IList List(object filterEntity, Type entityType, Type returnType, string procedureName, bool loadComposition)
        {
            XmlDocument queryReturn = null;
            string procCommand = string.Empty;
            string procParameters = string.Empty;
            Dictionary<object, object> columnParameters = null;
            Dictionary<object, object> sqlEntityData = null;
            int persistenceAction = (int)PersistenceAction.List;

            var entityProps = entityType.GetProperties();

            Type dynamicListType = typeof(List<>).MakeGenericType(new Type[] { entityType });
            object returnList = Activator.CreateInstance(dynamicListType, true);

            sqlEntityData = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, entityProps, persistenceAction, null, out columnParameters);

            procParameters = getMySqlProcParams(sqlEntityData);

            if (keepConnection || base.connect())
            {
                procedureName = string.Format(SQLANSIRepository.DataPersistence_Action_ExecuteProcedure_MySQL, procedureName);
                procCommand = string.Concat(procedureName, procParameters);

                queryReturn = base.executeQuery(procCommand);

                returnList = parseDatabaseReturn(queryReturn, returnType);
            }

            if (!keepConnection) base.disconnect();

            // Efetuando carga da composição quando existente (Eager Loading)

            if (loadComposition && (((IList)returnList).Count > 0))
                for (int inC = 0; inC < ((IList)returnList).Count; inC++)
                    fillComposition(((IList)returnList)[inC], ((IList)returnList)[inC].GetType());

            return (IList)returnList;
        }

        public void DefineSearchFilter(object entity, string filter)
        {
            DateTime datetimeValue;
            int numericValue;
            long longNumericValue;

            if (!string.IsNullOrEmpty(filter))
            {
                var filterableAttributes = entity.GetType().GetProperties()
                                                           .Where(atb => atb.GetCustomAttributes(true)
                                                           .Any(ca => (ca is IDataColumn)
                                                                   && ((IDataColumn)ca).IsFilterable()));

                foreach (PropertyInfo attrib in filterableAttributes)
                {
                    if (attrib.PropertyType.Name.Equals("DateTime")
                        && DateTime.TryParse(filter, out datetimeValue))
                        attrib.SetValue(entity, datetimeValue, null);
                    else if (attrib.PropertyType.Name.Equals("Int32")
                        && int.TryParse(filter, out numericValue))
                        attrib.SetValue(entity, numericValue, null);
                    else if (attrib.PropertyType.Name.Equals("Int64")
                        && long.TryParse(filter, out longNumericValue))
                        attrib.SetValue(entity, longNumericValue, null);
                    else if (attrib.PropertyType.Name.Equals("String"))
                        attrib.SetValue(entity, filter, null);
                }
            }
        }

        #endregion

        #region Helper Methods

        private string parseEntity(object entity, Type entityType, int action, object filterEntity, List<int> primaryKeyFilters, bool getExclusion, string[] showAttributes, Dictionary<string, double[]> rangeValues, out Dictionary<object, object> commandParameters)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> sqlFilterData;

            commandParameters = null;
            var entityProps = entityType.GetProperties();

            Dictionary<object, object> sqlEntityData = getAnnotationValueList(Convert.ChangeType(entity, entityType), entityType, entityProps, action, primaryKeyFilters, out commandParameters);

            if (filterEntity != null)
                sqlFilterData = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, entityProps, action, primaryKeyFilters, out commandParameters);
            else
                sqlFilterData = null;

            var keyColumn = EntityReflector.GetKeyColumn(entity, false).GetCustomAttributes(true)
                                                                 .FirstOrDefault(cln => cln is DataAnnotations.DataColumn
                                                                                     && ((IDataColumn)cln).IsPrimaryKey()) as DataAnnotations.DataColumn;
            string keyColumnName = string.Empty;
            if (keyColumn != null)
                keyColumnName = keyColumn.ColumnName;

            var hashCode = getEntityHashCode(entity);

            var childHashColumnName = getChildHashColumnName(entityProps);

            var hashColumnName = getEntityHashColumnName(entityProps);

            Dictionary<string, string> sqlParameters = getSqlParameters(sqlEntityData, action, sqlFilterData,
                                                                        showAttributes, keyColumnName, hashCode,
                                                                        childHashColumnName ?? hashColumnName,
                                                                        rangeValues, (primaryKeyFilters != null), getExclusion);

            switch (action)
            {
                case (int)PersistenceAction.Create:

                    sqlInstruction = String.Format(SQLANSIRepository.DataPersistence_Action_Create,
                                                   sqlParameters["dataTable"],
                                                   sqlParameters["columnList"],
                                                   sqlParameters["valueList"]);

                    break;

                case (int)PersistenceAction.Edit:

                    sqlInstruction = String.Format(SQLANSIRepository.DataPersistence_Action_Edit,
                                                   sqlParameters["dataTable"],
                                                   sqlParameters["columnValueList"],
                                                   sqlParameters["columnFilterList"]);

                    break;

                case (int)PersistenceAction.Delete:

                    sqlInstruction = String.Format(SQLANSIRepository.DataPersistence_Action_Delete,
                                                   sqlParameters["dataTable"],
                                                   sqlParameters["columnFilterList"]);

                    break;
                default: // Listagem ou Consulta

                    sqlInstruction = String.Format(SQLANSIRepository.DataPersistence_Action_Query,
                                                   "{0}",
                                                   sqlParameters["columnList"],
                                                   sqlParameters["dataTable"],
                                                   sqlParameters["relationList"],
                                                   sqlParameters["columnFilterList"],
                                                   "{1}", "{2}", string.Empty);

                    break;
            }

            return sqlInstruction;
        }

        private List<string> parseComposition(object entity, Type entityType, int action, object filterEntity)
        {
            List<string> result = new List<string>();
            object childEntityInstance = null;
            Dictionary<object, object> commandParameters;

            IEnumerable<PropertyInfo> childEntities = entityType.GetProperties().Where(prp => prp.GetCustomAttributes(true)
                                                                                .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach (PropertyInfo child in childEntities)
            {
                var relationAttrib = child.GetCustomAttributes(true)
                                          .FirstOrDefault(atb => atb.GetType().Name.Equals("RelatedEntity")) as RelatedEntity;

                childEntityInstance = child.GetValue(entity, null);
                object childEntityFilter = null;

                var entityParent = (action != (int)PersistenceAction.Edit) ? entity : filterEntity;

                if (childEntityInstance != null)
                {
                    if (!childEntityInstance.GetType().Name.Contains("List"))
                    {
                        action = setPersistenceAction(childEntityInstance, EntityReflector.GetKeyColumn(childEntityInstance, false));
                        childEntityFilter = Activator.CreateInstance(childEntityInstance.GetType());

                        if (action == (int)PersistenceAction.Edit)
                            EntityReflector.MigrateEntityPrimaryKey(childEntityInstance, childEntityFilter);

                        setEntityForeignKey(entityParent, child);

                        setEntityHashKey(entityParent, child);

                        result.Add(parseEntity(childEntityInstance, childEntityInstance.GetType(), action, childEntityFilter, null, false, null, null, out commandParameters));
                    }
                    else
                    {
                        var childListInstance = (IList)childEntityInstance;
                        List<object> childFiltersList = new List<object>();

                        if (childListInstance.Count > 0)
                        {
                            foreach (var listItem in childListInstance)
                            {
                                if (relationAttrib.Cardinality == RelationCardinality.OneToMany)
                                {
                                    childEntityFilter = Activator.CreateInstance(listItem.GetType());

                                    action = setPersistenceAction(listItem, EntityReflector.GetKeyColumn(listItem, false));

                                    if (action == (int)PersistenceAction.Edit)
                                    {
                                        EntityReflector.MigrateEntityPrimaryKey(listItem, childEntityFilter);
                                        childFiltersList.Add(childEntityFilter);
                                    }

                                    setEntityForeignKey(entityParent, listItem);
                                    setEntityHashKey(entityParent, listItem);

                                    result.Add(parseEntity(listItem, listItem.GetType(), action, childEntityFilter, null, false, null, null, out commandParameters));
                                }
                                else
                                {
                                    var manyToEntity = parseManyToRelation(listItem, relationAttrib);

                                    setEntityForeignKey(entityParent, manyToEntity);
                                    setEntityHashKey(entityParent, manyToEntity);

                                    var existRelation = this.Get(manyToEntity, manyToEntity.GetType(), null, false);

                                    if (existRelation != null) manyToEntity = existRelation;

                                    action = setPersistenceAction(manyToEntity, EntityReflector.GetKeyColumn(manyToEntity, false));

                                    object existFilter = null;
                                    if (action == (int)PersistenceAction.Edit)
                                    {
                                        existFilter = Activator.CreateInstance(manyToEntity.GetType());
                                        EntityReflector.MigrateEntityPrimaryKey(manyToEntity, existFilter);
                                        childFiltersList.Add(existFilter);
                                    }

                                    result.Add(parseEntity(manyToEntity, manyToEntity.GetType(), action, existFilter, null, false, null, null, out commandParameters));
                                }
                            }
                        }
                        else
                        {
                            var childInstance = Activator.CreateInstance(childListInstance.GetType().GetGenericArguments()[0]);

                            var childEntity = new object();
                            if (relationAttrib.Cardinality == RelationCardinality.ManyToMany)
                            {
                                childEntity = parseManyToRelation(childInstance, relationAttrib);
                                setEntityForeignKey(entityParent, childEntity);
                                setEntityHashKey(entityParent, childEntity);
                            }
                            else
                                childEntity = childInstance;

                            childFiltersList.Add(childEntity);
                        }

                        if ((childFiltersList.Count > 0) && !(relationAttrib.Cardinality == RelationCardinality.OneToOne))
                            result.Add(getExclusionComposition(filterEntity, childFiltersList));
                    }
                }
            }

            if (result.Any(rst => rst.Contains(SQLANSIRepository.DataPersistence_ReservedWord_INSERT)))
                result.Reverse();

            return result;
        }

        private string getExclusionComposition(object entity, IList existentComposition)
        {
            List<int> existentKeys = new List<int>();
            object compositionFilter = null;

            if (existentComposition.Count > 0)
            {
                compositionFilter = Activator.CreateInstance(existentComposition[0].GetType());
                setEntityHashKey(entity, compositionFilter);
            }

            foreach (var composit in existentComposition)
            {
                var compositKeyColumn = EntityReflector.GetKeyColumn(composit, false);
                existentKeys.Add(int.Parse(compositKeyColumn.GetValue(composit, null).ToString()));
            }

            Dictionary<object, object> commandParameters;

            string result = string.Empty;
            if (compositionFilter != null)
                result = parseEntity(compositionFilter, compositionFilter.GetType(), (int)PersistenceAction.Delete, compositionFilter, existentKeys, true, null, null, out commandParameters);

            return result;
        }

        private void setEntityForeignKey(object parentEntity, object childEntity)
        {
            var parentKey = EntityReflector.GetKeyColumn(parentEntity, false);
            var childForeignKey = EntityReflector.GetKeyColumn(childEntity, true, parentEntity.GetType().Name);

            if ((parentKey != null) && (childForeignKey != null))
                childForeignKey.SetValue(childEntity, parentKey.GetValue(parentEntity, null), null);
        }

        private bool setEntityHashKey(object parentEntity, object childEntity)
        {
            bool result = false;
            HashSignature parentHashAnnotation = null;

            var entityKeyColumn = EntityReflector.GetKeyColumn(parentEntity, false);

            if (entityKeyColumn != null)
            {
                var referenceId = childEntity.GetType().GetProperties()
                                             .FirstOrDefault(prp => prp.GetCustomAttributes(true)
                                             .Any(an => an.GetType().GetInterface("IDataColumn") != null)
                                                  && ((IDataColumn)(prp.GetCustomAttributes(true)
                                             .FirstOrDefault(an => an.GetType().GetInterface("IDataColumn") != null))).IsHashId());
                if (referenceId != null)
                {
                    referenceId.SetValue(childEntity, entityKeyColumn.GetValue(parentEntity, null), null);
                    result = true;
                }
                else
                    result = false;
            }

            var entityHashColumn = childEntity.GetType().GetProperties()
                                              .FirstOrDefault(prp => prp.GetCustomAttributes(true)
                                              .Any(an => an.GetType().GetInterface("IDataColumn") != null)
                                                   && ((IDataColumn)(prp.GetCustomAttributes(true)
                                              .FirstOrDefault(an => an.GetType().GetInterface("IDataColumn") != null))).IsHashSignature());

            if (entityHashColumn != null)
            {
                parentHashAnnotation = parentEntity.GetType().GetCustomAttributes(true)
                                                   .FirstOrDefault(ca => ca.GetType().Name
                                                   .Equals("HashSignature")) as HashSignature;

                if (parentHashAnnotation != null)
                {
                    entityHashColumn.SetValue(childEntity, parentHashAnnotation.HashCode, null);
                    result = result && true;
                }
            }

            return result;
        }

        private bool replicateChildHashes(object loadedEntity, object attributeInstance)
        {
            bool result = false;
            List<int> genericAttributesId = null;

            var genericAttributeFilter = attributeInstance.GetType()
                                                          .GetProperties()
                                                          .FirstOrDefault(atb => atb.PropertyType
                                                          .GetInterface("IEntityHash") != null);

            if (genericAttributeFilter != null)
            {
                var genericAttributeInstance = Activator.CreateInstance(genericAttributeFilter.PropertyType, true);
                var relationKeyColumn = EntityReflector.GetKeyColumn(attributeInstance, false);

                setEntityHashKey(loadedEntity, genericAttributeInstance);

                var genericAttributes = List(genericAttributeInstance, genericAttributeInstance.GetType(), null, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, false);

                genericAttributesId = new List<int>();

                foreach (var genAttrib in genericAttributes)
                {
                    var genericAttributeKeyColumn = EntityReflector.GetKeyColumn(genAttrib, false);

                    genericAttributesId.Add(int.Parse(
                    genericAttributeKeyColumn.GetValue(genAttrib, null).ToString()));

                    relationKeyColumn.SetValue(attributeInstance, int.Parse(SqlDefaultValue.False), null);
                }

                if (genericAttributesId.Count > 0)
                    attributeInstance = Get(attributeInstance, attributeInstance.GetType(), genericAttributesId, true);

                result = true;
            }

            return result;
        }

        private int setPersistenceAction(object entity, PropertyInfo entityKeyColumn)
        {
            return (entityKeyColumn.GetValue(entity, null).ToString().Equals(SqlDefaultValue.Zero))
                    ? (int)PersistenceAction.Create : (int)PersistenceAction.Edit;
        }

        private IList parseDatabaseReturn(XmlDocument databaseReturn, Type entityType)
        {
            Type dynamicListType = typeof(List<>).MakeGenericType(new Type[] { entityType });
            object returnList = Activator.CreateInstance(dynamicListType, true);
            object returnEntity = null;
            XmlNodeList elementList;

            elementList = databaseReturn.GetElementsByTagName("Table");

            var entityProps = entityType.GetProperties();

            foreach (XmlNode elem in elementList)
            {
                returnEntity = Activator.CreateInstance(entityType, true);

                foreach (PropertyInfo prop in entityProps)
                {
                    var propAttribs = prop.GetCustomAttributes(false);

                    var dataColumnAttrib = propAttribs.FirstOrDefault(ca => ca.GetType().Name.Equals("DataColumn")) as DataAnnotations.DataColumn;
                    var relColumnAttrib = propAttribs.FirstOrDefault(ca => ca.GetType().Name.Equals("RelationalColumn")) as DataAnnotations.RelationalColumn;

                    foreach (XmlNode childElem in elem.ChildNodes)
                    {
                        var setValue = (dataColumnAttrib != null) && dataColumnAttrib.ColumnName.Equals(childElem.Name);

                        setValue = setValue || ((relColumnAttrib != null)
                                               && ((relColumnAttrib.ColumnName.Equals(childElem.Name)
                                                  && (relColumnAttrib.ColumnAlias == null))
                                                     || ((relColumnAttrib.ColumnAlias != null)
                                                        && relColumnAttrib.ColumnAlias.Equals(childElem.Name))));

                        if (setValue)
                            prop.SetValue(returnEntity, formatSQLOutputValue(childElem.InnerText, prop.PropertyType), null);
                    }
                }

                ((IList)returnList).Add(returnEntity);
            }

            return (IList)returnList;
        }

        private Dictionary<object, object> getAnnotationValueList(object entity, Type entityType, PropertyInfo[] entityProperties, int action, List<int> primaryKeyFilters, out Dictionary<object, object> commandParameters)
        {
            var objectSQLDataRelation = new Dictionary<object, object>();
            commandParameters = new Dictionary<object, object>();

            objectSQLDataRelation.Add("Class", entityType.Name); // Não trocar por 'c'lass, pois conflitará com o tipo class nativo .NET

            object[] classAnnotations = entityType.GetCustomAttributes(true);

            var tableAnnotation = classAnnotations.FirstOrDefault(ant => ant.GetType().Name.Equals("DataTable"));
            if (tableAnnotation != null)
                objectSQLDataRelation.Add("dataTable", ((DataAnnotations.DataTable)tableAnnotation).TableName.ToLower());

            PropertyInfo primaryKeyAttribute = EntityReflector.GetKeyColumn(entity, false);

            foreach (var attrib in entityProperties)
            {
                object[] attributeAnnotations = attrib.GetCustomAttributes(true);

                foreach (object annotation in attributeAnnotations.Where(ca => ca is IDataColumn))
                {
                    object columnValue = attrib.GetValue(Convert.ChangeType(entity, entityType), null);
                    columnValue = formatSQLInputValue(columnValue, action, (IDataColumn)annotation, commandParameters);

                    if (annotation.GetType().Name.Equals("DataColumn"))
                    {
                        object sqlValueColumn = null;

                        var annotationRef = (DataAnnotations.DataColumn)annotation;

                        if (!(action == (int)PersistenceAction.Create
                            && (annotationRef).AutoNumbering))
                        {
                            if (primaryKeyFilters == null
                                || ((primaryKeyFilters != null) && !annotationRef.Filterable))
                            {
                                sqlValueColumn = new KeyValuePair<object, object>(annotationRef.ColumnName, columnValue);
                                objectSQLDataRelation.Add(attrib.Name, sqlValueColumn);
                            }
                            else
                            {
                                if (annotationRef.Filterable && annotationRef.MultipleFilters)
                                {
                                    string keysInterval = string.Empty;

                                    foreach (var filterKey in primaryKeyFilters)
                                        keysInterval += string.Concat(filterKey.ToString(), ",");

                                    sqlValueColumn = new KeyValuePair<object, object>(
                                                    (annotationRef).ColumnName,
                                                    keysInterval.Substring(0, keysInterval.Length - 1));

                                    objectSQLDataRelation.Add(attrib.Name, sqlValueColumn);
                                }
                            }
                        }
                    }
                    else if (annotation.GetType().Name.Equals("RelationalColumn")
                            && ((action == (int)PersistenceAction.List) || (action == (int)PersistenceAction.View)))
                    {
                        object sqlValueColumn = new KeyValuePair<object, object>((RelationalColumn)annotation, columnValue);
                        objectSQLDataRelation.Add(attrib.Name, sqlValueColumn);
                    }
                }
            }

            return objectSQLDataRelation;
        }

        private Dictionary<string, string> getSqlParameters(Dictionary<object, object> entitySqlData, int action, Dictionary<object, object> entitySqlFilter, string[] showAttributes, string keyColumnName, long entityHash, string hashColumnName, Dictionary<string, double[]> rangeValues, bool multipleFilters, bool getExclusion)
        {
            var returnDictionary = new Dictionary<string, string>();
            var relationshipDictionary = new Dictionary<string, string>();

            string tableName = string.Empty;
            string columnList = string.Empty;
            string valueList = string.Empty;
            string columnValueList = string.Empty;
            string columnFilterList = string.Empty;
            string relationList = string.Empty;
            string relation = string.Empty;
            bool rangeFilter = false;

            if (entitySqlData != null)
                foreach (var item in entitySqlData.Where(item => !item.Key.Equals("Class")))
                {
                    relation = string.Empty;

                    if (item.Key.Equals("dataTable"))
                    {
                        returnDictionary.Add(item.Key.ToString(), item.Value.ToString());
                        tableName = item.Value.ToString().ToLower();
                    }
                    else if (((KeyValuePair<object, object>)item.Value).Key is RelationalColumn)
                    {
                        RelationalColumn relationConfig = ((KeyValuePair<object, object>)item.Value).Key as RelationalColumn;

                        columnList += string.Format("{0}.{1} ", relationConfig.TableName.ToLower(), relationConfig.ColumnName);

                        if (!string.IsNullOrEmpty(relationConfig.ColumnAlias))
                            columnList += string.Format(SQLANSIRepository.DataPersistence_Action_ColumnName, relationConfig.ColumnAlias);

                        columnList += ", ";

                        if (relationConfig.JunctionType == RelationalJunctionType.Mandatory)
                        {
                            relation = string.Format(SQLANSIRepository.DataPersistence_Action_RelationateMandatorily,
                                                                    relationConfig.TableName.ToLower(),
                                                                    string.Concat(tableName, ".", relationConfig.KeyColumn),
                                                                    string.Concat(relationConfig.TableName.ToLower(), ".",
                                                                    relationConfig.ForeignKeyColumn));
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(relationConfig.IntermediaryColumnName))
                            {
                                relation = string.Format(SQLANSIRepository.DataPersistence_Action_RelationateOptionally,
                                                         relationConfig.IntermediaryColumnName.ToLower(),
                                                         string.Concat(tableName, ".", relationConfig.ForeignKeyColumn),
                                                         string.Concat(relationConfig.IntermediaryColumnName.ToLower(), ".",
                                                         relationConfig.ForeignKeyColumn));

                                relation += string.Format(SQLANSIRepository.DataPersistence_Action_RelationateOptionally,
                                                          relationConfig.TableName.ToLower(),
                                                          string.Concat(relationConfig.IntermediaryColumnName.ToLower(), ".", relationConfig.KeyColumn),
                                                          string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.ForeignKeyColumn));
                            }
                            else
                            {
                                relation = string.Format(SQLANSIRepository.DataPersistence_Action_RelationateOptionally,
                                                         relationConfig.TableName.ToLower(),
                                                         string.Concat(tableName, ".", relationConfig.ForeignKeyColumn),
                                                         string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.KeyColumn));
                            }

                            if (!string.IsNullOrEmpty(hashColumnName))
                                relation += string.Format(" AND {0} = {1} ", hashColumnName, entityHash);
                        }

                        if (relation.Contains(relationList)
                            || string.IsNullOrEmpty(relationList))
                            relationList = relation;
                        else if (!relationList.Contains(relation))
                            relationList += relation;
                    }
                    else if (item.Key.Equals("RelatedEntity"))
                    {

                    }
                    else
                    {
                        string entityAttributeName = item.Key.ToString();
                        object entityColumnName = ((KeyValuePair<object, object>)item.Value).Key;
                        object entityColumnValue = ((KeyValuePair<object, object>)item.Value).Value;

                        switch (action)
                        {
                            case (int)PersistenceAction.Create:
                                columnList += string.Format("{0}, ", entityColumnName);
                                valueList += string.Format("{0}, ", entityColumnValue);

                                break;
                            case (int)PersistenceAction.List:
                                if ((showAttributes != null) && (showAttributes.Length > 0))
                                    for (int vC = 0; vC < showAttributes.Length; vC++)
                                        showAttributes[vC] = showAttributes[vC].Trim();
                                if (((showAttributes == null) || (showAttributes.Length == 0))
                                    || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                                    columnList += string.Format("{0}.{1}, ", tableName, entityColumnName);

                                break;
                            case (int)PersistenceAction.View:
                                if (showAttributes.Length > 0)
                                    for (int vC = 0; vC < showAttributes.Length; vC++)
                                        showAttributes[vC] = showAttributes[vC].Trim();
                                if ((showAttributes.Length == 0)
                                    || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                                    columnList += string.Format("{0}.{1}, ", tableName, entityColumnName);

                                break;
                            default: // Alteração e Exclusão
                                if (!entityAttributeName.ToLower().Equals("id"))
                                {
                                    if (entityColumnValue == null)
                                        entityColumnValue = SqlDefaultValue.Null;

                                    columnValueList += string.Format("{0} = {1}, ", entityColumnName, entityColumnValue);
                                }

                                break;
                        }
                    }
                }

            if (entitySqlFilter != null)
            {
                foreach (var filter in entitySqlFilter)
                {
                    if ((!filter.Key.Equals("Class")) && (!filter.Key.Equals("dataTable"))
                        && (!filter.Key.Equals("RelatedEntity")))
                    {
                        object filterColumnName = null;
                        object filterColumnValue = null;
                        object columnName = null;
                        string columnNameStr = string.Empty;

                        if (!(((KeyValuePair<object, object>)filter.Value).Key is RelationalColumn))
                        {
                            columnName = ((KeyValuePair<object, object>)filter.Value).Key;
                            filterColumnName = string.Concat(tableName, ".", columnName);
                            filterColumnValue = ((KeyValuePair<object, object>)filter.Value).Value;
                        }
                        else
                        {
                            RelationalColumn relationConfig = ((KeyValuePair<object, object>)filter.Value).Key as RelationalColumn;

                            if ((action == (int)PersistenceAction.List) && relationConfig.Filterable)
                            {
                                filterColumnName = string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.ColumnName);
                                filterColumnValue = ((KeyValuePair<object, object>)filter.Value).Value;
                            }
                        }

                        if (rangeValues != null)
                        {
                            columnNameStr = columnName.ToString();
                            rangeFilter = rangeValues.ContainsKey(columnNameStr);
                        }

                        if (((filterColumnValue != null)
                                && (filterColumnValue.ToString() != SqlDefaultValue.Null)
                                && (filterColumnValue.ToString() != SqlDefaultValue.Zero))
                            || rangeFilter)
                        {
                            bool compareRule = (action == (int)PersistenceAction.List)
                                                   && !filterColumnName.ToString().ToLower().Contains("date")
                                                   && !filterColumnName.ToString().ToLower().Contains("hash")
                                                   && !filterColumnName.ToString().ToLower().StartsWith("id")
                                                   && !filterColumnName.ToString().ToLower().EndsWith("id")
                                                   && !filterColumnName.ToString().ToLower().Contains(".id");

                            string comparation = string.Empty;

                            if (!rangeFilter)
                            {
                                if (!multipleFilters)
                                    comparation = (compareRule)
                                                  ? string.Format(SqlOperator.Contains, filterColumnValue.ToString().Replace("'", string.Empty))
                                                  : string.Concat(SqlOperator.Equal, filterColumnValue);
                                else
                                {
                                    if (filterColumnValue.ToString().Contains(','))
                                    {
                                        comparation = string.Format(SqlOperator.In, filterColumnValue);
                                        if (getExclusion) comparation = string.Concat(SqlOperator.Not, comparation);
                                    }
                                    else
                                    {
                                        if (!getExclusion)
                                            comparation = string.Concat(SqlOperator.Equal, filterColumnValue);
                                        else
                                        {
                                            if (columnName.Equals(keyColumnName))
                                                comparation = string.Concat(SqlOperator.Different, filterColumnValue);
                                            else
                                                comparation = string.Concat(SqlOperator.Equal, filterColumnValue);
                                        }
                                    }
                                }

                                if (filterColumnValue.Equals(true))
                                    comparation = " = 1";

                                if ((action == (int)PersistenceAction.Edit) && filterColumnValue.Equals(false))
                                    comparation = " = 0";

                                if (!filterColumnValue.Equals(false))
                                    columnFilterList += filterColumnName + comparation +
                                        ((compareRule) ? SqlOperator.Or : SqlOperator.And);
                            }
                            else
                            {
                                double rangeFrom = rangeValues[columnNameStr][0];
                                double rangeTo = rangeValues[columnNameStr][1];

                                comparation = string.Format(SqlOperator.Between, rangeFrom, rangeTo);

                                columnFilterList += string.Concat(filterColumnName, " ", comparation, SqlOperator.And);
                            }
                        }
                    }
                }
            }

            if (action == (int)PersistenceAction.Create)
            {
                columnList = columnList.Substring(0, columnList.Length - 2);
                valueList = valueList.Substring(0, valueList.Length - 2);

                returnDictionary.Add("columnList", columnList);
                returnDictionary.Add("valueList", valueList);
            }
            else
            {
                if ((action == (int)PersistenceAction.List)
                    || (action == (int)PersistenceAction.View))
                {
                    columnList = columnList.Substring(0, columnList.Length - 2);
                    returnDictionary.Add("columnList", columnList);
                    returnDictionary.Add("relationList", relationList);
                }
                else
                    if (!string.IsNullOrEmpty(columnValueList))
                    {
                        columnValueList = columnValueList.Substring(0, columnValueList.Length - 2);

                        returnDictionary.Add("columnValueList", columnValueList);
                    }

                if (!string.IsNullOrEmpty(columnFilterList))
                {
                    var tokenRemove = (action == (int)PersistenceAction.List)
                                       ? SqlOperator.Or.Length
                                       : SqlOperator.And.Length;

                    columnFilterList = columnFilterList.Substring(0, columnFilterList.Length - tokenRemove);

                    returnDictionary.Add("columnFilterList", columnFilterList);
                }
                else
                    returnDictionary.Add("columnFilterList", "1 = 1");
            }

            return returnDictionary;
        }

        private Dictionary<object, object> getSqlProcParams(Dictionary<object, object> entitySqlData)
        {
            var returnDictionary = new Dictionary<object, object>();

            foreach (var prmVal in entitySqlData)
                if (prmVal.Value.GetType().Name.StartsWith("KeyValuePair"))
                {
                    var dicVal = (KeyValuePair<object, object>)prmVal.Value;

                    if (!dicVal.Value.ToString().Equals(SqlDefaultValue.FalseStr))
                        returnDictionary.Add(dicVal.Key, dicVal.Value);
                }

            return returnDictionary;
        }

        private string getMySqlProcParams(Dictionary<object, object> entitySqlData)
        {
            var sqlStr = new StringBuilder();
            var result = string.Empty;

            sqlStr.Append("(");
            foreach (var prmVal in entitySqlData)
                if (prmVal.Value.GetType().Name.StartsWith("KeyValuePair"))
                {
                    var dicVal = (KeyValuePair<object, object>)prmVal.Value;

                    if (!dicVal.Value.ToString().Equals(SqlDefaultValue.FalseStr))
                    {
                        var strVal = string.Concat(dicVal.Value, ", ");
                        sqlStr.Append(strVal);
                    }
                }

            result = sqlStr.ToString().Substring(0, sqlStr.Length - 2);
            result = string.Concat(result, ")");

            return result;
        }

        public void fillComposition(object loadedEntity, Type entityType)
        {
            RelatedEntity relationConfig = null;
            object attributeInstance = null;

            var attributeList = entityType.GetProperties()
                                          .Where(prp => prp.GetCustomAttributes(true)
                                                           .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach (var attribute in attributeList)
            {
                IEnumerable<object> attributeAnnotations = attribute.GetCustomAttributes(true)
                                                                    .Where(atb => atb.GetType().Name.Equals("RelatedEntity"));

                foreach (object annotation in attributeAnnotations)
                {
                    relationConfig = (RelatedEntity)annotation;

                    PropertyInfo foreignKeyColumn = null;
                    object foreignKeyValue = null;

                    PropertyInfo primaryKeyColumn = EntityReflector.GetKeyColumn(loadedEntity, false);

                    switch (relationConfig.Cardinality)
                    {
                        case RelationCardinality.OneToOne:

                            attributeInstance = Activator.CreateInstance(attribute.PropertyType, true);

                            foreignKeyColumn = loadedEntity.GetType().GetProperty(relationConfig.ForeignKeyAttribute);

                            foreignKeyValue = foreignKeyColumn.GetValue(loadedEntity, null);

                            if ((foreignKeyValue != null) && int.Parse(foreignKeyValue.ToString()) > 0)
                            {
                                var keyColumnAttribute = EntityReflector.GetKeyColumn(attributeInstance, false);

                                var keyAttributeAnnotation = keyColumnAttribute.GetCustomAttributes(true)
                                                                               .FirstOrDefault(ca => ca.GetType().Name.Equals("DataColumn")) as DataAnnotations.DataColumn;

                                keyColumnAttribute.SetValue(attributeInstance, foreignKeyColumn.GetValue(loadedEntity, null), null);

                                attributeInstance = Get(attributeInstance, attributeInstance.GetType(), null, false);
                            }

                            break;
                        case RelationCardinality.OneToMany:

                            attributeInstance = Activator.CreateInstance(attribute.PropertyType.GetGenericArguments()[0], true);

                            foreignKeyColumn = attributeInstance.GetType().GetProperty(relationConfig.ForeignKeyAttribute);
                            foreignKeyColumn.SetValue(attributeInstance, int.Parse(primaryKeyColumn.GetValue(loadedEntity, null).ToString()), null);

                            if (relationConfig.HashSigned)
                                setEntityHashKey(loadedEntity, attributeInstance);

                            attributeInstance = List(attributeInstance, attributeInstance.GetType(), null, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, false);

                            break;
                        case RelationCardinality.ManyToMany:

                            attributeInstance = Activator.CreateInstance(relationConfig.IntermediaryEntity, true);

                            if (attributeInstance != null)
                            {
                                setEntityForeignKey(loadedEntity, attributeInstance);
                                setEntityHashKey(loadedEntity, attributeInstance);

                                var manyToRelations = List(attributeInstance, attributeInstance.GetType(), null, 0, string.Empty, null, string.Empty, string.Empty, false, false, false, false, true);

                                Type childManyType = attribute.PropertyType.GetGenericArguments()[0];
                                Type dynamicManyType = typeof(List<>).MakeGenericType(new Type[] { childManyType });
                                IList childManyEntities = (IList)Activator.CreateInstance(dynamicManyType, true);

                                foreach (var rel in manyToRelations)
                                {
                                    var childManyKeyValue = rel.GetType().GetProperty(relationConfig.IntermediaryKeyAttribute).GetValue(rel, null);
                                    var childFilter = Activator.CreateInstance(childManyType);
                                    EntityReflector.GetKeyColumn(childFilter, false).SetValue(childFilter, childManyKeyValue, null);

                                    childManyEntities.Add(Get(childFilter, childFilter.GetType(), null, false));
                                }

                                attributeInstance = childManyEntities;
                            }
                            break;
                    }
                }

                if ((attributeInstance != null) && (attributeInstance.GetType().Name.Equals(attribute.PropertyType.Name)))
                    if (!attribute.PropertyType.Name.Contains("List"))
                        attribute.SetValue(loadedEntity, attributeInstance, null);
                    else
                        attribute.SetValue(loadedEntity, (IList)attributeInstance, null);
            }
        }

        private object formatSQLInputValue(object columnValue, int action, IDataColumn columnConfig, Dictionary<object, object> commandParameters)
        {
            if (columnValue != null)
            {
                switch (columnValue.GetType().ToString())
                {
                    case DataType.Short:
                        if (((short)columnValue == 0) && (!columnConfig.IsRequired()))
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case DataType.Integer:
                        if (((int)columnValue == 0) && ((!columnConfig.IsRequired()) || (action == (int)PersistenceAction.List)))
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case DataType.Long:
                        if (((long)columnValue == 0) && (!columnConfig.IsRequired()))
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case DataType.String:
                        columnValue = string.Concat("'", columnValue, "'");
                        break;
                    case DataType.DateTime:
                        if (!(((DateTime)columnValue).Equals(DateTime.MinValue)))
                            columnValue = "'" + ((DateTime)columnValue).ToString(DateTimeFormat.CompleteDateTime) + "'";
                        else
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case DataType.Binary:
                        commandParameters.Add(string.Concat("@", columnConfig.GetColumnName()),
                                             (byte[])columnValue);

                        columnValue = string.Concat("@", columnConfig.GetColumnName());

                        break;
                    case DataType.Float:
                        if (((float)columnValue == 0) && (!columnConfig.IsRequired()))
                            columnValue = SqlDefaultValue.Null;
                        else
                            columnValue = columnValue.ToString().Replace(",", ".");

                        break;
                    case DataType.Double:
                        if (((double)columnValue == 0) && (!columnConfig.IsRequired()))
                            columnValue = SqlDefaultValue.Null;
                        else
                            columnValue = columnValue.ToString().Replace(",", ".");

                        break;
                }
            }
            else
            {
                columnValue = (action == (int)PersistenceAction.Create) ? SqlDefaultValue.Null : null;
            }

            return columnValue;
        }

        private object formatSQLOutputValue(string columnValue, Type columnDataType)
        {
            object result = null;

            if (!string.IsNullOrEmpty(columnValue))
                if (!columnDataType.Name.Contains("Nullable"))
                {
                    if (columnDataType.FullName.Equals("System.Double")
                        || columnDataType.FullName.Equals("System.Decimal"))
                        columnValue = columnValue.Replace(".", ",");

                    result = Convert.ChangeType(columnValue, columnDataType);
                }
                else
                    result = Convert.ChangeType(columnValue, Nullable.GetUnderlyingType(columnDataType));

            return result;
        }

        private void validateListableAttributes(Type entityType, string showAttributes, out string[] exibitionAttributes)
        {
            IEnumerable<PropertyInfo> listableAttributes = null;
            bool notListableAttribute = false;
            string cultureAcronym = ConfigurationManager.AppSettings["RopSqlCulture"];

            listableAttributes = entityType.GetProperties().
                                            Where(prp => ((PropertyInfo)prp).GetCustomAttributes(true).
                                            Where(ca => ((ca is DataAnnotations.DataColumn) || (ca is RelationalColumn))
                                                     && ((IDataColumn)ca).IsListable()).Any());

            if (showAttributes == string.Empty)
            {
                exibitionAttributes = new string[listableAttributes.Count()];

                int cont = 0;
                foreach (var listableAttrib in listableAttributes)
                {
                    exibitionAttributes[cont] = listableAttrib.Name;
                    cont++;
                }
            }
            else
            {
                exibitionAttributes = showAttributes.Split(',');

                foreach (var attrib in exibitionAttributes)
                {
                    notListableAttribute = !listableAttributes.Contains(entityType.GetProperty(attrib));
                    if (!notListableAttribute) throw new AttributeNotListableException(cultureAcronym); break;
                }
            }
        }

        private long getEntityHashCode(object entity)
        {
            long result = 0;

            var hashSignature = entity.GetType().GetCustomAttributes(false)
                                      .SingleOrDefault(cln => cln is HashSignature) as HashSignature;

            if (hashSignature != null)
                result = hashSignature.HashCode;

            return result;
        }

        private string getChildHashColumnName(PropertyInfo[] entityProperties)
        {
            string result = null;

            var dataProps = entityProperties.Where(prp => prp.GetCustomAttributes(false)
                                            .Any(an => an.GetType().Name.Equals("DataColumn")));

            if (dataProps != null)
            {
                foreach (var prop in dataProps)
                {
                    var hashColumn = prop.GetCustomAttributes(false).FirstOrDefault() as DataAnnotations.DataColumn;

                    if ((hashColumn != null) && hashColumn.IsHashSignature())
                    {
                        result = prop.Name;
                        break;
                    }
                }
            }

            return result;
        }

        private string getEntityHashColumnName(PropertyInfo[] entityProperties)
        {
            string result = string.Empty;

            var relProps = entityProperties.Where(prp => prp.GetCustomAttributes(false)
                                           .Any(an => an.GetType().Name.Equals("RelationalColumn")));

            if (relProps != null)
            {
                foreach (var prop in relProps)
                {
                    var hashColumn = prop.GetCustomAttributes(false).FirstOrDefault() as RelationalColumn;

                    if (hashColumn != null)
                        result = hashColumn.GetHashColumn();

                    if (!string.IsNullOrEmpty(result))
                        break;
                }
            }

            return result;
        }

        private DataAnnotations.DataTable getTableAttrib(object entity)
        {
            return entity.GetType().GetCustomAttributes(false)
                                   .SingleOrDefault(atb => atb is DataAnnotations.DataTable) as DataAnnotations.DataTable;
        }

        private DataAnnotations.DataProcedure getProcAttrib(object entity)
        {
            return entity.GetType().GetCustomAttributes(false)
                                   .SingleOrDefault(atb => atb is DataAnnotations.DataProcedure) as DataAnnotations.DataProcedure;
        }

        private object parseManyToRelation(object childEntity, RelatedEntity relation)
        {
            object result = null;
            var relEntity = relation.IntermediaryEntity;

            if (relEntity != null)
            {
                var interEntity = Activator.CreateInstance(relation.IntermediaryEntity);

                var childKey = EntityReflector.GetKeyColumn(childEntity, false);
                var interKeyAttrib = interEntity.GetType().GetProperties()
                                                .FirstOrDefault(atb => atb.Name.Equals(relation.IntermediaryKeyAttribute));

                interKeyAttrib.SetValue(interEntity, childKey.GetValue(childEntity, null), null);

                result = interEntity;
            }

            return result;
        }

        private void parseCompositionAsync(object param)
        {
            Thread.Sleep(700);

            ParallelParam parallelParam = param as ParallelParam;

            object entity = parallelParam.Param1;
            Type entityType = parallelParam.Param2 as Type;
            int action = int.Parse(parallelParam.Param3.ToString());
            Dictionary<object, object> commandParameters = parallelParam.Param4 as Dictionary<object, object>;
            object filterEntity = parallelParam.Param5;

            try
            {
                List<string> childEntityCommands = parseComposition(entity, entityType, action, filterEntity);

                if (base.connection.State == ConnectionState.Closed)
                    base.connect();

                base.StartTransaction();

                foreach (var cmd in childEntityCommands)
                    executeCommand(cmd, commandParameters);

                base.CommitTransaction();

                if (!keepConnection) base.disconnect();

                // Atualizacao do Cache

                DataCache.Del(entity, true);
            }
            catch (Exception)
            {
                if (base.transactionControl != null)
                    base.CancelTransaction();

                if (base.connection.State == ConnectionState.Open)
                    Delete(filterEntity, filterEntity.GetType());

                throw;
            }
        }

        #endregion
    }

    public static class DataType
    {
        #region Declarations

        public const string Short = "System.Int16";
        public const string Integer = "System.Int32";
        public const string Long = "System.Int64";
        public const string String = "System.String";
        public const string DateTime = "System.DateTime";
        public const string Binary = "System.Byte[]";
        public const string Float = "System.Single";
        public const string Double = "System.Double";

        #endregion
    }

    public static class SqlDefaultValue
    {
        #region Declarations

        public const string Null = "NULL";
        public const string Zero = "0";
        public const string False = "-1";
        public const string FalseStr = "False";

        #endregion
    }

    public static class SqlOperator
    {
        #region Declarations

        public const string Equal = " = ";
        public const string Different = " <> ";
        public const string Contains = " LIKE '%{0}%' ";
        public const string And = " AND ";
        public const string Or = " OR ";
        public const string Major = " > ";
        public const string MajorOrEqual = " >= ";
        public const string Less = " < ";
        public const string LessOrEqual = " <= ";
        public const string IsNull = " IS NULL ";
        public const string In = " IN ({0})";
        public const string Between = "BETWEEN {0} AND {1}";
        public const string Not = " NOT ";

        #endregion
    }
    public static class DateTimeFormat
    {
        #region Declarations

        public const string CompleteDateTime = "yyyy-MM-dd HH:mm:ss";
        public const string NormalDate = "yyyy-MM-dd";
        public const string ShortDate = "yy-MM-dd";

        #endregion
    }

    public enum PersistenceAction
    {
        #region Declarations

        Create = 1,
        Edit = 2,
        Delete = 3,
        List = 4,
        View = 5

        #endregion
    }
}
