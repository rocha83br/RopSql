using System;
using System.Xml;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Configuration;
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
            base.connect();
        }

        #endregion

        #region Public Methods

        public int Create(object entity, Type entityType, bool persistComposition)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters;
            int lastInsertedId = 0;
            List<string> childEntityCommands = null;

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(entity, entityType), 
                                                  entityType,
                                                  (int)PersistenceAction.Create, 
                                                  null, null,
                                                  false,
                                                  emptyArray,
                                                  out commandParameters);
                 
                lastInsertedId = base.executeCommand(sqlInstruction, commandParameters);

                if (persistComposition)
                {
                    var entityColumnKey = getKeyColumn(entity, false);

                    if (entityColumnKey != null)
                        entityColumnKey.SetValue(entity, lastInsertedId, null);

                    childEntityCommands = parseComposition(entity, entityType, (int)PersistenceAction.Create, null);

                    foreach (var cmd in childEntityCommands)
                        executeCommand(cmd, commandParameters);
                }
                
                if (!keepConnection) base.disconnect();
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
                                                     entityType, (int)PersistenceAction.Edit, 
                                                     filterEntity, null, false, emptyArray, out commandParameters);

                recordsAffected = executeCommand(sqlInstruction, commandParameters);
            }

            if (persistComposition)
            {
                childEntityCommands = parseComposition(entity, entityType, (int)PersistenceAction.Edit, filterEntity);

                foreach (var cmd in childEntityCommands)
                    recordsAffected += executeCommand(cmd, commandParameters);
            }

            if (!keepConnection) base.disconnect();

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
                                             out commandParameters);

                recordAffected = executeCommand(sqlInstruction, commandParameters);
            }

            if (!keepConnection) base.disconnect();

            return recordAffected;
		}

        public object Get(object filterEntity, Type entityType, List<int> primaryKeyFilters, bool loadComposition)
        {
            object returnEntity = null;
            
            var queryList = List(filterEntity, entityType, primaryKeyFilters, 0, 
                                       string.Empty, string.Empty, string.Empty, 
                                       false, false, false, true, loadComposition);

            if (queryList.Count > 0) returnEntity = queryList[0];
            
            return returnEntity;
		}

        public List<T> List<T>(object filterEntity, Type entityType, List<int> primaryKeyFilters, int recordLimit, string showAttributes, string groupAttributes, string orderAttributes, bool onlyListableAttributes, bool getExclusion, bool orderDescending, bool uniqueQuery, bool loadComposition)
        {
            var result = List(filterEntity, entityType, primaryKeyFilters, recordLimit, showAttributes, groupAttributes, orderAttributes, onlyListableAttributes, getExclusion, orderDescending, uniqueQuery, loadComposition);

            return result as List<T>;
        }

        public IList List(object filterEntity, Type entityType, List<int> primaryKeyFilters, int recordLimit, string showAttributes, string groupAttributes, string orderAttributes, bool onlyListableAttributes, bool getExclusion, bool orderDescending, bool uniqueQuery, bool loadComposition)
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
                                         out commandParameters);

            sqlInstruction = string.Format(sqlInstruction, recordLimit > 0 ? string.Format(SQLANSIRepository.DataPersistence_Action_LimitResult_MySQL, recordLimit) : string.Empty, "{0}", "{1}");

            if (!string.IsNullOrEmpty(groupAttributes))
            {
                string complementaryColumnList = string.Empty;

                groupingAttributes = groupAttributes.Split(',');

                for (int cont = 0; cont < groupingAttributes.Length; cont++)
                    groupingAttributes[cont] = groupingAttributes[cont].Trim();

                attributeColumnRelation = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, persistenceAction, null, out commandParameters);

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
                attributeColumnRelation = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, persistenceAction, null, out commandParameters);

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

            if(!keepConnection) base.disconnect();

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

        private string parseEntity(object entity, Type entityType, int action, object filterEntity, List<int> primaryKeyFilters, bool getExclusion, string[] showAttributes, out Dictionary<object, object> commandParameters)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> sqlFilterData;

            commandParameters = null;

            Dictionary<object, object> sqlEntityData = getAnnotationValueList(Convert.ChangeType(entity, entityType), entityType, action, primaryKeyFilters, out commandParameters);

            if (filterEntity != null)
                sqlFilterData = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, action, primaryKeyFilters, out commandParameters);
            else
                sqlFilterData = null;

            var keyColumn = getKeyColumn(entity, false).GetCustomAttributes(true)
                                                       .FirstOrDefault(cln => cln is DataAnnotations.DataColumn 
                                                                           && ((IDataColumn)cln).IsPrimaryKey()) as DataAnnotations.DataColumn;
            string keyColumnName = string.Empty;
            if (keyColumn != null)
                keyColumnName = keyColumn.ColumnName;

            var entityHash = entityType.GetCustomAttributes(true)
                                       .FirstOrDefault(cln => cln is HashSignature) as HashSignature;
            var hashCode = entityHash != null ? entityHash.HashCode : 0;

            var hashColumn = entityType.GetProperties().FirstOrDefault(prp => prp.GetCustomAttributes(true)
                                                       .Any(an => an.GetType().GetInterface("IDataColumn") != null)
                                                            && ((IDataColumn)(prp.GetCustomAttributes(true)
                                                       .FirstOrDefault(an => an.GetType().GetInterface("IDataColumn") != null))).IsHashSignature());

            Dictionary<string, string> sqlParameters = getSqlParameters(sqlEntityData, action, sqlFilterData,
                                                                        showAttributes, keyColumnName, hashCode, 
                                                                        (hashColumn != null) ? hashColumn.Name : string.Empty, 
                                                                        (primaryKeyFilters != null), getExclusion);

            switch(action)
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
                                                                                .Any(an => an.GetType().Name.Equals("RelatedEntity")));

            foreach (PropertyInfo child in childEntities)
            {
                childEntityInstance = child.GetValue(entity, null);
                object childEntityFilter = null;

                var entityParent = (action != (int)PersistenceAction.Edit) ? entity : filterEntity;

                if (childEntityInstance != null)
                {
                    if (!childEntityInstance.GetType().Name.Contains("List"))
                    {
                        action = setPersistenceAction(childEntityInstance, getKeyColumn(filterEntity, false));
                        childEntityFilter = Activator.CreateInstance(childEntityInstance.GetType());

                        if (action == (int)PersistenceAction.Edit)
                            migrateEntityPrimaryKey(childEntityInstance, childEntityFilter);

                        setEntityHashKey(entityParent, child);

                        result.Add(parseEntity(childEntityInstance, childEntityInstance.GetType(), action, childEntityFilter, null, false, null, out commandParameters));
                    }
                    else
                    {
                        var childListInstance = (IList)childEntityInstance;
                        List<object> childFiltersList = new List<object>();

                        foreach (var listItem in childListInstance)
                        {
                            action = setPersistenceAction(listItem, getKeyColumn(listItem, false));
                            childEntityFilter = Activator.CreateInstance(listItem.GetType());

                            if (action == (int)PersistenceAction.Edit)
                            {
                                migrateEntityPrimaryKey(listItem, childEntityFilter);
                                childFiltersList.Add(childEntityFilter);
                            }

                            setEntityHashKey(entityParent, listItem);

                            result.Add(parseEntity(listItem, listItem.GetType(), action, childEntityFilter, null, false, null, out commandParameters));
                        }

                        if (childFiltersList.Count > 0)
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
            RopSqlDataAdapter compositionFilter = null;

            if (existentComposition.Count > 0)
            {
                compositionFilter = Activator.CreateInstance(existentComposition[0].GetType()) as RopSqlDataAdapter;
                setEntityHashKey(entity, compositionFilter);
            }

            foreach (var composit in existentComposition)
            {
                var compositKeyColumn = getKeyColumn(composit, false);
                existentKeys.Add(int.Parse(compositKeyColumn.GetValue(composit, null).ToString()));
            }

            Dictionary<object, object> commandParameters;
            
            string result = string.Empty;
            if (compositionFilter != null)
                result = parseEntity(compositionFilter, compositionFilter.GetType(), (int)PersistenceAction.Delete, compositionFilter, existentKeys, true, null, out commandParameters);

            return result;
        }

        private bool setEntityHashKey(object parentEntity, object childEntity)
        {
            bool result = false;
            HashSignature parentHashAnnotation = null;

            var entityKeyColumn = getKeyColumn(parentEntity, false);

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
                var relationKeyColumn = getKeyColumn(attributeInstance, false);

                setEntityHashKey(loadedEntity, genericAttributeInstance);

                var genericAttributes = List(genericAttributeInstance, genericAttributeInstance.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, false);

                genericAttributesId = new List<int>();

                foreach (var genAttrib in genericAttributes)
                {
                    var genericAttributeKeyColumn = getKeyColumn(genAttrib, false);

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

        private void migrateEntityPrimaryKey(object entity, object filterEntity)
        {
            var entityKeyColumn = getKeyColumn(entity, false);

            if (entityKeyColumn != null)
            {
                var valorChave = entityKeyColumn.GetValue(entity, null);
                entityKeyColumn.SetValue(filterEntity, valorChave, null);
                entityKeyColumn.SetValue(entity, 0, null);
            }
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

            foreach (XmlNode elem in elementList)
            {
                returnEntity = Activator.CreateInstance(entityType, true);

                foreach (XmlNode childElem in elem.ChildNodes)
                {
                    foreach (PropertyInfo prop in entityType.GetProperties())
                    {
                        if (prop.GetCustomAttributes(false).FirstOrDefault(ca => (ca is DataAnnotations.DataColumn
                                                                                      && ((DataAnnotations.DataColumn)ca).ColumnName.Equals(childElem.Name))
                                                                                      || (ca is RelationalColumn
                                                                                             && (((((RelationalColumn)ca).ColumnName.Equals(childElem.Name)
                                                                                                  && ((RelationalColumn)ca).ColumnAlias == null))
                                                                                                 || (((RelationalColumn)ca).ColumnAlias != null
                                                                                                 && ((RelationalColumn)ca).ColumnAlias.Equals(childElem.Name))))) != null)
                            
                            prop.SetValue(returnEntity, formatSQLOutputValue(childElem.InnerText, prop.PropertyType), null);
                    }
                }

                ((IList)returnList).Add(returnEntity);
            }

            return (IList)returnList;
        }

        private Dictionary<object, object> getAnnotationValueList(object entity, Type entityType, int action, List<int> primaryKeyFilters, out Dictionary<object, object> commandParameters)
        {
            var objectSQLDataRelation = new Dictionary<object, object>();
            commandParameters = new Dictionary<object, object>();

            objectSQLDataRelation.Add("Class", entityType.Name);

            object[] classAnnotations = entityType.GetCustomAttributes(true);

            var tableAnnotation = classAnnotations.FirstOrDefault(ant => ant.GetType().Name.Equals("DataTable"));
            if (tableAnnotation != null)
                objectSQLDataRelation.Add("dataTable", ((DataAnnotations.DataTable)tableAnnotation).TableName.ToLower());

            PropertyInfo primaryKeyAttribute = getKeyColumn(entity, false);

            PropertyInfo[] attributeList = entityType.GetProperties();

            foreach (var attrib in attributeList)
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

                                    foreach (var filtroChave in primaryKeyFilters)
                                        keysInterval += string.Concat(filtroChave.ToString(), ",");

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

        private Dictionary<string, string> getSqlParameters(Dictionary<object, object> entitySqlData, int action, Dictionary<object, object> entitySqlFilter, string[] showAttributes, string keyColumnName, long entityHash, string hashColumnName, bool multipleFilters, bool getExclusion)
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

                            if (relationConfig.ConnectHash)
                                relation += string.Format(" AND {0} = {1} ", hashColumnName, entityHash);
                        }
                        else
                        {
                            relation = string.Format(SQLANSIRepository.DataPersistence_Action_RelationateOptionally,
                                                     relationConfig.TableName.ToLower(),
                                                     string.Concat(tableName, ".", relationConfig.ForeignKeyColumn),
                                                     string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.KeyColumn));
                        }
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
                            if (showAttributes.Length > 0)
                                for (int vC = 0; vC < showAttributes.Length; vC++)
                                    showAttributes[vC] = showAttributes[vC].Trim();
                            if ((showAttributes.Length == 0)
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
                            if ((entityColumnValue != null)
                                && (entityColumnValue.ToString() != SqlDefaultValue.Zero)
                                && (entityColumnValue.ToString() != SqlDefaultValue.Null))
                                columnValueList += string.Format("{0} = {1}, ", entityColumnName, entityColumnValue);
                            
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

                        if ((filterColumnValue != null)
                                && (filterColumnValue.ToString() != SqlDefaultValue.Null)
                                && (filterColumnValue.ToString() != SqlDefaultValue.Zero))
                        {
                            string comparation = string.Empty;

                            if (!multipleFilters)
                                comparation = ((action == (int)PersistenceAction.List) && !filterColumnName.ToString().Contains("DT_"))
                                              ? string.Format(SqlOperator.Contains, filterColumnValue.ToString().Replace("'", string.Empty))
                                              : string.Concat(SqlOperator.Equal, filterColumnValue);
                            else
                            {
                                if (filterColumnValue.ToString().Contains(','))
                                {
                                    comparation = string.Format(SqlOperator.And, filterColumnValue);
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

                            if (!(filterColumnName.ToString().EndsWith(".Active") && filterColumnValue.Equals(false)))
                                columnFilterList += filterColumnName + comparation +
                                    ((action == (int)PersistenceAction.List) ? SqlOperator.Or : SqlOperator.And);
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

        public void fillComposition(object loadedEntity, Type entityType)
        {
            RelatedEntity relationConfig = null;
            object attributeInstance = null;

            var attributeList = entityType.GetProperties()
                                          .Where(prp => prp.GetCustomAttributes(true)
                                                           .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach(var attribute in attributeList)
            {
                IEnumerable<object> attributeAnnotations = attribute.GetCustomAttributes(true)
                                                                    .Where(atb => atb.GetType().Name.Equals("RelatedEntity"));

                foreach (object annotation in attributeAnnotations)
                {
                    relationConfig = (RelatedEntity)annotation;

                    PropertyInfo foreignKeyColumn = null;

                    PropertyInfo primaryKeyColumn = getKeyColumn(loadedEntity, false);

                    switch (relationConfig.Cardinality)
                    {
                        case RelationCardinality.OneToOne :

                            attributeInstance = Activator.CreateInstance(attribute.PropertyType, true);

                            foreignKeyColumn = loadedEntity.GetType().GetProperty(relationConfig.ForeignKeyAttribute);

                            if (int.Parse(foreignKeyColumn.GetValue(loadedEntity, null).ToString()) > 0)
                            {
                                var keyColumnAttribute = getKeyColumn(attributeInstance, false);

                                var keyAttributeAnnotation = keyColumnAttribute.GetCustomAttributes(true)
                                                                               .FirstOrDefault(ca => ca.GetType().Name.Equals("DataColumn")) as DataAnnotations.DataColumn;

                                keyColumnAttribute.SetValue(attributeInstance, foreignKeyColumn.GetValue(loadedEntity, null), null);

                                attributeInstance = Get(attributeInstance, attributeInstance.GetType(), null, false);
                            }

                            break;
                        case RelationCardinality.OneToMuch :

                            attributeInstance = Activator.CreateInstance(attribute.PropertyType.GetGenericArguments()[0], true);

                            foreignKeyColumn = attributeInstance.GetType().GetProperty(relationConfig.ForeignKeyAttribute);
                            foreignKeyColumn.SetValue(attributeInstance, int.Parse(primaryKeyColumn.GetValue(loadedEntity, null).ToString()), null);

                            attributeInstance = List(attributeInstance, attributeInstance.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, false);

                            break;
                        case RelationCardinality.MuchToMuch :

                            attributeInstance = Activator.CreateInstance(attribute.PropertyType, true);

                            if (attributeInstance != null)
                            {
                                if (int.Parse(primaryKeyColumn.GetValue(loadedEntity, null).ToString()) > 0)
                                {
                                    if (!attributeInstance.GetType().Name.Contains("List"))
                                    {
                                        setEntityHashKey(loadedEntity, attributeInstance);
                                        replicateChildHashes(loadedEntity, attributeInstance);
                                    }
                                    else
                                    {
                                        var childItemInstance = Activator.CreateInstance(attribute.PropertyType.GetGenericArguments()[0], true);

                                        setEntityHashKey(loadedEntity, childItemInstance);
                                        replicateChildHashes(loadedEntity, childItemInstance);

                                        attributeInstance = List(childItemInstance, childItemInstance.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, true, true);
                                    }
                                }
                                else
                                    attributeInstance = List(attributeInstance, attributeInstance.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, true);
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
                result = Convert.ChangeType(columnValue, columnDataType);

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

        private PropertyInfo getKeyColumn(object entity, bool foreignKey)
        {
            PropertyInfo entityKeyColumn = null;

            if (!foreignKey)
                entityKeyColumn = entity.GetType().GetProperties().FirstOrDefault(fd =>
                                                   (fd.GetCustomAttributes(true).Any(ca =>
                                                   (ca.GetType().Name.Equals("DataColumn")
                                                    && ((DataAnnotations.DataColumn)ca).IsPrimaryKey()))));
            else
                entityKeyColumn = entity.GetType().GetProperties().LastOrDefault(fd =>
                                                   (fd.GetCustomAttributes(true).Any(ca =>
                                                   (ca.GetType().Name.Equals("DataColumn")
                                                       && ((DataAnnotations.DataColumn)ca).IsPrimaryKey()))));

            return entityKeyColumn;
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

        #endregion
    }

    public static class SqlDefaultValue
    {
        #region Declarations

        public const string Null = "NULL";
        public const string Zero = "0";
        public const string False = "-1";

        #endregion
    }

    public static class SqlOperator
    {
        #region Declarations

        public const string Equal = " = ";
        public const string Different = " <> ";
        public const string Contains = " LIKE '%{0}%' ";
        public const string And = " AND ";
        public const string Or  = " OR ";
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

        public const string CompleteDateTime = "yyyy-MM-dd hh:mm:ss";
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
