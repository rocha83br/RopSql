using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Data.RopSql.Interfaces;

namespace System.Data.RopSql.DataAnnotations
{
    public class HashSignature : Attribute
    {
        public long HashCode;
    }

    public class DataTable : Attribute
    {
        public string TableName;
    }

    public class DataColumn : Attribute, IDataColumn
    {
        #region Declarations

        public bool PrimaryKey;
        public bool ForeignKey;
        public bool AutoNumbering;
        public bool Required;
        public bool Listable;
        public bool Filterable;
        public bool MultipleFilters;
        public bool ListLabel;
        public string ColumnName;

        public bool IsPrimaryKey()
        {
            return PrimaryKey;
        }

        public bool IsRequired()
        {
            return Required;
        }

        public bool IsForeignKey()
        {
            return ForeignKey;
        }

        public bool IsListable()
        {
            return Listable;
        }

        public bool IsFilterable()
        {
            return Filterable;
        }

        public bool EhRequired()
        {
            return Required;
        }

        public bool IsListLabel()
        {
            return ListLabel;
        }

        public string GetColumnName()
        {
            return ColumnName;
        }

        #endregion

        #region Public Methods

        public static PropertyInfo GetKeyColumn(Type entityType)
        {
            return entityType.GetProperties().FirstOrDefault(prp => prp.GetCustomAttributes(true)
                                                                       .Any(atb => (atb is DataColumn)
                                                                                && ((IDataColumn)atb).IsPrimaryKey()));
        }

        public static IEnumerable<PropertyInfo> GetListableColumns(Type entityType)
        {
            return entityType.GetProperties().Where(prp => prp.GetCustomAttributes(true)
                                                              .Any(atb => (atb is DataColumn || atb is RelationalColumn)
                                                                       && (((IDataColumn)atb).IsPrimaryKey()
                                                                       || ((IDataColumn)atb).IsListable())));
        }

        public static IEnumerable<PropertyInfo> GetListLabelColumns(Type entityType)
        {
            IEnumerable<PropertyInfo> labelColumns = null;

            labelColumns = entityType.GetProperties().Where(prp => prp.GetCustomAttributes(true)
                                                                   .Any(atb => (atb is DataColumn || atb is RelationalColumn)
                                                                            && ((IDataColumn)atb).IsListLabel()));
            return labelColumns;
        }

        public static PropertyInfo GetListLabelColumn(Type entityType)
        {
            return GetListLabelColumns(entityType).FirstOrDefault();
        }

        public static string GetKeyValue(object entity, PropertyInfo keyColumn)
        {
            string result = string.Empty;

            result = string.Concat(keyColumn.GetValue(entity, null).ToString());

            return result;
        }

        public static string GetListLabelValues(object entity, IEnumerable<PropertyInfo> labelColumns)
        {
            string result = string.Empty;

            foreach (var col in labelColumns)
                result += string.Concat(col.GetValue(entity, null).ToString(), " ");

            return result;
        }

        #endregion
    }

    public enum RelationalJunctionType
    {
        Mandatory = 1,
        Optional = 2
    }

    public class RelationalColumn : Attribute, IDataColumn
    {
        #region Declarations

        public string TableName;
        public string IntermediaryColumnName;
        public string ColumnName;
        public string ColumnAlias;
        public string KeyColumn;
        public string ForeignKeyColumn;
        public string IntermediaryColumnKey;
        public RelationalJunctionType JunctionType;
        public bool Listable;
        public bool Filterable;
        public bool ListLabel;
        public bool ConnectHash;

        public bool IsPrimaryKey()
        {
            return false;
        }

        public bool IsRequired()
        {
            return false;
        }

        public bool IsListable()
        {
            return Listable;
        }

        public bool IsFilterable()
        {
            return Filterable;
        }

        public bool IsListLabel()
        {
            return ListLabel;
        }

        public string GetColumnName()
        {
            return ColumnName;
        }

        #endregion
    }

    public enum DataAggregationType
    {
        Sum = 1,
        Count = 2,
        Minimum = 3,
        Maximum = 4,
        Average = 5
    }

    public class DataAggregationColumn : Attribute
    {
        public string ColumnName;
        public string ColumnAlias;
        public DataAggregationType AggregationType;
    }

    public enum RelationCardinality
    {
        OneToOne = 1,
        OneToMuch = 2,
        MuchToMuch = 3
    }

    public class RelatedEntity : Attribute 
    {
        public RelationCardinality Cardinality; 
        public string ForeignKeyAttribute;
        public bool RecordableComposition = false;
    }

    public class AttributeFormat : Attribute
    {
        public string InFormat = string.Empty;
        public string OutFormat = string.Empty;
    }
}
