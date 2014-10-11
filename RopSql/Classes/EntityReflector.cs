using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace System.Data.RopSql
{
    public static class EntityReflector
    {
        public static PropertyInfo GetKeyColumn(object entity, bool foreignKey, string parentName = "")
        {
            PropertyInfo entityKeyColumn = null;

            if (!foreignKey)
                entityKeyColumn = entity.GetType().GetProperties().FirstOrDefault(fd =>
                                                   (fd.GetCustomAttributes(true).Any(ca =>
                                                   (ca.GetType().Name.Equals("DataColumn")
                                                    && ((DataAnnotations.DataColumn)ca).IsPrimaryKey()))));
            else
                entityKeyColumn = entity.GetType().GetProperties().FirstOrDefault(fd =>
                                                   fd.Name.Contains(parentName)
                                                   && (fd.GetCustomAttributes(true).Any(ca =>
                                                   (ca.GetType().Name.Equals("DataColumn")
                                                    && ((DataAnnotations.DataColumn)ca).IsForeignKey()))));

            return entityKeyColumn;
        }

        public static void MigrateEntityPrimaryKey(object entity, object filterEntity)
        {
            var entityKeyColumn = GetKeyColumn(entity, false);

            if (entityKeyColumn != null)
            {
                var keyValue = entityKeyColumn.GetValue(entity, null);
                entityKeyColumn.SetValue(filterEntity, keyValue, null);
                entityKeyColumn.SetValue(entity, 0, null);
            }
        }

        public static bool MatchKeys(object sourceEntity, object destinEntity)
        {
            var entityKey = GetKeyColumn(sourceEntity, false);

            return entityKey.GetValue(sourceEntity, null)
                   .Equals(entityKey.GetValue(destinEntity, null));
        }

    }
}
