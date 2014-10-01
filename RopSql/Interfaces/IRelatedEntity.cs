using System;
using System.Collections.Generic;
using System.Data.RopSql.DataAnnotations;
using System.Linq;
using System.Text;

namespace System.Data.RopSql.Interfaces
{
    interface IRelatedEntity
    {
        RelationCardinality GetRelationCardinality();
        Type GetIntermediaryEntity();
        string GetIntermediaryKeyAttribute();
    }
}
