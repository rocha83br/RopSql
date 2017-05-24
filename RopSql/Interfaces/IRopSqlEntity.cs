using System;

namespace System.Data.RopSql.Interfaces
{
    public interface IRopSqlEntity
    {
        int Id { get; set; }
        int MaxId { get; set; }
        int Count { get; set; }
    }
}
