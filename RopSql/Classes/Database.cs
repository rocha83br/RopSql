using System;
using System.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.RopSql.Resources;
using System.Data.RopSql.Exceptions;
using System.Security.InMemProfile;
using System.Xml;
using System.Text;

namespace System.Data.RopSql
{
    public abstract class DataBase
    {
        #region Declarations

        protected string connectionConfig;
        protected string cultureAcronym;

        #endregion

        #region Constructors

        protected DataBase()
        {
            using (var crypto = new Encrypter())
            {
                connectionConfig = crypto.DecryptText(ConfigurationManager.ConnectionStrings["RopSqlConnStr"].ConnectionString);
            }

            cultureAcronym = ConfigurationManager.AppSettings["RopSqlCulture"];
        }

        #endregion
    }
}
