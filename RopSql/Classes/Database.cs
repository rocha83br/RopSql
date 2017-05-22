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

        string replicaConn;

        protected string connectionConfig;
        protected string[] replicaConnectionsConfig;
        protected string cultureAcronym;
        protected string logPath;

        #endregion

        #region Public Properties

        protected bool replicationEnabled
        {
            get 
            {
                return (replicaConnectionsConfig.Length > 0);
            }
        }

        #endregion

        #region Constructors

        protected DataBase()
        {
            using (var crypto = new Encrypter())
            {
                connectionConfig = crypto.DecryptText(ConfigurationManager.ConnectionStrings["RopSqlConnStr"].ConnectionString);

                if (ConfigurationManager.ConnectionStrings["RopSqlReplicaConnStr"] != null)
                    replicaConn = crypto.DecryptText(ConfigurationManager.ConnectionStrings["RopSqlReplicaConnStr"].ConnectionString);
            }

            if (!string.IsNullOrWhiteSpace(replicaConn))
                replicaConnectionsConfig = replicaConn.Split('|');

            cultureAcronym = ConfigurationManager.AppSettings["RopSqlCulture"];
            logPath = ConfigurationManager.AppSettings["RopSqlLogPath"];
        }

        #endregion
    }
}
