using System;
using System.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.RopSql.Resources;
using System.Data.RopSql.Exceptions;
using System.Xml;
using System.Text;
using MySqlConnector;

namespace System.Data.RopSql
{
    public class DataBaseMySqlConnection : DataBase, IDisposable
    {
        #region Declarations

        protected MySqlConnection connection;
        protected MySqlTransaction transactionControl;

        #endregion

        #region Constructors

        protected DataBaseMySqlConnection() : base()
        {

        }

        #endregion

        #region Public Methods

        public void StartTransaction()
        {
            if (connection.State != System.Data.ConnectionState.Open)
                connect();

            this.transactionControl = connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if ((connection.State == System.Data.ConnectionState.Open)
                && (this.transactionControl != null))
                this.transactionControl.Commit();
        }

        public void CancelTransaction()
        {
            if ((connection.State == System.Data.ConnectionState.Open)
                && (this.transactionControl != null))
                this.transactionControl.Rollback();
        }

        public void Dispose()
        {
            connection.Dispose();

            if (transactionControl != null)
                transactionControl.Dispose();

            GC.ReRegisterForFinalize(this);
        }

        #endregion

        #region Helper Methods

        protected bool connect(string optionalConnConfig = "")
        {
            if (!string.IsNullOrEmpty(this.connectionConfig) || !string.IsNullOrEmpty(optionalConnConfig))
            {
                connection = new MySqlConnection();

                if ((connection.State != ConnectionState.Open) && (connection.State != ConnectionState.Connecting))
                {
                    if (!string.IsNullOrEmpty(optionalConnConfig))
                        connection.ConnectionString = optionalConnConfig;
                    else
                        connection.ConnectionString = this.connectionConfig;

                    connection.Open();
                }
            }
            else
                throw new ConnectionConfigurationNotFoundException(cultureAcronym);

            return (connection.State == ConnectionState.Open);
        }

        protected bool disconnect()
        {
            if (connection.State == ConnectionState.Open)
                connection.Close();

            return (connection.State == ConnectionState.Closed);
        }

        // Suppression defined because the input variable sqlInstruction could not be provided by user input, but only by the ropsql own entity parse methods
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        protected int executeCommand(string sqlInstruction, Dictionary<object, object> parameters)
        {
            MySqlCommand sqlCommand;
            string insertCommand = SQLANSIRepository.DataPersistence_ReservedWord_INSERT;

            int executionReturn = 0;

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = connection.CreateCommand();
                sqlCommand.CommandText = sqlInstruction;
                if ((transactionControl != null)
                        && (transactionControl.Connection != null))
                    sqlCommand.Transaction = transactionControl;

                sqlCommand.Parameters.Clear();
                foreach (var param in parameters)
                {
                    SqlParameter newSqlParameter = new SqlParameter(param.Key.ToString(), param.Value);
                    sqlCommand.Parameters.Add(newSqlParameter);
                }

                if (transactionControl != null)
                    sqlCommand.Transaction = transactionControl;

                int insertedKey = 0;
                if (sqlCommand.CommandText.Contains(insertCommand))
                {
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = SQLANSIRepository.DataPersistence_Action_GetLastId;
                    int.TryParse(sqlCommand.ExecuteScalar().ToString(), out insertedKey);
                    executionReturn = insertedKey;
                }
                else
                    executionReturn = sqlCommand.ExecuteNonQuery();

                sqlCommand = null;
            }

            return executionReturn;
        }

        // Suppression defined because the input variable procedureName could not be provided by user input, but only by the ropsql own entity parse methods
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        protected XmlDocument executeProcedure(string procedureName, Dictionary<object, object> parameters)
        {
            MySqlCommand sqlCommand = null;
            MySqlDataAdapter sqlAdapter = null;

            DataSet dataTables = new DataSet();
            StringBuilder xmlText = new StringBuilder();
            XmlDocument returnStruct = new XmlDocument();

            if (connection.State == System.Data.ConnectionState.Open)
            {
                XmlWriter xmlWriter = XmlWriter.Create(xmlText);

                sqlCommand = connection.CreateCommand();
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.CommandText = procedureName;

                sqlCommand.Parameters.Clear();
                foreach (var param in parameters)
                {
                    SqlParameter newSqlParameter = new SqlParameter(param.Key.ToString(), param.Value);
                    sqlCommand.Parameters.Add(newSqlParameter);
                }

                if ((transactionControl != null)
                        && (transactionControl.Connection != null))
                    sqlCommand.Transaction = transactionControl;

                sqlAdapter = new MySqlDataAdapter(sqlCommand);

                sqlAdapter.Fill(dataTables);

                dataTables.WriteXml(xmlWriter);

                returnStruct.LoadXml(xmlText.ToString());

                sqlCommand = null;
            }

            return returnStruct;
        }

        // Suppression defined because the input variable sqlInstruction could not be provided by user input, but only by the ropsql own entity parse methods
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        protected XmlDocument executeQuery(string sqlInstruction)
        {
            MySqlCommand sqlCommand = null;
            MySqlDataAdapter sqlAdapter = null;

            DataSet dataTables = new DataSet();
            StringBuilder xmlText = new StringBuilder();
            XmlDocument returnStruct = new XmlDocument();

            if (connection.State == System.Data.ConnectionState.Open)
            {
                XmlWriter xmlWriter = XmlWriter.Create(xmlText);

                sqlCommand = connection.CreateCommand();
                sqlCommand.CommandText = sqlInstruction;

                if ((transactionControl != null)
                        && (transactionControl.Connection != null))
                    sqlCommand.Transaction = transactionControl;

                sqlAdapter = new MySqlDataAdapter(sqlCommand);

                sqlAdapter.Fill(dataTables);

                dataTables.WriteXml(xmlWriter);

                returnStruct.LoadXml(xmlText.ToString());

                sqlCommand = null;
            }

            return returnStruct;
        }

        #endregion
    }
}
