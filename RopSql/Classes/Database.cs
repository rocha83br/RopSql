using System;
using System.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.RopSql.Resources;
using System.Data.RopSql.Exceptions;
using System.Xml;
using System.Text;

namespace System.Data.RopSql
{
    public class DataBaseOleDbConnection
    {
        #region Declarations

        readonly string connectionConfig;
        readonly string cultureAcronym;

        private readonly SqlConnection connection;
        SqlTransaction transactionControl;

        #endregion

        #region Constructors

        protected DataBaseOleDbConnection()
        {
            connectionConfig = new Encrypter().DecryptText(
                               ConfigurationManager.ConnectionStrings["SGE_ConnStr"].ConnectionString);

            cultureAcronym = ConfigurationManager.AppSettings["Culture"];

            connection = new SqlConnection(connectionConfig);

            transactionControl = null;
        }

        #endregion

        #region Public Methods

        public void StartTransaction()
        {
            if (connection.State == System.Data.ConnectionState.Open)
                this.transactionControl = connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if ((connection.State == ConnectionState.Open)
                && (this.transactionControl != null))
                this.transactionControl.Commit();
        }

        public void CancelTransaction()
        {
            if ((connection.State == ConnectionState.Open)
                && (this.transactionControl != null))
                this.transactionControl.Rollback();
        }

        #endregion

        #region Helper Methods

        protected bool connect()
        {
            if (!string.IsNullOrEmpty(this.connectionConfig))
            {
                if (connection.State != ConnectionState.Open)
                {
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

        protected int executeCommand(string sqlInstruction, Dictionary<object, object> parameters)
        {
            SqlCommand sqlCommand;
            string insertCommand = SQLANSIRepository.DataPersistence_ReservedWord_INSERT;

            int executionReturn = 0;

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = connection.CreateCommand();
                sqlCommand.CommandText = sqlInstruction;

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

        protected XmlDocument executeSearch(string sqlInstruction)
        {
            SqlCommand sqlCommand = null;
            SqlDataAdapter sqlAdapter = null;

            DataSet dataTables = new DataSet();
            StringBuilder xmlText = new StringBuilder();
            XmlDocument returnStruct = new XmlDocument();

            if (connection.State == System.Data.ConnectionState.Open)
            {
                XmlWriter xmlWriter = XmlWriter.Create(xmlText);

                sqlCommand = connection.CreateCommand();
                sqlCommand.CommandText = sqlInstruction;

                sqlAdapter = new SqlDataAdapter(sqlCommand);

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
