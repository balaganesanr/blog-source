using System;
using System.Data;
using System.Data.SqlClient;

namespace DBHelper
{
    public class SQLHelper
    {
        #region [ LOCAL VARIABLES ]

        string _connectionString = string.Empty;

        int _commandTimeout = 0;

        #endregion [ LOCAL VARIABLES ]

        #region [ PRIVATE METHODS ]

        SqlConnection GetConnection()
        {
            return new SqlConnection { ConnectionString = _connectionString };;
        }

        SqlCommand GetCommand(SqlConnection connection, string commandText, CommandType commandType)
        {
            return new SqlCommand(commandText, connection)
            {
                CommandTimeout = _commandTimeout,
                CommandType = commandType
            };
        }

        #endregion [ PRIVATE METHODS ]

        public SQLHelper(string connectionString, int commandTimeout)
        {
            _connectionString = connectionString;
            _commandTimeout = commandTimeout;
        }

        #region "FILL DATA TABLE"

        public void Fill(DataTable dataTable, String procedureName)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);
            var adapter = new SqlDataAdapter { SelectCommand = command };

            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dataTable);
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    adapter.Dispose();
                }
            }
        }

        public void Fill(DataTable dataTable, String procedureName, SqlParameter[] parameters)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);
            var adapter = new SqlDataAdapter { SelectCommand = command };

            if (parameters != null)
                command.Parameters.AddRange(parameters);
            
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dataTable);
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    adapter.Dispose();
                }
            }
        }

        #endregion

        #region "FILL DATASET"

        public void Fill(DataSet dataSet, String procedureName)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);
            var adapter = new SqlDataAdapter { SelectCommand = command };

            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dataSet);
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    adapter.Dispose();
                }
            }
        }

        public void Fill(DataSet dataSet, String procedureName, SqlParameter[] parameters)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);
            var adapter = new SqlDataAdapter { SelectCommand = command };

            if (parameters != null)
                command.Parameters.AddRange(parameters);

            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dataSet);
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    adapter.Dispose();
                }
            }
        }

        #endregion

        #region "EXECUTE SCALAR"

        public object ExecuteScalar(String procedureName)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);
            
            object oReturnValue;
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    command.Transaction = transaction;
                    oReturnValue = command.ExecuteScalar();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    command.Dispose();
                }
            }
            return oReturnValue;
        }

        public object ExecuteScalar(String procedureName, SqlParameter[] parameters)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);

            object oReturnValue;
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    if (parameters != null)
                        command.Parameters.AddRange(parameters);

                    command.Transaction = transaction;
                    oReturnValue = command.ExecuteScalar();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    command.Dispose();
                }
            }
            return oReturnValue;
        }

        #endregion

        #region "EXECUTE NON QUERY"

        public int ExecuteNonQuery(string procedureName)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);
            
            int iReturnValue;
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    command.Transaction = transaction;
                    iReturnValue = command.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    command.Dispose();
                }
            }
            return iReturnValue;
        }

        public int ExecuteNonQuery(string procedureName, SqlParameter[] parameters)
        {
            var connection = GetConnection();
            var command = GetCommand(connection, procedureName, CommandType.StoredProcedure);

            int iReturnValue;
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    if (parameters != null)
                        command.Parameters.AddRange(parameters);

                    command.Transaction = transaction;
                    iReturnValue = command.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                    connection.Dispose();
                    command.Dispose();
                }
            }
            return iReturnValue;
        }

        #endregion
    }
}