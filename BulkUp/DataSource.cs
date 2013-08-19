using System.Data.SqlClient;

namespace BulkUp
{
	public class DataSource
	{
		public static SqlConnection Connect(string database, bool staging = false)
		{
			string connString = GetConnectionString(database, staging);
			SqlConnection connection = new SqlConnection(connString);
			connection.Open();

			return connection;
		}

		public static SqlConnection ConnectCustom(string connectionString)
		{
			SqlConnection connection = new SqlConnection(connectionString);
			connection.Open();

			return connection;
		}

		public static string GetConnectionString(string database, bool staging = false)
		{
			string connectAddress = "";
			string devAddress = @"";
			string userName = "";
			string pw = "";

			SqlConnectionStringBuilder sqlConString = new SqlConnectionStringBuilder();

			if (staging)
				sqlConString.DataSource = devAddress;
			else
				sqlConString.DataSource = connectAddress;

			sqlConString.UserID = userName;
			sqlConString.Password = pw;
			sqlConString.InitialCatalog = database;
			sqlConString.ConnectTimeout = 120;

			return sqlConString.ConnectionString;
		}
	}
}
