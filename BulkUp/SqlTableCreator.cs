using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace BulkUp
{
	public class SqlTableCreator
	{
		#region Instance Variables
		private SqlConnection _connection;
		public SqlConnection Connection
		{
			get { return _connection; }
			set { _connection = value; }
		}

		private SqlTransaction _transaction;
		public SqlTransaction Transaction
		{
			get { return _transaction; }
			set { _transaction = value; }
		}

		private string _tableName;
		public string DestinationTableName
		{
			get { return _tableName; }
			set { _tableName = value; }
		}
		#endregion

		#region Constructor
		public SqlTableCreator() { }

		public SqlTableCreator(SqlConnection connection) : this(connection, null) { }

		public SqlTableCreator(SqlConnection connection, SqlTransaction transaction)
		{
			_connection = connection;
			_transaction = transaction;
		}
		#endregion

		#region Instance Methods
		public object Create(DataTable schema)
		{
			return Create(schema, null);
		}

		public object Create(DataTable schema, int numKeys)
		{
			int[] primaryKeys = new int[numKeys];
			for (int i = 0; i < numKeys; i++)
			{
				primaryKeys[i] = i;
			}
			return Create(schema, primaryKeys);
		}

		public object Create(DataTable schema, int[] primaryKeys)
		{
			string sql = GetCreateSQL(_tableName, schema, primaryKeys);

			SqlCommand cmd;
			if (_transaction != null && _transaction.Connection != null)
				cmd = new SqlCommand(sql, _connection, _transaction);
			else
				cmd = new SqlCommand(sql, _connection);

			return cmd.ExecuteNonQuery();
		}

		public object CreateFromDataTable(DataTable table)
		{
			string sql = GetCreateFromDataTableSQL(_tableName, table);

			SqlCommand cmd;
			if (_transaction != null && _transaction.Connection != null)
				cmd = new SqlCommand(sql, _connection, _transaction);
			else
				cmd = new SqlCommand(sql, _connection);

			return cmd.ExecuteNonQuery();
		}

		public object CreateFromColumnDetails(List<ColumnDetails> tableColumns)
		{
			string sql = GetCreateFromColumnDetailSQL(_tableName, tableColumns);

			SqlCommand cmd;
			if (_transaction != null && _transaction.Connection != null)
				cmd = new SqlCommand(sql, _connection, _transaction);
			else
				cmd = new SqlCommand(sql, _connection);

			return cmd.ExecuteNonQuery();
		}

		public List<ColumnDetails> CreateFromSqlTable(string tableName)
		{
			List<ColumnDetails> columnDetails;

			const string sql =
				@"SELECT c.COLUMN_NAME as Name, c.COLUMN_DEFAULT as DefaultValue, 
					CASE WHEN k.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS IsPrimaryKey,
					COLUMNPROPERTY(object_id(c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IsIdentity,
					c.DATA_TYPE as DataType, c.CHARACTER_MAXIMUM_LENGTH as CharLength
				FROM INFORMATION_SCHEMA.COLUMNS as c
				LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as k 
					ON k.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME
				WHERE c.TABLE_NAME = @TableName";

			columnDetails = _connection.Query<ColumnDetails>(sql, new { TableName = tableName }).ToList();
			CreateFromColumnDetails(columnDetails);
			return columnDetails;
		}
		#endregion

		#region Static Methods

		public static string GetCreateSQL(string tableName, DataTable schema, int[] primaryKeys)
		{
			string sql = "CREATE TABLE " + tableName + " (\n";

			// columns
			foreach (DataRow column in schema.Rows)
			{
				if (!(schema.Columns.Contains("IsHidden") && (bool)column["IsHidden"]))
					sql += column["ColumnName"].ToString() + " " + SQLGetType(column) + ",\n";
			}
			sql = sql.TrimEnd(new char[] { ',', '\n' }) + "\n";

			// primary keys
			string pk = "CONSTRAINT PK_" + tableName + " PRIMARY KEY CLUSTERED (";
			bool hasKeys = (primaryKeys != null && primaryKeys.Length > 0);
			if (hasKeys)
			{
				// user defined keys
				foreach (int key in primaryKeys)
				{
					pk += schema.Rows[key]["ColumnName"].ToString() + ", ";
				}
			}
			else
			{
				// check schema for keys
				string keys = string.Join(", ", GetPrimaryKeys(schema));
				pk += keys;
				hasKeys = keys.Length > 0;
			}
			pk = pk.TrimEnd(new char[] { ',', ' ', '\n' }) + ")\n";
			if (hasKeys) sql += pk;
			sql += ")";

			return sql;
		}

		public static string GetCreateFromColumnDetailSQL(string tableName, List<ColumnDetails> tableColumns)
		{
			string sql = "CREATE TABLE [" + tableName + "] (\n";
			// columns
			foreach (var column in tableColumns)
			{
				sql += "[" + column.Name + "] " + column.DataType;
				if (column.CharLength > 0)
					sql += "(" + column.CharLength.ToString() + ") ";
				if (column.IsIdentity)
					sql += " IDENTITY (0,1) ";
				else if (column.DefaultValue != null && column.DefaultValue.ToString().Length > 0)
					sql += " DEFAULT " + column.DefaultValue.ToString();
				sql += ",\n";
			}
			sql = sql.TrimEnd(new char[] { ',', '\n' }) + "\n";
			// primary keys
			if (tableColumns.Count(c => c.IsPrimaryKey) > 0)
			{
				sql += "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED (";
				tableColumns.Where(c => c.IsPrimaryKey).ToList()
				            .ForEach(c => sql += "[" + c.Name + "]," );
				
				sql = sql.TrimEnd(new char[] { ',' }) + ")\n";
			}
			sql += ")";
			return sql;
		}

		public static string GetCreateFromDataTableSQL(string tableName, DataTable table)
		{
			string sql = "CREATE TABLE [" + tableName + "] (\n";
			// columns
			foreach (DataColumn column in table.Columns)
			{
				sql += "[" + column.ColumnName + "] " + SQLGetType(column);
				if (column.AutoIncrement)
					sql += " IDENTITY (" + column.AutoIncrementSeed.ToString() + "," + column.AutoIncrementStep.ToString() + ")";
				else if (column.DefaultValue != null && column.DefaultValue.ToString().Length > 0)
					sql += " DEFAULT " + column.DefaultValue.ToString();
				sql += ",\n";
			}
			sql = sql.TrimEnd(new char[] { ',', '\n' }) + "\n";
			// primary keys
			if (table.PrimaryKey.Length > 0)
			{
				sql += "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED (";
				foreach (DataColumn column in table.PrimaryKey)
				{
					sql += "[" + column.ColumnName + "],";
				}
				sql = sql.TrimEnd(new char[] { ',' }) + ")\n";
			}
			sql += ")";
			return sql;
		}

		public static string[] GetPrimaryKeys(DataTable schema)
		{
			List<string> keys = new List<string>();

			foreach (DataRow column in schema.Rows)
			{
				if (schema.Columns.Contains("IsKey") && (bool)column["IsKey"])
					keys.Add(column["ColumnName"].ToString());
			}

			return keys.ToArray();
		}

		// Return T-SQL data type definition, based on schema definition for a column
		// Based off of http://msdn.microsoft.com/en-us/library/ms131092.aspx
		public static string SQLGetType(object type, int columnSize, int numericPrecision, int numericScale)
		{
			switch (type.ToString())
			{
				case "System.Byte[]":
					return "VARBINARY(MAX)";

				case "System.Boolean":
					return "BIT";

				case "System.DateTime":
					return "DATETIME";

				case "System.DateTimeOffset":
					return "DATETIMEOFFSET";

				case "System.Decimal":
					if (numericPrecision != -1 && numericScale != -1)
						return "DECIMAL(" + numericPrecision + "," + numericScale + ")";
					else
						return "DECIMAL";

				case "System.Double":
					return "FLOAT";

				case "System.Single":
					return "REAL";

				case "System.Int64":
					return "BIGINT";

				case "System.Int32":
					return "INT";

				case "System.Int16":
					return "SMALLINT";

				case "System.String":
					return "NVARCHAR(" + ((columnSize == -1 || columnSize > 8000) ? "MAX" : columnSize.ToString()) + ")";

				case "System.Byte":
					return "TINYINT";

				case "System.Guid":
					return "UNIQUEIDENTIFIER";

				default:
					throw new Exception(type.ToString() + " not implemented.");
			}
		}

		// Overload based on row from schema table
		public static string SQLGetType(DataRow schemaRow)
		{
			int numericPrecision;
			int numericScale;

			if (!int.TryParse(schemaRow["NumericPrecision"].ToString(), out numericPrecision))
			{
				numericPrecision = -1;
			}
			if (!int.TryParse(schemaRow["NumericScale"].ToString(), out numericScale))
			{
				numericScale = -1;
			}

			return SQLGetType(schemaRow["DataType"],
								int.Parse(schemaRow["ColumnSize"].ToString()),
								numericPrecision,
								numericScale);
		}

		// Overload based on DataColumn from DataTable type
		public static string SQLGetType(DataColumn column)
		{
			return SQLGetType(column.DataType, column.MaxLength, -1, -1);
		}
		#endregion
	}

	public class ColumnDetails
	{
		public string Name { get; set; }
		public string DefaultValue { get; set; }
		public bool IsPrimaryKey { get; set; }
		public bool IsIdentity { get; set; }
		public string DataType { get; set; }
		public int CharLength { get; set; }
	}
}
