using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;



namespace BulkUp
{
	public class BulkInsert<T>
		where T : new()
	{
		private readonly DataTable dataTable;
		private readonly List<Tuple<string, string>> fieldMappings = new List<Tuple<string, string>>();
		private readonly List<Tuple<string, string>> fieldExclusions = new List<Tuple<string, string>>();
		private IEnumerable<ColumnDetails> tableSchema;
		private String tempTable = "";

		public bool StrictMap { get; set; }

		public BulkInsert(IEnumerable<T> data)
		{
			Data = data;
			dataTable = data.ToDataTable();
			StrictMap = false;
		}

		public IEnumerable<T> Data { get; set; }
		public string Table { get; set; }
		public SqlConnection Connection { get; set; }

		~BulkInsert()
		{
			FreeData();
		}

		public void FreeData()
		{
			try
			{
				if (dataTable != null)
					dataTable.Dispose();

				if (tempTable.Length > 0 && Connection != null)
				{
					Connection.Execute("DROP TABLE " + tempTable);
				}
			}
			catch
			{
			}
		}

		public BulkInsert<T> SetStrictMap(bool strictMap = true)
		{
			StrictMap = strictMap;
			return this;
		}

		public BulkInsert<T> SetTable(string table)
		{
			Table = table;
			return this;
		}

		public BulkInsert<T> SetConnection(IDbConnection con)
		{
			Connection = con as SqlConnection;
			return this;
		}

		public string CreateTempTable()
		{
			tempTable = "##T" + Guid.NewGuid().ToString().Replace("-", "_");
			var tableCreator = new SqlTableCreator(Connection);
			tableCreator.DestinationTableName = tempTable;
			tableSchema = tableCreator.CreateFromSqlTable(Table);

			return tempTable;
		}

		public BulkInsert<T> AddMapping(string src, string dest)
		{
			fieldMappings.Add(new Tuple<string, string>(src, dest));
			return this;
		}

		public BulkInsert<T> AddExclusion(string src, string dest)
		{
			fieldExclusions.Add(new Tuple<string, string>(src, dest));
			return this;
		}

		public BulkInsert<T> ExecuteInsert(string table)
		{
			using (var bulkCopy = new SqlBulkCopy(Connection))
			{
				bulkCopy.DestinationTableName = table;

				//SqlBulkCopyColumnMapping courseIDMap = new SqlBulkCopyColumnMapping("courseID", "courseID");
				//bulkCopy.ColumnMappings.Add(courseIDMap);

				if (fieldMappings.Count > 0) // map using supplied mappings
				{
					fieldMappings.ForEach(m => bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(m.Item1, m.Item2)));
				}
				else if (tableSchema.Any()) // map by table schema
				{
					PropertyInfo[] props = new T().GetType().GetProperties(); // check to make sure table fields exist in object

					var propsToMap = tableSchema.Where(c => !c.IsIdentity && props.Any(p => p.Name == c.Name)).ToList();
					propsToMap.ForEach(c => bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(c.Name, c.Name)));
				}
				else // map by reflection
				{
					PropertyInfo[] props = new T().GetType().GetProperties();
					props.ToList().ForEach(p => bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(p.Name, p.Name)));
				}

				fieldExclusions.ForEach(e => bulkCopy.ColumnMappings.Remove(new SqlBulkCopyColumnMapping(e.Item1, e.Item2)));

				bulkCopy.BulkCopyTimeout = 60;
				bulkCopy.BatchSize = 5000;
				try
				{
					bulkCopy.WriteToServer(dataTable);
				}
				catch (Exception)
				{
					FreeData();
					throw;
				}
			}

			return this;
		}

		public BulkInsert<T> ExecuteInsert()
		{
			ExecuteInsert(Table);
			return this;
		}

		public BulkInsert<T> ExecuteMerge(IEnumerable<string> keys, bool delete = false, string deleteParam = "")
		{
			ExecuteInsert(CreateTempTable());

			var keysCondition = new List<string>();
			keys.ToList().ForEach(k =>
			{
				string condition = "t.[" + k + "]=s.[" + k + "]";
				keysCondition.Add(condition);
			});

			IEnumerable<string> dataObjectFields = typeof(T).GetProperties().Select(f => f.Name);
			List<string> identityList = tableSchema
				.Where(c => c.IsIdentity)
				.Select(k => k.Name).ToList();


			var columns = new List<string>();

			if (StrictMap)
			{
				columns = fieldMappings.Except(fieldExclusions).Select(m => m.Item2).ToList();
			}
			else
			{
				foreach (ColumnDetails column in tableSchema)
				{
					if (identityList.All(k => k != column.Name) && !fieldExclusions.Any(e => e.Item2 == column.Name))
						columns.Add("[" + column.Name + "]");
				}
			}

			var columnsUpdate = new List<string>();
			var columnsInsert = new List<string>();
			columns.ForEach(c =>
			{
				string currentColumn = c + " = s." + c;
				columnsUpdate.Add(currentColumn);
				string insertColumn = "s." + c;
				columnsInsert.Add(insertColumn);
			});

			//(" + String.Join(", ", columns) + @") merge_hint
			string mergeQuery =
				@"merge " + Table + @" as t
				using " + tempTable + @" as s 
					on (" + String.Join(" and ", keysCondition) + @")
				when matched then 
					update set " + String.Join(", ", columnsUpdate) + @" 
				when not matched by target then 
					insert (" + String.Join(", ", columns) + @") 
					values (" + String.Join(", ", columnsInsert) + @") ";
			if (delete)
			{
				if (!String.IsNullOrWhiteSpace(deleteParam))
				{
					mergeQuery +=
					@"when not matched by source and " + deleteParam + @" then
						delete";
				}
				else
				{
					mergeQuery +=
					@"when not matched by source then
						delete";
				}

			}
			mergeQuery += ";";

			try
			{
				SqlTransaction trans = Connection.BeginTransaction();
				Connection.Execute(mergeQuery, transaction: trans);
				trans.Commit();
			}
			catch (Exception)
			{
				FreeData();
				throw;
			}


			return this;
		}
	}
}
