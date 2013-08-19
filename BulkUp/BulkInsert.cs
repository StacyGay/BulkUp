using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace BulkUp
{
	public class BulkInsert<T>
	{
		public IEnumerable<T> Data { get; set; }
		public string Table { get; set; }
		public string ConnectionString { get; set; }
		private DataTable dataTable;
		private IEnumerable<ColumnDetails> tableSchema;
		private String tempTable = "";
		private List<Tuple<string, string>> fieldMappings = new List<Tuple<string, string>>(); 

		public BulkInsert(IEnumerable<T> data)
		{
			Data = data;
			dataTable = data.ToDataTable();
		}

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

				if (tempTable.Length > 0 && ConnectionString.Length > 0)
				{
					using (var db = DataSource.ConnectCustom(ConnectionString))
					{
						db.Execute("DROP TABLE " + tempTable);
					}
				}
			}
			catch { }
		}

		public BulkInsert<T> SetTable(string table)
		{
			Table = table;
			return this;
		}

		public BulkInsert<T> SetConnection(string conString)
		{
			ConnectionString = conString;
			return this;
		}

		public string CreateTempTable()
		{
			tempTable = "T"+Guid.NewGuid().ToString().Replace("-","_");
			using (var db = DataSource.ConnectCustom(ConnectionString))
			{
				var tableCreator = new SqlTableCreator(db);
				tableCreator.DestinationTableName = tempTable;
				tableSchema = tableCreator.CreateFromSqlTable(Table);
			}

			return tempTable;
		}

		public BulkInsert<T> AddMapping(string src, string dest)
		{
			fieldMappings.Add(new Tuple<string, string>(src,dest));
			return this;
		}

		public BulkInsert<T> ExecuteInsert(string table)
		{
			using (SqlBulkCopy bulkCopy = new SqlBulkCopy(ConnectionString))
			{
				bulkCopy.DestinationTableName = table;

				//SqlBulkCopyColumnMapping courseIDMap = new SqlBulkCopyColumnMapping("courseID", "courseID");
				//bulkCopy.ColumnMappings.Add(courseIDMap);

				fieldMappings.ForEach(m => bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(m.Item1,m.Item2)));

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

		public BulkInsert<T> ExecuteMerge(IEnumerable<string> keys, bool delete = false)
		{
			ExecuteInsert(CreateTempTable());

			var keysCondition = new List<string>();
			keys.ToList().ForEach(k =>
				{
					var condition = "t." + k + "=s." + k;
					keysCondition.Add(condition);
				});

			var primaryKeys = tableSchema.Where(c => c.IsPrimaryKey).Select(k => k.Name).ToList();

			var columns = new List<string>();
			foreach (var column in tableSchema)
			{
				if(primaryKeys.Count(k => k == column.Name) < 1)
					columns.Add(column.Name);
			}

			var columnsUpdate = new List<string>();
			var columnsInsert = new List<string>();
			columns.ForEach(c =>
			{
				var currentColumn = c + " = s." + c;
				var insertColumn = "s." + c;
				columnsUpdate.Add(currentColumn);
				columnsInsert.Add(insertColumn);
			});

			using (var db = DataSource.ConnectCustom(ConnectionString))
			{
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
				if(delete)
					mergeQuery += 
						@"when not matched by source then
							delete";
				mergeQuery += ";";

				try
				{
					SqlTransaction trans = db.BeginTransaction();
					db.Execute(mergeQuery, transaction: trans);
					trans.Commit();
				}
				catch (Exception)
				{
					FreeData();
					throw;
				}
			}

			return this;
		}
	}
}
