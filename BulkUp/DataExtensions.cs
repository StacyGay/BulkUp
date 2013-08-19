using System.Collections.Generic;
using System.Data;
using System.Linq;


namespace BulkUp
{
	public static class DataExtensions
	{
		public static DataTable ToDataTable<T>(this IEnumerable<T> items)
		{
			var props = typeof (T).GetProperties();

			var dt = new DataTable();
			dt.Columns.AddRange(
				props.Select(p => new DataColumn(p.Name, p.PropertyType)).ToArray()
				);

			items.ToList().ForEach(
				i => dt.Rows.Add(props.Select(p => p.GetValue(i, null)).ToArray()));

			return dt;
		}
	}
}
