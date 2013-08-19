BulkUp
======

A simple SQL Server Bulk Insert / Merge API for C#.  Converts object collections to datatable to avoid messy mappings.  Creates temp tables on the server for bulk insert before merging.  Handles all merge sql creation automatically.

This is just an initial push of a quick utility I use in production.  Feel free to look over.  Add your DB credentials to DataSource.cs or wire in your config file.

Example Usage
-------------
new BulkInsert<RateToInsert>(ratesToInsert)
	.SetConnection(DataSource.GetConnectionString("testDB"))
	.SetTable("blogData")
	.AddMapping("userId","userId")
	.AddMapping("aDate","theDate")
	.AddMapping("type","contentType")
	.AddMapping("content","body")
	.ExecuteMerge(new [] {"userId", "theDate"});