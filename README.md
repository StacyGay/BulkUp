BulkUp
======

A simple SQL Server Bulk Insert / Merge API for C#.  Converts object collections to datatable to avoid messy mappings.  Creates temp tables on the server for bulk insert before merging.  Handles all merge sql creation automatically.

This is just an initial push of a quick utility I use in production.  Feel free to look over.  Add your DB credentials to DataSource.cs or wire in your config file.

Dependencies
------------
This does include a copy of Sam Saffron's Dapper that I've added a few extensions to.  SqlTableCreator is a bastard version of code I've collected from forums that I've added to to suit my own needs.

Example Usage
-------------
```C#
new BulkInsert<BlogPost>(allBlogPosts)
	.SetConnection(DataSource.GetConnectionString("testDB"))
	.SetTable("blogData")
	.AddMapping("userId","userId")
	.AddMapping("aDate","theDate")
	.AddMapping("type","contentType")
	.AddMapping("content","body")
	.ExecuteMerge(new [] {"userId", "theDate"});
```