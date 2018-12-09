SELECT foo FROM bar WHERE foo = 1;
SELECT foo FROM bar WHERE foo = 2;
-- comment line, will be ignored
-- empty lines as well

-- SQL comments inside the query will be parsed and used as a "method" in the report
/* get_items.sql */ SELECT foo FROM bar ORDER BY foo LIMIT 20;