SELECT v.table_catalog AS catalog ,
         v.table_schema AS database ,
         v.table_name AS view_name ,
         v.view_definition AS script
FROM information_schema.views v
