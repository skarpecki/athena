SELECT c.table_catalog AS catalog ,
         c.table_schema AS database ,
         c.table_name AS view_name ,
         c.ordinal_position ,
         c.column_default ,
         c.is_nullable ,
         c.data_type ,
         c.comment ,
         c.extra_info
FROM information_schema.columns c
JOIN information_schema.views v
    ON c.table_catalog = v.table_catalog
        AND c.table_schema = v.table_schema
        AND c.table_name = v.table_name 