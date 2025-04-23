select  coalesce(max(cut_param), 0) as cut_param
from    stg.cut_param
where   table_name = '{table_name}';