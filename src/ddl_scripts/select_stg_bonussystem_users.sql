select  id
        , order_user_id
from    public.users
where   id > {cut_param};