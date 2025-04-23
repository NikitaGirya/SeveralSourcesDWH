select  id
        , "name"
        , bonus_percent
        , min_payment_threshold
from    public.ranks
where   id > {cut_param};