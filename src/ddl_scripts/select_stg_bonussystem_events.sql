select  id
        , event_ts
        , event_type
        , event_value
from    public.outbox
where   id > {cut_param};