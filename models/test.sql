select *
from {{ source('DEMO', 'bike') }}
limit 10;