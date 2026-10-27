-- A user cannot order before being born: the test fails if this query returns rows.
-- Invalid birthdates are replaced with NULL in stg_users (birthdate_is_valid = false),
-- so only the validated birthdates are compared.
select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
where u.birthdate_is_valid
  and o.ordered_at < u.birthdate
