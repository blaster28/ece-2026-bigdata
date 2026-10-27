{{ config(severity='warn') }}

-- The 3 first digits of a ZIP code (USPS prefix) identify the state:
-- the test returns the users whose ZIP code prefix does not belong to their state
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where u.zip_code <> ''
  and not exists (
    select 1
    from {{ ref('zip_prefixes') }} z
    where z.state = u.state
      and cast(left(u.zip_code, 3) as integer) between z.prefix_min and z.prefix_max
  )
