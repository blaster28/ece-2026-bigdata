-- birthdate must be NULL exactly when birthdate_is_valid is false
select user_id, birthdate, birthdate_is_valid
from {{ ref('stg_users') }}
where (birthdate_is_valid and birthdate is null)
   or (not birthdate_is_valid and birthdate is not null)
