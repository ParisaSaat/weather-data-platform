select
    city,
    station_id,
    resolution_method,
    match_rule,
    _batch_id   as batch_id,
    _loaded_at  as resolved_at
from {{ source('pipeline', 'pipeline_target_stations') }}
