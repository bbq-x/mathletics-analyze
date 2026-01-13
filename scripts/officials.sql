
with player as (
    select
        game_id
        ,team_id
        ,sum(pf) as fouls
        ,sum(fta) as ft_attempts  
    from "202526".game_player
    group by game_id, team_id
)

select
    gd.game_id
    ,gd.home_id
    ,gd.away_id
    ,gd.officials
    ,gd.duration
    ,p1.fouls as home_fouls
    ,p2.fouls as away_fouls
    ,p1.fouls - p2.fouls as fouls_diff
    ,p1.fouls + p2.fouls as fouls_total   
    ,p1.ft_attempts as home_fta
    ,p2.ft_attempts as away_fta
    ,p1.ft_attempts - p2.ft_attempts as fta_diff
    ,p1.ft_attempts + p2.ft_attempts as fta_total
from "202526".game_details as gd
left join player as p1
    on gd.game_id = p1.game_id
        and gd.home_id = p1.team_id
left join player as p2
    on gd.game_id = p2.game_id
        and gd.away_id = p2.team_id
where home_pts is not null
