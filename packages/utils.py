import pandas as pd

def backup_cnxn():
    from sqlalchemy import create_engine
    import psycopg2
    database = 'mathletics_prod' #'mathletics-data'                                               
    host = 'localhost' #'35.231.81.254'
    user = 'postgres' #'bbqx'
    pgsql_pw = "CinCity79!"
    port = 5432
    con_string = f"postgresql://{user}:{pgsql_pw}@{host}:{port}/{database}"
    engine = create_engine(con_string)
    con = psycopg2.connect(con_string)
    return con, engine

def cloud_cnxn():
    from sqlalchemy import create_engine
    import psycopg2
    # Heroku Connection Details
    database = 'dcchkn8kpopgf4'                                              
    host = 'c3np6sk103ru76.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com'
    user = 'uegq769s81o426' #'bbqx'
    pgsql_pw = 'p8721cc31fc535479daebd2bb4cf5778ae9ccedeba4523f8ecd34efd0b57f8718'
    port = 5432
    con_string = f"postgresql://{user}:{pgsql_pw}@{host}:{port}/{database}"
    engine = create_engine(con_string)
    con = psycopg2.connect(con_string)
    return con, engine    

def local_cnxn():
    from sqlalchemy import create_engine
    import psycopg2
    # Container Connection Details
    database = 'mathletics-local'                                              
    host = 'localhost'
    user = 'user' #'bbqx'
    pgsql_pw = "CinCity79!"
    port = 5433
    con_string = f"postgresql://{user}:{pgsql_pw}@{host}:{port}/{database}"
    engine = create_engine(con_string)
    con = psycopg2.connect(con_string)
    return con, engine
     
def get_libraries():
    import os
    import subprocess
    print("Checking for libraries...")
    git_codespace_libs = ["bs4","selenium","psycopg2","sqlalchemy","nba_api"]
    for lib in git_codespace_libs:
        print(f'  ...{lib}: {subprocess.check_call(["pip","install","{}".format(lib)])}')

def get_markets():
    return ['h2h','spreads','totals']

def get_slashes(op_sys):
    if op_sys == "ios":
        get_libraries()
    slash_dict = {
        "ios":"/",
        "win":"\\"
    }
    return slash_dict[op_sys]

def get_tables(dev_table):
    dev_dict = {
        "dev$sch":"schedule",
        "dev$ps":"player_stats",
        "dev$rs":"rotation_stats",
        "dev$odds":["h2h","spreads","totals"]
    }
    return dev_dict[dev_table]

def get_teams():
    team_info = pd.read_sql_query("select * from teams",con=cnxn_info()[1])  # Read in team info
    team_info.set_index("team_id",inplace=True)                      # Set index of team info to 'team_id'
    return team_info

def current_odds_url():
    return "https://api.the-odds-api.com/v4/sports/{}/odds/?apiKey={}&regions=us&markets={}"

def historical_odds_url():
    return "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/odds?apiKey={}&regions=us&markets={}&date={}"


def record_builder(season,game_type_flag=".*"):
    # Generate W/L Records from events table BY GAME_TYPE
    if game_type_flag == "Regular Season":
        game_type_flag = r"Regular Season|Emirates NBA Cup"
    team_info = get_teams()
    dict_records = {}                               # Stores win/loss data as incremented

    events = pd.read_sql_query(f"""select * from schedule${season_conversion(season)}
                               where home_pts is not null""",con=cnxn_info()[1])    # Returns query results for table and year
    #events = events[ (events["game_type"]=="Regular Season") | (events["game_type"]=="Emirates NBA Cup") ]   #### Using Regular Season for validation
    events = events[ events.game_type.str.contains(rf"{game_type_flag}") ]
    events.set_index("game_id",inplace=True)     # Sets index to events' unique id   
    
    for event in events.index:                      # Iterate through game_id's
        team_home = events.loc[event,"home_id"]     # Retrieve and assign home team_id
        team_away = events.loc[event,"away_id"]     # Retrieve and assign away team_id

        pts_home = events.loc[event,"home_pts"]
        pts_away = events.loc[event,"away_pts"]
        
        list_teams = [team_home,team_away]
        for team in list_teams:
            if team not in dict_records:
                dict_records[team] = {"wins":0,"losses":0,"games":0}
        
        dict_records[team_home]["games"] += 1
        dict_records[team_away]["games"] += 1
        
        if pts_home > pts_away:
            dict_records[team_home]["wins"] += 1
            dict_records[team_away]["losses"] += 1
        else:
            dict_records[team_away]["wins"] += 1
            dict_records[team_home]["losses"] += 1
    
    df_record = pd.DataFrame(dict_records).T
    df_record["team_name"] = team_info["team_name"].loc[df_record.index]
    df_record["conference"] = team_info["conference"].loc[df_record.index]
    df_record["win_%"] = round(df_record["wins"] / df_record["games"],2)
    
    for conf in df_record["conference"].unique():
        df_print = df_record[ df_record["conference"]==conf ]
        df_print = df_print[["team_name","wins","losses","win_%"]]        # Re-order columns
        df_print.set_index("team_name",inplace=True)
        print(season,conf)
        df_print.sort_index(inplace=True)
        print(df_print.sort_values('win_%',ascending=False),"\n")

def schedule_stat(season,stat):
    pd.options.mode.chained_assignment = None
    df_teams = get_teams()
    events = pd.read_sql_query(f"""select * from schedule${season_conversion(season)}
                               where game_type in ('Regular Season','Emirates NBA Cup') and
                               home_pts is not null""",con=cnxn_info()[1])    # Returns query results for table and year
    events.set_index("game_id",inplace=True)
    events.sort_values("game_time",inplace=True)
    
    checklist = []
    for col in events.columns:
        if stat in col:
            checklist.append(col)
    if len(checklist)==0:
        print(f"{stat} is not in 'schedule' columns")
        return
    
    list_events_teams = events["home_id"].unique()   # For iterating
    df_by_league = pd.DataFrame()
    
    list_of_dfs = [] 
    new_cols = ["game_time","home_id","away_id"]+checklist
    for team in list_events_teams:

        # All team records
        df_team_all = events[ (events["home_id"]==team) | (events["away_id"]==team) ]
        df_team_all = df_team_all[new_cols]
        df_team_all["game_num"] = df_team_all["game_time"].expanding().count()
        df_team_all["game_num"] = df_team_all["game_num"].astype(int)
        
        # As home team
        df_home = df_team_all[ df_team_all["home_id"]==team ]
        df_home["flag"] = "Home"
        df_home.rename(columns={f"home_{stat}":f"{stat}",
                                "home_id":"team",
                                "away_id":"opponent",
                                f"away_{stat}":"allowed"},inplace=True)
        df_home["team"] = df_home["team"].apply(lambda x: df_teams["team_name"].loc[x])
        df_home["opponent"] = df_home["opponent"].apply(lambda x: df_teams["team_name"].loc[x])
        
        # As away team   
        df_away = df_team_all[ df_team_all["away_id"]==team ]
        df_away["flag"] = "Away"
        df_away.rename(columns={f"away_{stat}":f"{stat}",
                                "away_id":"team",
                                "home_id":"opponent",
                                f"home_{stat}":"allowed"},inplace=True)
        df_away["team"] = df_away["team"].apply(lambda x: df_teams["team_name"].loc[x])
        df_away["opponent"] = df_away["opponent"].apply(lambda x: df_teams["team_name"].loc[x])        
        df_h_a = pd.concat([df_home,df_away])
        list_of_dfs.append(df_h_a) 
        
    df_by_league = pd.concat(list_of_dfs)
    df_by_league.sort_values(["team","game_num"],inplace=True)
    return df_by_league

def season_conversion(season):
    season_for_table = season.replace("-","")
    return season_for_table

def seasons():
    list_of_seasons = ["2021-22"
                       ,"2022-23"
                       ,"2023-24"
                       ,"2024-25"
                       ,"2025-26"]
    return list_of_seasons






    


        


        