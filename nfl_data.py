import rpy2.robjects as robj
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
import pandas as pd
import matplotlib.pyplot as plt
import os

pandas2ri.activate()
nfl_scrap_r = importr("nflscrapR")

output_dir = f"{os.getcwd()}/wp_data"

def create_output_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_game_id(home_team, week, year=2019):
    sched_df = nfl_scrap_r.scrape_game_ids(year, weeks=week)
    return sched_df.loc[sched_df['home_team'] == home_team].iloc[0]['game_id']

def get_game_play_by_play(game_id):
    return nfl_scrap_r.scrape_json_play_by_play(game_id)

def get_win_percentage_df(game_id):
    df_wp = get_game_play_by_play(game_id)
    return df_wp[['game_seconds_remaining', 'home_wp', 'away_wp']]

def plot_wp(df_wp, team):
    if not (team in ['home', 'away']):
        return None

    y_val = f"{team}_wp"
    time = df_wp['game_seconds_remaining']
    wp = df_wp[y_val]

    plt.plot(time, wp, 'b')
    plt.title(f'{team} win percentage')
    plt.xlim(3600, 0)
    plt.ylim(0, 1)
    plt.show()


games = [['LA', 'BAL', 12], ['NE', 'DAL', 12], ['SF', 'GB', 12], ['DAL', 'BUF', 13], ['DET', 'CHI', 13], ['ATL', 'NO', 13]]

create_output_dir(output_dir)

for game in games:
    week = game[2]
    home = game[0]
    away = game[1]
    wp_df = get_win_percentage_df(get_game_id(home, week))
    wp_df.to_csv(f"{output_dir}/week-{week}_{home}_{away}_wp.csv")
