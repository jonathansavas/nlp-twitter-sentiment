import os
import nflgame
import pandas as pd
import datetime
from pytz import timezone
import traceback

output_dir = f"{os.getcwd()}/nflgame_output"

gametime_dict = {}
gameinfo_df = pd.DataFrame(columns=['away','day','eid','gamekey','home','meridiem',
                                    'month','season_type','time','wday','week','year'])
game_persisted = {}

def create_output_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)

def persist_gametime(game):
    df = gametime_dict[game.gamekey]
    path = f"{output_dir}/{game.gamekey}_{game.home}_{game.away}_week-{game.schedule['week']}.csv"

    if os.path.exists(path):
        df.to_csv(path, index=False, header=False, mode='a')
    else: 
        df.to_csv(path, index=False)

def persist_gametime_on_failure(gamekey):
    df = gametime_dict[gamekey]
    info = gameinfo_df[gameinfo_df['gamekey'] == gamekey]

    home = info['home'].values[0]
    away = info['away'].values[0]
    week = info['week'].values[0]

    path = f"{output_dir}/{gamekey}_{home}_{away}_week-{week}_on-failure.csv"

    if os.path.exists(path):
        df.to_csv(path, index=False, header=False, mode='a')
    else: 
        df.to_csv(path, index=False)

def persist_gameinfo(df):
    path = f"{output_dir}/nfl_week_{df.iloc[0]['week']}.csv"

    if os.path.exists(path):
        df.to_csv(path, index=False, header=False, mode='a')
    else: 
        df.to_csv(path, index=False)

def log_game_updates(active, completed, diffs):
    for game in active:
        if not game.gamekey in gametime_dict:
            global gameinfo_df
            gameinfo_df = gameinfo_df.append(game.schedule, ignore_index=True)
            gametime_dict[game.gamekey] = pd.DataFrame(columns=['quarter', 'clock', 'real_time', 'is_halftime'])
            game_persisted[game.gamekey] = False

        if game.playing():
            df = gametime_dict[game.gamekey]
            game_status = {'quarter': game.time.qtr, 'clock': game.time.clock, 'real_time': str(datetime.datetime.now()), 'is_halftime': str(game.time.is_halftime())}

            gametime_dict[game.gamekey] = df.append(game_status, ignore_index=True)

    for game in completed:
        if not game_persisted[game.gamekey]:
            persist_gametime(game)
            game_persisted[game.gamekey] = True

# returns tomorrow at 2am EST, allows time for the primetime game on Mon, Thurs, Sun to finish
def get_stop_time():
    tomorrow = datetime.datetime.now(timezone('EST')) + datetime.timedelta(days=1)
    return datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, hour=7)
        

def main():    
    create_output_dir(output_dir)

    try:
        nflgame.live.run(log_game_updates, stop=get_stop_time())
    except:
        print(traceback.format_exc())
        print()
    finally:
        for gamekey, persisted in game_persisted.items():
            if not persisted:
                persist_gametime_on_failure(gamekey)

        persist_gameinfo(gameinfo_df)

    print(f"Finished grabbing data, files persisted in {output_dir}")


if __name__ == '__main__':
    main()
