import tweepy as tw
import pandas as pd
import datetime
import re
import time
import os

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth,wait_on_rate_limit=True)

keyword_csv_path = './nfl-team-keywords.csv'
nfl_keyword_df = pd.read_csv(keyword_csv_path)

output_dir = f"{os.getcwd()}/tweets_output"

def create_output_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def convert_datetime_str_to_datetime(dt_str):
    date_time = dt_str.split(" ")
    date = date_time[0]
    time = date_time[1].split(".")[0]

    date_comp = date.split("-")
    time_comp = time.split(":")

    return datetime.datetime(int(date_comp[0]), int(date_comp[1]), int(date_comp[2]), int(time_comp[0]), int(time_comp[1]), int(time_comp[2]))

def convert_twitter_time_to_eastern(twitter_time, dt_hours=5):
    est = twitter_time - datetime.timedelta(hours=dt_hours)
    return datetime.datetime(est.year, est.month, est.day, est.hour, est.minute, est.second)

def get_team_keywords(keyword_df, team_abbr):
    return keyword_df[keyword_df['short'] == team_abbr]['keywords'].values[0].split(';')

def create_twitter_search_query(list_of_keyword_lists):
    return " OR ".join([" OR ".join(keywords) for keywords in list_of_keyword_lists])

def create_keyword_regex(keywords):
    return re.compile(r"|".join(keywords).replace(r'"', ''), re.I)

def get_date_str_plus_time(dt_str, dt_days=0, dt_hours=0):
    return str((convert_datetime_str_to_datetime(dt_str) + datetime.timedelta(days=dt_days, hours=dt_hours)).date())

def write_headers(ff):
    ff.write("time,text,subject")
    ff.write("\n")

def write_record(ff, time, text, subject):
    ff.write(f"{time},{text},{subject}")
    ff.write("\n")

def get_tweets_for_game(home, away, start_time, end_time, id_min, id_max, week):
    start_time_dt = convert_datetime_str_to_datetime(start_time)
    end_time_dt = convert_datetime_str_to_datetime(end_time)

    game_dir = f"{output_dir}/week-{week}_{home}_{away}"
    create_output_dir(game_dir)

    away_file = open(f"{game_dir}/week-{week}_{home}v{away}_{away}-tweets.csv", "a", encoding='utf-32')
    write_headers(away_file)

    home_file = open(f"{game_dir}/week-{week}_{home}v{away}_{home}-tweets.csv", "a", encoding='utf-32')
    write_headers(home_file)

    both = f"{home}+{away}"
    both_file = open(f"{game_dir}/week-{week}_{home}v{away}_{both}-tweets.csv", "a", encoding='utf-32')
    write_headers(both_file)

    home_keywords = get_team_keywords(nfl_keyword_df, home)
    away_keywords = get_team_keywords(nfl_keyword_df, away)

    home_regex = create_keyword_regex(home_keywords)
    away_regex = create_keyword_regex(away_keywords)

    c = tw.Cursor(api.search, q=create_twitter_search_query([home_keywords, away_keywords]), 
                              count=100, 
                              lang="en", 
                              since_id=id_min, 
                              max_id=id_max, # find tweet close to end_time to limit number of tweets skipped
                              tweet_mode="extended").items()

    gametag_regex = re.compile(f"\S*{home}\S*{away}\S*|\S*{away}\S*{home}\S*", re.I) # remove game tag ex. #BALvsLAR to avoid ignoring tweets with this common hash tag

    ctr = 0

    while True:
        try:
            tweet = c.next()
            tweet_time = convert_twitter_time_to_eastern(tweet.created_at)

            ctr = ctr + 1

            if ctr % 2000 == 0:
                print(f"Tweet: {ctr}, timestamp: {str(tweet_time)}")

            if (tweet_time > end_time_dt): continue
            if (tweet_time < start_time_dt): break

            tweet_text = tweet.full_text

            if tweet_text.startswith('RT '): continue

            tweet_text = gametag_regex.sub(" ", tweet_text)

            subject = ""

            if bool(home_regex.search(tweet_text)):
                if bool(away_regex.search(tweet_text)):
                    subject = both

                else:
                    subject = home

            elif bool(away_regex.search(tweet_text)):
                subject = away

            else:
                subject = "neither"

            tweet_text = tweet_text.replace(",", " ").replace("\n", " ")

            if (subject == home):
                write_record(home_file, str(tweet_time), tweet_text, subject)
            elif (subject == away):
                write_record(away_file, str(tweet_time), tweet_text, subject)
            elif (subject == both):
                write_record(both_file, str(tweet_time), tweet_text, subject)

        except tw.TweepError:
            print(f"tweep error, sleeping from time: {str(datetime.datetime.now())}")
            time.sleep(60 * 15)
            continue
        except StopIteration:
            break

    away_file.close()
    home_file.close()
    both_file.close()
    
games = [['ATL', 'NO', '2019-11-28 20:18:19.657311', '2019-11-28 23:41:04.136168', '1200216874547646466', '1200274989901078528', '13']]

for game in games:
    get_tweets_for_game(game[0], game[1], game[2], game[3], game[4], game[5], game[6])

print('Finished grabbing tweets for games')