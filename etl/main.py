# needed libraries for scraping
from bs4 import BeautifulSoup
import pandas as pd
import requests
from tqdm import tqdm

# needed libraries to read env variables
from dotenv import load_dotenv
import os

# needed libraries for connecting to postgres
from sqlalchemy import create_engine
import psycopg2

# parsing the webpage with all the events
print("Parsing the webpage with all events...")
url = "https://limitlessvgc.com/events/?time=all&type=all&region=all&format=all"
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

# getting the table with all the data
table = soup.find(class_="table")
thead = table.find("thead")
tbody = table.find("tbody")

# this function gets a row <tr> from the <table> and transforms it into a row for our dataframe
def get_row_data(row):
  row = row.findAll("td")
  row[0] = row[0].text
  row[1] = row[1].find("span")["title"]
  event_url = "https://limitlessvgc.com/events" + row[2].find("a")["href"]
  event_id = int(event_url.split('/')[-1])
  row[2] = row[2].text
  row[3] = row[3].text
  row[4] = row[4].text
  row.append(event_id)
  return row

# defines the headers (column names) and extracts data from all the rows in the table
print("Extracting table rows...")
headers = ['date', 'event_country', 'name', 'num_players', 'winner', 'event_id']
rows = [get_row_data(tr) for tr in tbody.findAll("tr")]

# creates a tournaments_df with the data extracted
print("Creating tournaments DataFrame...")
tournaments_df = pd.DataFrame(data=rows, columns=headers)

# gets a list of all event urls from our tournaments_df
event_id_list = tournaments_df['event_id'].tolist()
event_url_list = [f"https://limitlessvgc.com/events/{id}/" for id in event_id_list]

# this functions is just like the get_row_data, but for the specific event table
def get_event_row_data(row):
  row = row.findAll("td")
  row[0] = row[0].text # get the placement
  row[1] = row[1].text # get the player name
  row[2] = row[2].find("span")["title"] # get the player country
  pokemon_team_data = row[3].findAll("span")
  pokemon_team = [pkmn["title"] for pkmn in pokemon_team_data]
  row[3] = pokemon_team
  return row[:-1]

# this function builds a dataframe with the standings of given event url
def build_event_df(event_url):
  # parse the html page
  page = requests.get(event_url)
  soup = BeautifulSoup(page.content, "html.parser")

  # get the table body with all the data
  table = soup.find(class_="table")
  tbody = table.find("tbody")

  # get the headers for our event df
  headers = ['event_id', 'placement', 'player', 'country', 'team']
  event_id = int(event_url.split('/')[-2])
  rows = [[event_id] + get_event_row_data(row) for row in tbody.findAll("tr")]
  df = pd.DataFrame(data=rows, columns=headers)
  return df

# iterates over event_url_list, creates a dataframe for every event and concatenates all of them
print("Building event-specific DataFrames...")
def build_pokemon_stats_df(event_url_list):
  df = build_event_df(event_url_list[0])
  print(f"Processed: {event_url_list[0]}")
  for event_url in tqdm(event_url_list[1:]):
    df = pd.concat([df, build_event_df(event_url)], ignore_index=True)
    print(f"Processed: {event_url}")
  return df

# builds pokemon_stats_df
print("Generating pokemon_stats DataFrame...")
pokemon_stats_df = build_pokemon_stats_df(event_url_list)

# we explode the team column (instead of having a team column with [pikachu, charmandar, ...]
# it creates a row for each of the pokemon)
print("Exploding team column...")
pokemon_stats_df = pokemon_stats_df.explode('team') # explode the team column
pokemon_stats_df.reset_index(drop=True, inplace=True) # reset the indexes
pokemon_stats_df.rename(columns={"team": "pokemon"}, inplace=True) # rename the column from team to pokemon
pokemon_stats_df = pokemon_stats_df.loc[pokemon_stats_df['pokemon'] != ""] # drop all rows with empty pokemon

# this function gets the ruleset for given event_url
def get_ruleset_from_event_id(event_id):
  event_url = f"https://limitlessvgc.com/events/{event_id}/"
  page = requests.get(event_url)
  soup = BeautifulSoup(page.content, "html.parser")

  infobox = soup.find(class_="infobox-text")
  format = infobox.find("a").text
  print(f"Fetched ruleset for event_id {event_id}")
  return format

# apply that function, creating a column format from the event_ids
print("Fetching ruleset for events...")
tournaments_df['format'] = tournaments_df['event_id'].apply(lambda id: get_ruleset_from_event_id(id))

# remove rows with empty format
print("Cleaning and mapping formats...")
tournaments_df = tournaments_df.loc[tournaments_df['format'] != ""]

# this dict maps the format to the corresponding generation
format_to_gen_dict = {
  "VGC 2010": "gen_4",
  "VGC 2011": "gen_5",
  "VGC 2012": "gen_5",
  "VGC 2013": "gen_5",
  "VGC 2014": "gen_6",
  "VGC 2015": "gen_6",
  "VGC 2016": "gen_6",
  "VGC 2017": "gen_7",
  "VGC 2018": "gen_7",
  "Sun Series": "gen_7",
  "Moon Series": "gen_7",
  "Ultra Series": "gen_7",
  "VGC 2020": "gen_8",
  "VGC 2022": "gen_8",
  "Scarlet & Violet - Regulation A": "gen_9",
  "Scarlet & Violet - Regulation B": "gen_9",
  "Scarlet & Violet - Regulation C": "gen_9",
  "Scarlet & Violet - Regulation D": "gen_9",
  "Scarlet & Violet - Regulation E": "gen_9",
  "Scarlet & Violet - Regulation F": "gen_9",
  "Scarlet & Violet - Regulation G": "gen_9",
  "Scarlet & Violet - Regulation H": "gen_9",
}

# applies the function to map the format to the gen
tournaments_df['format'] = tournaments_df['format'].apply(lambda x: format_to_gen_dict[x])

# now we'll need a dataframe containing all pokemon, here we parse the bulbapedia page containing a list of all pokemon
print("Parsing the webpage with all pokemon...")
url = "https://bulbapedia.bulbagarden.net/wiki/List_of_Pok%C3%A9mon_by_name"
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

# this function builds a dataframe with all pokemon in that webpage
def build_pokemon_df():
    pokemon_data = []
    tables = soup.findAll("table")
    for table in tqdm(tables):
        try: # not all tables are holding pokemon data, there is some misc tables we do this to no deal with them
            rows = table.findAll("tr")[1:]
            for row in rows:
                entries = row.findAll("td")
                pokemon_id = int(entries[0].text.replace("#", ""))
                pokemon_name = entries[2].text
                pokemon_type_1 = entries[3].text.replace("\n", "")
                if len(entries) > 4: # check if pokemon has secondary type (if it has the table has one more <td>)
                    pokemon_type_2 = entries[4].text.replace("\n", "")
                else: pokemon_type_2 = None
                pokemon_img_url = entries[1].find("img")["src"]
                pokemon_data.append([pokemon_id, pokemon_name, pokemon_type_1, pokemon_type_2, pokemon_img_url])
        except:
            continue

    pokemon_df = pd.DataFrame(data=pokemon_data, columns=["pokemon_id", "name", "type_1", "type_2", "img_url"])
    pokemon_df = pokemon_df.sort_values(by="pokemon_id").reset_index(drop=True)
    return pokemon_df

# creates the pokemon_df
print("Building pokemon DataFrame...")
pokemon_df = build_pokemon_df()

# orders and renames the columns from tournaments_df to match our psql table
print("Cleaning and formatting DataFrames...")
tournaments_df = tournaments_df[['event_id', 'date', 'event_country', 'name', 'num_players', 'winner', 'format']]
tournaments_df = tournaments_df.rename(columns={"date": "event_date", "name": "event_name"})

# orders and renames the columns from pokemon_stats_df to match our psql table
pokemon_stats_df = pokemon_stats_df.rename(columns={"player": "player_name", "country": "player_country", "pokemon": "pokemon_name"})
pokemon_stats_df = pokemon_stats_df.drop_duplicates(subset=["event_id", "placement", "player_name", "pokemon_name"])

# renames a column from pokemon_df to match our psql table
pokemon_df = pokemon_df.rename(columns={"name": "pokemon_name"})

# access env variables
print("Connecting to database and populating tables...")
load_dotenv()

# define sslmode
sslmode = "require" if os.getenv("DB_SSL", "false").lower() == "true" else "disable"

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "sslmode": sslmode
}

# connects to postgres db
conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(conn_str)
conn = psycopg2.connect(**DB_CONFIG)

# this function executes a sql file on a given connections
def execute_sql_file(connection, sql_file_path):
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()
    with connection.cursor() as cursor:
        cursor.execute(sql_script)
    connection.commit()

# drops existing tables and then re-creates them
execute_sql_file(conn, './sql/drop_existing_tables.sql')
execute_sql_file(conn, './sql/ddl.sql')

# populates the created tables
tournaments_df.to_sql("events", engine, if_exists="append", index=False)
pokemon_df.to_sql("pokemon", engine, if_exists="append", index=False)
pokemon_stats_df.to_sql("pokemon_stats", engine, if_exists="append", index=False)

# executes sql script that populates the ranking table
execute_sql_file(conn, './sql/ranking_pipeline.sql')