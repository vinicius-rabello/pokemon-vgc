DROP TABLE pokemon;
DROP TABLE events;
DROP TABLE pokemon_stats;

CREATE TABLE pokemon (
    pokemon_id INTEGER,
    pokemon_name TEXT,
    type_1 TEXT,
    type_2 TEXT,
    PRIMARY KEY(pokemon_id)
);

CREATE TABLE events (
    event_id INTEGER,
    event_date TEXT,
    event_country TEXT,
    event_name TEXT,
    num_players INTEGER,
    winner TEXT,
    format TEXT,
    PRIMARY KEY(event_id)
);

CREATE TABLE pokemon_stats (
    event_id INTEGER,
    placement INTEGER,
    player_name TEXT,
    player_country TEXT,
    pokemon_name TEXT,
    PRIMARY KEY(event_id, placement, player_name, pokemon_name)
);