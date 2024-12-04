DROP TABLE ranking;
CREATE TABLE ranking (
	pokemon_id INTEGER,
	pokemon_name TEXT,
	greatness_metric DECIMAL,
	type_1 TEXT,
	type_2 TEXT,
	PRIMARY KEY(pokemon_name)
);

DROP TYPE event_type;
CREATE TYPE event_type AS ENUM (
	'world',
	'international',
	'national',
	'regional',
	'special',
	'other'
);

DROP TYPE placement_type;
CREATE TYPE placement_type AS ENUM (
	'winner',
	'runner-up',
	'top8',
	'other'
);

WITH with_event_type AS (
	SELECT *,
		CASE
			WHEN LOWER(event_name) LIKE '%world%'
			THEN 'world'
			WHEN LOWER(event_name) LIKE '%international%'
			THEN 'international'
			WHEN LOWER(event_name) LIKE '%national%'
			THEN 'national'
			WHEN LOWER(event_name) LIKE '%regional%'
			THEN 'regional'
			WHEN LOWER(event_name) LIKE '%special%'
			THEN 'special'
		ELSE 'other' END::event_type AS event_type
	FROM events
), with_placement_type AS (
	SELECT *,
		CASE
			WHEN placement = 1
			THEN 'winner'
			WHEN placement = 2
			THEN 'runner-up'
			WHEN placement <= 8
			THEN 'top8'
		ELSE 'other' END::placement_type AS placement_type
	FROM pokemon_stats
), joined_status AS (
	SELECT s.pokemon_name, s.placement_type, e.event_type, e.format FROM with_placement_type s
	LEFT JOIN with_event_type e
	ON s.event_id = e.event_id
), with_longevity AS (
	SELECT pokemon_name, COUNT(DISTINCT format) AS longevity FROM joined_status
	WHERE placement_type <> 'other'
	GROUP BY pokemon_name
), joined_status_with_longevity AS (
	SELECT s.pokemon_name, s.event_type, s.placement_type, s.format, COUNT(*) AS n, l.longevity AS longevity FROM joined_status s
	LEFT JOIN with_longevity l
	ON s.pokemon_name = l.pokemon_name
	GROUP BY(s.pokemon_name, s.placement_type, s.event_type, s.format, l.longevity)
	ORDER BY (s.pokemon_name, s.format, s.event_type, s.placement_type, -1)
), with_points AS (
	SELECT
		*,
		CASE
			WHEN event_type = 'world' THEN 7
			WHEN event_type = 'international' THEN 5
			WHEN event_type = 'national' THEN 3
			ELSE 1
		END AS event_type_points,
		CASE
			WHEN placement_type = 'winner' THEN 4
			WHEN placement_type = 'runner-up' THEN 2
			WHEN placement_type = 'top8' THEN 1
			ELSE 0
		END AS placement_type_multiplier
	FROM joined_status_with_longevity
), with_greatness_metric AS (
	SELECT
		pokemon_name,
		event_type_points * placement_type_multiplier * n * (1 + CAST(longevity AS FLOAT)/10) AS greatness_metric
	FROM with_points
), with_summed_greatness_metric AS (
	SELECT pokemon_name, SUM(greatness_metric) AS greatness_metric
	FROM with_greatness_metric
	GROUP BY pokemon_name
), pokemon_ranking AS (
	SELECT p.pokemon_id, r.pokemon_name, r.greatness_metric, p.type_1, p.type_2 FROM with_summed_greatness_metric r
	LEFT JOIN pokemon p
	ON r.pokemon_name = p.pokemon_name
	WHERE greatness_metric IS NOT NULL
	ORDER BY greatness_metric DESC
)

INSERT INTO ranking
SELECT * FROM pokemon_ranking
ORDER BY greatness_metric DESC