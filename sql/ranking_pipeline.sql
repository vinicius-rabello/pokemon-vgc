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
			WHEN event_type = 'world' THEN 5
			WHEN event_type = 'international' THEN 3
			WHEN event_type = 'national' THEN 2
			ELSE 1
		END AS event_type_multiplier,
		CASE
			WHEN placement_type = 'winner' THEN 7
			WHEN placement_type = 'runner-up' THEN 4
			WHEN placement_type = 'top8' THEN 1
			ELSE 0
		END AS placement_type_points
	FROM joined_status_with_longevity
), with_greatness_metric AS (
	SELECT
		pokemon_name,
		event_type_multiplier * placement_type_points * n * (1 + CAST(longevity AS FLOAT)/10) AS greatness_metric,
		longevity
	FROM with_points
), with_summed_greatness_metric AS (
	SELECT pokemon_name, SUM(greatness_metric) AS greatness_metric, MIN(longevity) AS longevity
	FROM with_greatness_metric
	GROUP BY pokemon_name
), pokemon_ranking AS (
	SELECT p.pokemon_id, r.pokemon_name, r.greatness_metric, p.type_1, p.type_2, r.longevity FROM with_summed_greatness_metric r
	LEFT JOIN pokemon p
	ON r.pokemon_name = p.pokemon_name
	WHERE greatness_metric IS NOT NULL
), corrected_pokemon_ranking AS (
	SELECT p.pokemon_id, p.pokemon_name, ROUND(SUM(r.greatness_metric)::numeric, 2) AS greatness_metric, p.type_1, p.type_2, MAX(r.longevity) AS longevity FROM pokemon_ranking r
	LEFT JOIN pokemon p
	ON r.pokemon_name = p.pokemon_name
	OR r.pokemon_name LIKE CONCAT('% ', p.pokemon_name)
	OR r.pokemon_name LIKE CONCAT(p.pokemon_name, ' %')
	GROUP BY (p.pokemon_id, p.pokemon_name, p.type_1, p.type_2)
), with_times_won AS (
	SELECT pokemon_name, placement_type, event_type, SUM(n) AS times_won FROM joined_status_with_longevity
	WHERE placement_type = 'winner'
	GROUP BY pokemon_name, event_type, placement_type
), pivoted_with_times_won AS (
	SELECT 
	    pokemon_name,
		SUM(CASE WHEN event_type = 'world' THEN times_won ELSE 0 END) AS times_won_world,
	    SUM(CASE WHEN event_type = 'international' THEN times_won ELSE 0 END) AS times_won_national,
	    SUM(CASE WHEN event_type = 'national' THEN times_won ELSE 0 END) AS times_won_international,
	    SUM(CASE WHEN event_type = 'regional' THEN times_won ELSE 0 END) AS times_won_regional,
	    SUM(CASE WHEN event_type = 'special' OR event_type = 'other' THEN times_won ELSE 0 END) AS times_won_other
	FROM with_times_won
	GROUP BY pokemon_name
), corrected_pivoted_with_times_won AS (
	SELECT
		p.pokemon_id,
		p.pokemon_name,
		SUM(t.times_won_world) AS times_won_world,
		SUM(t.times_won_international) AS times_won_international,
		SUM(t.times_won_national) AS times_won_national,
		SUM(t.times_won_regional) AS times_won_regional,
		SUM(t.times_won_other) AS times_won_other
	FROM pivoted_with_times_won t
	LEFT JOIN pokemon p
	ON t.pokemon_name = p.pokemon_name
	OR t.pokemon_name LIKE CONCAT('% ', p.pokemon_name)
	OR t.pokemon_name LIKE CONCAT(p.pokemon_name, ' %')
	GROUP BY (p.pokemon_id, p.pokemon_name)
	ORDER BY times_won_world DESC
), final_ranking AS (
	SELECT
		r.*,
		COALESCE(t.times_won_world, 0) AS times_won_world,
		COALESCE(t.times_won_international, 0) AS times_won_international,
		COALESCE(t.times_won_national, 0) AS times_won_national,
		COALESCE(t.times_won_regional, 0) AS times_won_regional,
		COALESCE(t.times_won_other, 0) AS times_won_other
	FROM corrected_pokemon_ranking r
	LEFT JOIN corrected_pivoted_with_times_won t
	ON r.pokemon_id = t.pokemon_id
), final_ranking_with_gen AS (
	SELECT 
		*,
		CASE
			WHEN pokemon_id IS NULL THEN NULL
			WHEN pokemon_id <= 151 THEN 1
			WHEN pokemon_id <= 251 THEN 2
			WHEN pokemon_id <= 386 THEN 3
			WHEN pokemon_id <= 493 THEN 4
			WHEN pokemon_id <= 649 THEN 5
			WHEN pokemon_id <= 721 THEN 6
			WHEN pokemon_id <= 809 THEN 7
			WHEN pokemon_id <= 905 THEN 8
			ELSE 9 END AS gen
	FROM final_ranking
)
INSERT INTO ranking
SELECT
	fr.pokemon_id,
	fr.pokemon_name,
	fr.type_1,
	fr.type_2,
	fr.greatness_metric,
	fr.gen,
	fr.longevity,
	fr.times_won_world,
	fr.times_won_international,
	fr.times_won_national,
	fr.times_won_regional,
	fr.times_won_other,
	p.img_url
FROM final_ranking_with_gen fr
LEFT JOIN pokemon p ON fr.pokemon_id = p.pokemon_id
WHERE fr.pokemon_id IS NOT NULL
ORDER BY greatness_metric DESC