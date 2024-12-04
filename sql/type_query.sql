WITH pokemon_titles AS (
	SELECT pokemon_name, COUNT(*) AS n_appearances FROM pokemon_stats
	WHERE placement = 1
	GROUP BY pokemon_name
	ORDER BY n_appearances DESC
), pokemon_titles_with_type AS (
	SELECT pt.pokemon_name, pt.n_appearances AS n_titles, p.type_1 AS type_ FROM pokemon_titles pt
	LEFT JOIN pokemon p
	ON pt.pokemon_name = p.pokemon_name
	WHERE p.type_1 IS NOT NULL
	UNION ALL
	SELECT pt.pokemon_name, pt.n_appearances AS n_titles, p.type_2 AS type_ FROM pokemon_titles pt
	LEFT JOIN pokemon p
	ON pt.pokemon_name = p.pokemon_name
	WHERE p.type_2 IS NOT NULL
)
SELECT SUM(n_titles) AS n_titles, type_ FROM pokemon_titles_with_type
GROUP BY type_
ORDER BY n_titles DESC