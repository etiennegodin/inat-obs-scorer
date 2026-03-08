CREATE OR REPLACE MACRO community_taxon_windowed(eval_interval) AS TABLE

WITH id_window AS (
    SELECT i.*
    FROM staged.identifications i
    JOIN staged.observations o ON i.observation_id = o.id
    WHERE i."current" IS TRUE
      AND i.created_at >= o.created_at
      AND i.created_at <= o.created_at + eval_interval  -- <- window parameter
),

counts AS(

    SELECT i.observation_id,
        COUNT(*) AS total_ids,
        -- How many distinct values exist at each rank (disagreement signal)
        COUNT(DISTINCT i.class)     AS class_distinct_count,
        COUNT(DISTINCT i."order")   AS order_distinct_count,
        COUNT(DISTINCT i.family)    AS family_distinct_count,
        COUNT(DISTINCT i.genus)     AS genus_distinct_count,
        COUNT(DISTINCT i.species)   AS species_distinct_count,


        COUNT(i.class)     AS class_count,
        COUNT(i."order")   AS order_count,
        COUNT(i.family)    AS family_count,
        COUNT(i.genus)     AS genus_count,
        COUNT(i.species)   AS specie_count,


        -- Full distribution: {value: count, ...} per rank
        HISTOGRAM(i.class)          AS class_map,
        HISTOGRAM(i."order")        AS order_map,
        HISTOGRAM(i.family)         AS family_map,
        HISTOGRAM(i.genus)          AS genus_map,
        HISTOGRAM(i.species)        AS species_map,

        -- Full distribution with taxon_id
        HISTOGRAM(CASE WHEN i."taxonRank" = 'class' THEN i.taxon_id END) AS class_id_map,
        HISTOGRAM(CASE WHEN i."taxonRank" = 'order' THEN i.taxon_id END) AS order_id_map,
        HISTOGRAM(CASE WHEN i."taxonRank" = 'family' THEN i.taxon_id END) AS family_id_map,
        HISTOGRAM(CASE WHEN i."taxonRank" = 'genus' THEN i.taxon_id END) AS genus_id_map,
        HISTOGRAM(CASE WHEN i."taxonRank" = 'species' THEN i.taxon_id END) AS species_id_map,
    
        -- Top proportion for each taxonomic level
        list_max(map_values(class_map))::FLOAT / class_count::FLOAT AS class_top_proportion,
        list_max(map_values(order_map))::FLOAT / order_count::FLOAT AS order_top_proportion,
        list_max(map_values(family_map))::FLOAT / family_count::FLOAT AS family_top_proportion,
        list_max(map_values(genus_map))::FLOAT / genus_count::FLOAT AS genus_top_proportion,
        list_max(map_values(species_map))::FLOAT / specie_count::FLOAT AS species_top_proportion,


    FROM id_window i
    GROUP BY observation_id
),

with_consensus AS(

    SELECT *,
        CASE
            WHEN specie_count = genus_count  THEN 'species'
            WHEN genus_count   = family_count THEN 'genus'
            WHEN family_count  = order_count  THEN 'family'
            ELSE 'order'
        END AS consensus_level,
    FROM counts

)

SELECT 
    observation_id,
    consensus_level,
    consensus_level = 'species' AS consensus_level_rg,
    CASE consensus_level
        WHEN 'species' THEN species_id_map
        WHEN 'genus'   THEN genus_id_map
        WHEN 'family'  THEN family_id_map
        WHEN 'order'   THEN order_id_map
        ELSE class_id_map
    END AS consensus_level_histogram,

    -- Extract the winning taxon cleanly (argmax of map)
    list_max(map_values(
        CASE consensus_level
            WHEN 'species' THEN species_id_map
            WHEN 'genus'   THEN genus_id_map
            WHEN 'family'  THEN family_id_map
            ELSE order_id_map
        END
    )) AS top_taxon_count,
    -- community taxon = key with max votes (no unnest fan-out)
    map_keys(consensus_level_histogram)[
        list_indexof(map_values(consensus_level_histogram), top_taxon_count)
    ] AS community_taxon

FROM with_consensus 
