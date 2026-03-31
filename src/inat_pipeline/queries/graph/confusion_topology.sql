CREATE OR REPLACE TABLE graph.confusion_topology AS

WITH base_edges AS (

    SELECT
        taxon_id,
        similar_taxon_id
    FROM staged.similar_species

),

augmented_edges AS (

    SELECT
        e.*,
        td.taxonomic_distance,
        td.taxonomic_distance_rank,
        ta.out_degree,
        ta.in_degree,
        ta.magnet_score,
        ts.rank_level AS similar_rank_level,
        ts.genus_id AS similar_genus_id,
        ts.family_id AS similar_family_id,

        CASE
            WHEN tf.genus_id = ts.genus_id THEN 1
            WHEN tf.family_id = ts.family_id THEN 2
            WHEN tf.order_id = ts.order_id THEN 3
            ELSE 4
        END AS confusion_boundary,

    FROM base_edges e
    JOIN staged.taxa_distance td
        ON
            td.taxon_id = e.taxon_id
            AND td.similar_taxon_id = e.similar_taxon_id
    JOIN staged.taxa_asymmetry ta ON ta.taxon_id = e.taxon_id
    JOIN staged.taxa tf ON tf.taxon_id = e.taxon_id
    JOIN staged.taxa ts ON ts.taxon_id = e.similar_taxon_id --join similar taxon

),

-- Clustering coefficient
neighbor_pairs AS (
    -- All pairs (b, c) where both are neighbors of focal a
    SELECT
        e1.taxon_id AS focal,
        e1.similar_taxon_id AS b, --first hop
        e2.similar_taxon_id AS c  --second hop

    FROM base_edges e1
    JOIN base_edges e2 ON e2.taxon_id = e1.taxon_id
    WHERE e1.similar_taxon_id != e2.similar_taxon_id
),

connected_pairs AS (
    -- Which of those pairs have an edge between them?
    SELECT
        p.focal,
        COUNT(*) AS connected
    FROM neighbor_pairs p
    JOIN base_edges e ON e.taxon_id = p.b AND e.similar_taxon_id = p.c
    GROUP BY p.focal
),

total_pairs AS (
    SELECT
        focal,
        COUNT(*) AS total
    FROM neighbor_pairs
    GROUP BY focal
),

clustering_coefficient AS (

    SELECT
        t.focal AS taxon_id,
        ROUND(COALESCE(c.connected, 0)::FLOAT / NULLIF(t.total, 0), 4) AS clustering_coefficient
    FROM total_pairs t
    LEFT JOIN connected_pairs c ON c.focal = t.focal
),

focal_taxon_aggs AS (
    SELECT
        taxon_id,
        COUNT(DISTINCT(similar_taxon_id)) AS similar_species_count,

        -- Taxonomic distance aggregates
        MAX(taxonomic_distance) AS nbor_dist_max,
        ROUND(AVG(taxonomic_distance), 2) AS nbor_dist_mean,
        MEDIAN(taxonomic_distance) AS nbor_dist_median,

        COUNT(DISTINCT(similar_genus_id)) AS single_hop_genus_diversity,
        COUNT(DISTINCT(similar_family_id)) AS single_hop_family_diversity,
        MAX(similar_rank_level) single_hop_rank_max,
        MIN(similar_rank_level) single_hop_rank_min,

        MAX(confusion_boundary) AS max_confusion_boundary_crossed,

        -- Fraction of confusers that cross genus boundary (0→1)
        ROUND(
            COUNT(*) FILTER (WHERE confusion_boundary > 1)::FLOAT
            / NULLIF(COUNT(*), 0),
            4
        ) AS cross_genus_confusion_rate,

    FROM augmented_edges e
    GROUP BY taxon_id
)

SELECT

    f.*,
    c.clustering_coefficient

FROM focal_taxon_aggs f
LEFT JOIN clustering_coefficient c ON c.taxon_id = f.taxon_id
