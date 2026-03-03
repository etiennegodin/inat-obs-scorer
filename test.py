from pprint import pprint

pprint(
    [
        {
            "id": "UBIGINT",
            "uuid": "VARCHAR",
            "created_at": "VARCHAR",
            "user": {
                "id": "UBIGINT",
                "login": "VARCHAR",
                "observations_count": "UBIGINT",
                "identifications_count": "UBIGINT",
                "species_count": "UBIGINT",
            },
            "body": "VARCHAR",
            "category": "VARCHAR",
            "current": "BOOLEAN",
            "own_observation": "BOOLEAN",
            "vision": "BOOLEAN",
            "disagreement": "NULL",
            "previous_observation_taxon_id": "NULL",
            "taxon_id": "UBIGINT",
        }
    ],
    sort_dicts=False,
)
