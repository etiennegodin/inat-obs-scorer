from dataclasses import dataclass, field
from typing import Union


def fields_to_string(fields_dict: dict, level=0):
    parts = []
    for key, value in fields_dict.items():
        if isinstance(value, dict):
            nested = fields_to_string(value, level + 1)
            parts.append(f"{key}:({nested})")
        elif value is True:
            parts.append(f"{key}:!t")
    return ",".join(parts)


@dataclass
class EndpointConfig:
    endpoint: str
    fields: dict = field(default_factory=dict)
    params: dict = field(default_factory=dict)
    per_page: Union[int, None] = 200
    chunk_size: int = 200
    id_param: Union[str, None] = None
    api_version: int = 2

    def __post_init_(self):
        if self.id_param is None:
            self.url = (
                f"https://api.inaturalist.org/{self.api_version}/{self.endpoint}/"
            )
        else:
            self.url = f"https://api.inaturalist.org/{self.api_version}/{self.endpoint}"

        # Convert fields dict to request format
        self.fields = f"({fields_to_string(self.fields)})"
