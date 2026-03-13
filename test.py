from dataclasses import dataclass

import sqlparams


@dataclass
class UserParams:
    user_id: int
    status: str


params = UserParams(user_id=1, status="active")

params = {"user_id": 1, "status": "active"}
# Define the conversion (e.g., from 'named' to 'qmark' for SQLite)
query_tool = sqlparams.SQLParams("named", "qmark")

sql, values = query_tool.format("SELECT * FROM users WHERE id = :user_id", params)
print(f"SQL: {sql}")  # SELECT * FROM users WHERE id = ?
print(f"Params: {values}")  # [1]
print(type(values))
