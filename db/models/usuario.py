from pydantic import BaseModel

class Usuario(BaseModel):
    name: str
    username: str
    password: str
    keywords_count: int
    user_type: str