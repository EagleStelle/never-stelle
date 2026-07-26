from __future__ import annotations

from pydantic import BaseModel


class AuthSessionResponse(BaseModel):
    authenticated: bool
    username: str = ""


class LoginPayload(BaseModel):
    username: str = ""
    password: str = ""


class CredentialsPayload(BaseModel):
    username: str = ""
    current_password: str = ""
    new_password: str = ""

