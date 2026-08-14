from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_password: SecretStr

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_default_user: str
    rabbitmq_default_pass: SecretStr
    rabbitmq_default_vhost: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
