from pydantic import BaseSettings
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, 'resources')
print( "CONFIG_PATH = ", CONFIG_PATH )

class Settings(BaseSettings):
    app_name: str = "easy-bitool"
    admin_email: str = "sjjang61@gmail.com"
    env = os.getenv('APP_ENV', 'local')


settings = Settings()