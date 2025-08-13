from pydantic_settings import BaseSettings, SettingsConfigDict


class GoogleDriveServiceAccountInfo(BaseSettings):
    """Pydantic model for Google Drive Service Account Info.

    The class properties are dynamically extracted from the environment
    variables prefixed with "GOOGLE_".
    """

    model_config = SettingsConfigDict(env_prefix="GOOGLE_")

    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str
    universe_domain: str
