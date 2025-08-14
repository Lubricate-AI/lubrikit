from pydantic_settings import BaseSettings, SettingsConfigDict


class GoogleDriveAPIServiceAccountInfo(BaseSettings):
    """Model for Google Drive API Service Account authentication.

    This class defines the complete set of service account credentials
    required to authenticate with the Google Drive API. All fields are
    automatically populated from environment variables prefixed with
    "GOOGLE_". This follows the standard Google Cloud service account
    JSON format.

    Environment Variables:
        All attributes are loaded from environment variables with the
        "GOOGLE_" prefix:
        - GOOGLE_TYPE: Service account type identifier
        - GOOGLE_PROJECT_ID: Google Cloud project identifier
        - GOOGLE_PRIVATE_KEY_ID: Service account private key identifier
        - GOOGLE_PRIVATE_KEY: RSA private key for authentication
        - GOOGLE_CLIENT_EMAIL: Service account email address
        - GOOGLE_CLIENT_ID: Unique client identifier
        - GOOGLE_AUTH_URI: OAuth2 authorization endpoint
        - GOOGLE_TOKEN_URI: OAuth2 token endpoint
        - GOOGLE_AUTH_PROVIDER_X509_CERT_URL: Certificate authority URL
        - GOOGLE_CLIENT_X509_CERT_URL: Client certificate URL
        - GOOGLE_UNIVERSE_DOMAIN: Google Cloud universe domain

    Attributes:
        type (str): The type of Google Cloud credential. Should always
            be "service_account" for service account authentication.
        project_id (str): The Google Cloud project ID where the service
            account was created.
            Example: "my-project-123456"
        private_key_id (str): Unique identifier for the service
            account's private key.
            Example: "abc123def456"
        private_key (str): The RSA private key in PEM format used for
            JWT signing. This is a multi-line string starting with
            "-----BEGIN PRIVATE KEY-----".
        client_email (str): The service account's email address.
            Example:
            "my-service@my-project-123456.iam.gserviceaccount.com"
        client_id (str): Unique numerical identifier for the service
            account.
            Example: "123456789012345678901"
        auth_uri (str): OAuth2 authorization server endpoint.
            Default: "https://accounts.google.com/o/oauth2/auth"
        token_uri (str): OAuth2 token server endpoint for obtaining
            access tokens.
            Default: "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url (str): URL for the OAuth2
            certificate authority.
            Default: "https://www.googleapis.com/oauth2/v1/certs"
        client_x509_cert_url (str): URL for this service account's
            public certificate.
            Example:
            "https://www.googleapis.com/robot/v1/metadata/x509/my-service%40my-project.iam.gserviceaccount.com"
        universe_domain (str): The Google Cloud universe domain.
            Default: "googleapis.com"

    Note:
        This class automatically validates that all required environment
        variables are present and properly formatted. Missing variables
        will raise a ValidationError during instantiation.

    Example:
        Set environment variables:
        ```bash
        export GOOGLE_TYPE="service_account"
        export GOOGLE_PROJECT_ID="my-project-123456"
        export GOOGLE_PRIVATE_KEY_ID="abc123"
        # ... set other variables
        ```

        Then instantiate:
        >>> service_account = GoogleDriveAPIServiceAccountInfo()
        >>> print(service_account.project_id)
        my-project-123456
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
