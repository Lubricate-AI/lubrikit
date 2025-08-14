import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from lubrikit.extract.connectors.configs.google_drive_api import (
    GoogleDriveAPIServiceAccountInfo,
)


@pytest.fixture
def valid_service_account_env() -> dict[str, str]:
    """Valid service account environment variables."""
    return {
        "GOOGLE_TYPE": "service_account",
        "GOOGLE_PROJECT_ID": "test-project-123",
        "GOOGLE_PRIVATE_KEY_ID": "key123",
        "GOOGLE_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKB\n-----END PRIVATE KEY-----\n",
        "GOOGLE_CLIENT_EMAIL": "test@test-project-123.iam.gserviceaccount.com",
        "GOOGLE_CLIENT_ID": "123456789012345678901",
        "GOOGLE_AUTH_URI": "https://accounts.google.com/o/oauth2/auth",
        "GOOGLE_TOKEN_URI": "https://oauth2.googleapis.com/token",
        "GOOGLE_AUTH_PROVIDER_X509_CERT_URL": "https://www.googleapis.com/oauth2/v1/certs",
        "GOOGLE_CLIENT_X509_CERT_URL": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project-123.iam.gserviceaccount.com",
        "GOOGLE_UNIVERSE_DOMAIN": "googleapis.com",
    }


@pytest.mark.parametrize(
    "field_name, expected",
    [
        ("type", "service_account"),
        ("project_id", "test-project-123"),
        ("private_key_id", "key123"),
        (
            "private_key",
            "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKB\n-----END PRIVATE KEY-----\n",
        ),
        ("client_email", "test@test-project-123.iam.gserviceaccount.com"),
        ("client_id", "123456789012345678901"),
        ("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
        ("token_uri", "https://oauth2.googleapis.com/token"),
        ("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
        (
            "client_x509_cert_url",
            "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project-123.iam.gserviceaccount.com",
        ),
        ("universe_domain", "googleapis.com"),
    ],
)
def test_initialization_with_environment_variables(
    valid_service_account_env: dict[str, str], field_name: str, expected: str
) -> None:
    """Test initialization from environment variables."""
    with patch.dict(os.environ, valid_service_account_env, clear=False):
        service_account = GoogleDriveAPIServiceAccountInfo()

        assert getattr(service_account, field_name) == expected


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("type", "service_account"),
        ("project_id", "explicit-project"),
        ("private_key_id", "explicit-key-id"),
        ("client_email", "explicit@example.com"),
        ("universe_domain", "example.com"),
    ],
)
def test_initialization_with_explicit_values(field_name: str, value: str) -> None:
    """Test initialization with explicit parameter values."""
    # Provide all required fields with defaults, then override the specific field being tested
    base_data = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "test-key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "123456789012345678901",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    }
    # Override the specific field being tested
    base_data[field_name] = value

    service_account = GoogleDriveAPIServiceAccountInfo(**base_data)  # type: ignore[arg-type]

    assert getattr(service_account, field_name) == value


def test_missing_required_environment_variables() -> None:
    """Test that missing required environment variables raise ValidationError."""
    # Clear all GOOGLE_ environment variables
    env_without_google = {
        k: v for k, v in os.environ.items() if not k.startswith("GOOGLE_")
    }

    with patch.dict(os.environ, env_without_google, clear=True):
        with pytest.raises(ValidationError) as exc_info:
            GoogleDriveAPIServiceAccountInfo()

        errors = exc_info.value.errors()
        # Should have multiple validation errors for missing fields
        assert len(errors) >= 10  # All fields are required

        # Check that all required fields are mentioned in errors
        error_fields = {error["loc"][0] for error in errors}
        expected_fields = {
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "auth_uri",
            "token_uri",
            "auth_provider_x509_cert_url",
            "client_x509_cert_url",
            "universe_domain",
        }
        assert expected_fields.issubset(error_fields)


@pytest.mark.parametrize(
    "missing_field",
    [
        "GOOGLE_TYPE",
        "GOOGLE_PROJECT_ID",
        "GOOGLE_PRIVATE_KEY_ID",
        "GOOGLE_PRIVATE_KEY",
        "GOOGLE_CLIENT_EMAIL",
        "GOOGLE_CLIENT_ID",
        "GOOGLE_AUTH_URI",
        "GOOGLE_TOKEN_URI",
        "GOOGLE_AUTH_PROVIDER_X509_CERT_URL",
        "GOOGLE_CLIENT_X509_CERT_URL",
        "GOOGLE_UNIVERSE_DOMAIN",
    ],
)
def test_individual_missing_fields(
    valid_service_account_env: dict[str, str], missing_field: str
) -> None:
    """Test that each individual field is required."""
    env_vars = valid_service_account_env.copy()
    del env_vars[missing_field]

    with patch.dict(os.environ, env_vars, clear=True):
        with pytest.raises(ValidationError) as exc_info:
            GoogleDriveAPIServiceAccountInfo()

        errors = exc_info.value.errors()
        error_fields = {error["loc"][0] for error in errors}

        # Convert environment variable name to field name
        field_name = missing_field.replace("GOOGLE_", "").lower()
        assert field_name in error_fields


def test_environment_prefix() -> None:
    """Test that GOOGLE_ prefix is correctly handled."""
    # Only set the GOOGLE_ prefixed variables we need for this test
    full_env = {
        "GOOGLE_TYPE": "service_account",
        "GOOGLE_PROJECT_ID": "prefix-test",
        "GOOGLE_PRIVATE_KEY_ID": "test",
        "GOOGLE_PRIVATE_KEY": "test-key",
        "GOOGLE_CLIENT_EMAIL": "test@example.com",
        "GOOGLE_CLIENT_ID": "test-id",
        "GOOGLE_AUTH_URI": "https://example.com",
        "GOOGLE_TOKEN_URI": "https://example.com",
        "GOOGLE_AUTH_PROVIDER_X509_CERT_URL": "https://example.com",
        "GOOGLE_CLIENT_X509_CERT_URL": "https://example.com",
        "GOOGLE_UNIVERSE_DOMAIN": "example.com",
        "NOT_GOOGLE_TYPE": "should_be_ignored",
    }

    with patch.dict(os.environ, full_env, clear=True):
        service_account = GoogleDriveAPIServiceAccountInfo()

        assert service_account.type == "service_account"
        assert service_account.project_id == "prefix-test"


def test_settings_config_dict() -> None:
    """Test that the settings configuration is properly set."""
    # This tests the model_config class attribute
    assert GoogleDriveAPIServiceAccountInfo.model_config["env_prefix"] == "GOOGLE_"
