import unittest
from unittest.mock import MagicMock

from cognite.extractorutils.authentication import AuthenticatorConfig
from cognite.extractorutils.exceptions import InvalidConfigError

from cognite.extractorutils.rest.authentiaction import AuthConfig, AuthenticationProvider, BasicAuthConfig

oauth_config = AuthenticatorConfig(
    client_id="id", scopes=["scp"], secret="verysecret", token_url="https://localhost:8050/token"
)

basic_config = BasicAuthConfig(
    username="user",
    password="pass",
)


class TestAuthenticationProvider(unittest.TestCase):
    def test_config_validation(self) -> None:
        with self.assertRaises(InvalidConfigError):
            AuthenticationProvider(AuthConfig(basic=None, oauth=None))

        with self.assertRaises(InvalidConfigError):
            AuthenticationProvider(AuthConfig(basic=basic_config, oauth=oauth_config))

        try:
            AuthenticationProvider(AuthConfig(basic=None, oauth=oauth_config))
            AuthenticationProvider(AuthConfig(basic=basic_config, oauth=None))
        except InvalidConfigError:
            self.fail("Incorrect fail for valid config")

    def test_basic(self) -> None:
        auth = AuthenticationProvider(AuthConfig(basic=basic_config, oauth=None))
        self.assertTrue(auth.is_configured)
        self.assertEqual(auth.auth_header, "Basic dXNlcjpwYXNz")

    def test_oauth(self) -> None:
        auth = AuthenticationProvider(AuthConfig(basic=None, oauth=oauth_config))
        self.assertTrue(auth.is_configured)

        self.assertIsNotNone(auth.authenticator)
        auth.authenticator._request = MagicMock(return_value={"expires_in": 1000, "access_token": "tokey"})
        self.assertEqual(auth.auth_header, "Bearer tokey")
        auth.authenticator._request.assert_called()
