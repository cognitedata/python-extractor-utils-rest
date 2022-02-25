import logging
from base64 import b64encode
from dataclasses import dataclass
from typing import Any, Optional

from cognite.extractorutils.authentication import Authenticator, AuthenticatorConfig
from cognite.extractorutils.exceptions import InvalidConfigError


@dataclass
class BasicAuthConfig:
    username: Optional[str]
    password: Optional[str]


@dataclass
class AuthConfig:
    basic: Optional[BasicAuthConfig]
    oauth: Optional[AuthenticatorConfig]


def _number_of_not_nones(*args: Any) -> int:
    none_checks = [i is not None for i in args]
    return sum(none_checks)


class AuthenticationProvider:
    def __init__(self, config: Optional[AuthConfig]):
        self.config = config
        self.authenticator: Optional[Authenticator] = None
        self.logger = logging.getLogger()

        if self.config is not None:
            if _number_of_not_nones(self.config.oauth, self.config.basic) != 1:
                raise InvalidConfigError(f"One of {AuthConfig.__dataclass_fields__.keys()} is required for auth")

            if self.config.oauth:
                self.authenticator = Authenticator(self.config.oauth)

    @property
    def is_configured(self) -> bool:
        return self.config is not None

    @property
    def auth_header(self) -> str:
        if self.config is None:
            raise InvalidConfigError("No auth configured")

        if self.config.basic:
            self.logger.info("Using basic auth")
            token = b64encode(f"{self.config.basic.username or ''}:{self.config.basic.password or ''}".encode("utf8"))
            return f"Basic {token.decode('utf8')}"

        if self.config.oauth:
            if not self.authenticator:
                # Will never happen, but to appease mypy
                raise ValueError("Illegal stage: no authenticator when oauth2 is configured")
            self.logger.info("Using OAuth2")
            return f"Bearer {self.authenticator.get_token()}"

        raise RuntimeError("Unexpected error: no auth config defined")
