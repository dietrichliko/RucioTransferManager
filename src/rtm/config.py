"""Rucio Transfer Manager - Configuration.
"""
import pathlib
import os
import importlib
import shutil
import logging
import tomllib
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.expanduser(
    f'{os.environ.get("XDG_CONFIG_HOME", "~/.config")}' "/RucioTransferManager/rtm.toml"
)


@dataclass
class Configuration:
    """Configuration parameters."""

    db_url: str = ""
    db_echo: bool = True
    max_dasgoclient: int = 4
    max_xrdadler32: int = 10
    min_timeleft: int = 0
    voms_proxy_args: list[str] = field(default_factory=list)
    rucio_client_args: dict[str, str] = field(default_factory=dict)

    def load(self, config_path: pathlib.Path) -> None:
        """Load config from toml."""
        config_path = config_path.expanduser()
        if not config_path.exists():
            log.info("Creating %s", config_path)
            config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            cfg_path = importlib.resources.files(__package__).joinpath("rtm.toml")
            with importlib.resources.as_file(cfg_path) as cfg_file:
                shutil.copyfile(cfg_file, config_path)

        with open(config_path, "rb") as f:
            cfg = tomllib.load(f)

        self.db_url = cfg.get("db_url", "sqlite:///rtm.db")
        self.db_echo = cfg.get("db_echo", False)
        self.max_dasgoclient = cfg.get("max_dasgoclient", 4)
        self.max_dasgoclient = cfg.get("max_xrdadler32", 10)

        voms_proxy_cfg = cfg.get("voms-proxy", {})
        self.min_timeleft = voms_proxy_cfg.get("min_timeleft", 1000)
        self.voms_proxy_args = voms_proxy_cfg.get(
            "voms_proxy_args", ["-rfc", "-voms", "cms", "-valid", "96:0"]
        )

        self.rucio_client_args = cfg.get("rucio-client", {})


_config = Configuration()


def get() -> Configuration:
    """Singleton."""
    return _config
