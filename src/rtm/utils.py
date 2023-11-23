"""Rucio Transfer Manager - utilities.
"""

import subprocess
import logging
import sys

from rtm import config

log = logging.getLogger(__name__)
cfg = config.get()

def voms_proxy() -> bool:
    """verify voms proxy."""
    try:
        timeleft = int(
            subprocess.run(
                ['voms-proxy-info', '-timeleft'], 
                capture_output=True,
                check=True,
            ).stdout
        )
    except subprocess.CalledProcessError:
        timeleft = 0
    
    if timeleft < cfg.min_timeleft:
        try:
            subprocess.run(
                [ "voms-proxy-init" ] + cfg.voms_proxy_args,
                check = True
            )
        except subprocess.CalledProcessError:
            return False

    return  True
