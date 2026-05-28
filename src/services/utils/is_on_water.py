from typing import Optional

import requests

from logger import logger


def is_on_water(lat: int, lon: int) -> Optional[bool]:
    try:
        data = requests.get(f"https://is-on-water.balbona.me/api/v1/get/{lat}/{lon}")
        data.raise_for_status()
        if data.status_code != 200:
            logger.warning(f"IsOnWater returned status code {data.status_code}")
            if data.status_code == 429:
                logger.error(f"IsOnWater returned 429 status code {data.status_code}, break")
            return None
        result = data.json()
        if result['isWater']:
            return True
        else:
            return False
    except Exception as e:
        logger.error(e)
        raise e
