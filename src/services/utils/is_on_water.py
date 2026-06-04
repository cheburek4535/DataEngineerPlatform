from typing import Optional

import requests
import re
from logger import logger


def is_on_water(lat: int, lon: int) -> Optional[bool]:
    try:
        data = requests.get(f"https://is-on-water.balbona.me/api/v1/get/{lat}/{lon}", timeout=5)
        data.raise_for_status()
        logger.debug(f"Response text: {data.text[:500]}")
        if data.status_code != 200:
            logger.warning(f"IsOnWater returned status code {data.status_code}")
            if data.status_code == 429:
                logger.error(f"IsOnWater returned 429 status code {data.status_code}, break")
            return None

        try:
            result = data.json()
            is_water = result['isWater']
        except Exception:
            # Если JSON битый, ищем isWater в тексте через regex
            text = data.text
            match = re.search(r'"isWater"\s*:\s*(true|false)', text, re.IGNORECASE)
            if match:
                is_water = match.group(1).lower() == 'true'
                logger.warning(f"Extracted isWater from broken JSON: {is_water}, raw: {text[:100]}")
            else:
                logger.error(f"Cannot find isWater in response: {text[:200]}")
                return None

        return is_water
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None
