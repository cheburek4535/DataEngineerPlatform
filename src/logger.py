import logging
import logging.handlers
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent  # Уровень выше от файла логгера
LOG_DIR = PROJECT_ROOT / "logs"

class SmartLogger:
    """
    Умный логгер - баланс между функциональностью и простотой
    """

    def __init__(self, name=None, log_level="INFO"):
        self.name = name or __name__
        self.log_level = getattr(logging, log_level.upper())
        self.log_dir = LOG_DIR

        # Создаем директорию для логов если ее нет
        self.log_dir.mkdir(exist_ok=True)

        # Создаем логгер
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)

        self.logger.propagate = False
        self.logger.handlers.clear()

        # Очищаем старые обработчики чтобы избежать дублирования

        self.logger.handlers.clear()

        # Настраиваем форматы и обработчики
        self._setup_handlers()

    def _setup_handlers(self):
        """Настраиваем обработчики - только самые нужные"""

        # 1. КОНСОЛЬ - цветной и красивый
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(self._get_console_formatter())
        self.logger.addHandler(console_handler)

        # 2. ФАЙЛ - подробный с ротацией
        file_handler = logging.handlers.RotatingFileHandler(
            filename=self.log_dir / "app.log",
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)  # В файл пишем всё
        file_handler.setFormatter(self._get_file_formatter())
        self.logger.addHandler(file_handler)

    def _get_console_formatter(self):
        """Форматтер для консоли с цветами"""
        return ColoredFormatter(
            fmt='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )

    def _get_file_formatter(self):
        """Форматтер для файла с подробной информацией"""
        return logging.Formatter(
            fmt='%(asctime)s | %(name)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def get_logger(self):
        """Возвращает настроенный логгер"""
        return self.logger


class ColoredFormatter(logging.Formatter):
    """Простой форматтер с цветами"""

    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        log_message = super().format(record)

        if record.levelname in self.COLORS:
            return f"{self.COLORS[record.levelname]}{log_message}{self.RESET}"
        return log_message


# Функция для быстрого создания логгера
def setup_logger(name=None, log_level="INFO"):
    """
    Быстрая настройка логгера

    Использование:
    logger = setup_logger(__name__)
    logger.info("Сообщение")
    """
    return SmartLogger(name, log_level).get_logger()


# Пример использования
if __name__ == "__main__":
    logger = setup_logger("MyApp", "DEBUG")

    logger.debug("Отладочная информация")
    logger.info("Обычное сообщение")
    logger.warning("Предупреждение!")
    logger.error("Ошибка!")
    logger.critical("Критическая ошибка!")

logger = setup_logger(__name__)
