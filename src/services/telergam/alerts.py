import os
import asyncio
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALERT_CHAT_ID = os.getenv("ADMIN_TG_ID")


bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Бот для уведомлений готов к работе")


async def send_alert(text: str, chat_id: int = None) -> None:
    target_id = chat_id if chat_id is not None else ALERT_CHAT_ID
    if not target_id:
        raise ValueError("No alert chat id configured")


    try:
            await bot.send_message(chat_id=target_id, text=text)
    except Exception as e:
            # тут лучше логировать, но не ронять весь цикл рассылки
            print(f"Failed to send alert to {target_id}: {e}")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())