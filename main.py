import asyncio
from datetime import datetime, timedelta
from itertools import cycle
import json
import logging

from aiogram import Bot, Dispatcher, executor, types
from aiogram import types
import aiogram
from config import ADMINS, BOT_TOKEN
from filters import Admin
from middlewares import UsersMiddleware
from aiogram.dispatcher import FSMContext
import csv

from aiogram.types import InputFile, Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from models.settings import Setting
from users import count_users, delete_user, get_user, get_user_ids, get_users
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.date import DateTrigger
from aiogram.utils.exceptions import BotBlocked
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%H:%M:%S')

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}
scheduler = AsyncIOScheduler(jobstores=jobstores)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(UsersMiddleware())
dp.filters_factory.bind(Admin)

@dp.message_handler(commands=["id"])
async def get_id(message: types.Message):
    await message.answer(message.from_user.id)

class MailingStates(StatesGroup):
    msg = State()
    idle = State()
    change_kb = State()
    delete_time = State()
    amount = State()
    fast = State()

class StartMailingStates(StatesGroup):
    msg = State()
    idle = State()
    change_kb = State()
class ChangeDeleteKbStates(StatesGroup):
    date = State()

def get_admin_markup():
    markup = InlineKeyboardMarkup(row_width=1)

    markup.add(InlineKeyboardButton("Скачать БД 📁", callback_data="get_db"))
    markup.add(InlineKeyboardButton("Посчитать пользователей 👥", callback_data="get_users"))
    markup.add(InlineKeyboardButton("Почистить неактивных", callback_data="clear_users"))
    # markup.add(InlineKeyboardButton("Посчитать пользователей БЫСТРО 👥 (beta)", callback_data="get_users_fast"))
    markup.add(InlineKeyboardButton("Сделать рассылку 📬", callback_data="make_mail"))
    markup.add(InlineKeyboardButton("Изменить начальное сообщение ✉️", callback_data="change_default"))
    markup.add(InlineKeyboardButton("Настроить удаление начального", callback_data="change_delete_kb"))
    change_start_text = ""
    if Setting.get_many(["send_start"])[0] == "0":
        change_start_text = "Включить начальное сообщение"
    else:
        change_start_text = "Выключить начальное сообщение"
    markup.add(InlineKeyboardButton(change_start_text, callback_data="change_start"))

    return markup

@dp.callback_query_handler(text="change_delete_kb", is_admin=True)
async def _change_delete_kb(call: CallbackQuery):
    await ChangeDeleteKbStates.date.set()
    await call.answer()
    await call.message.answer("Введите время, через которое удалить сообщение, в формате гг:мм:сс. Чтобы сообщение не удалялось, напишите 0")

@dp.message_handler(is_admin=True, state=ChangeDeleteKbStates.date)
async def _confirm_make_mail(message: Message, state: FSMContext):
    if message.text == "0":
        Setting.set_many({
            "start_delete": "0"
        })
        await message.answer("Успешно!")
        await state.finish()
        return
    try:
        time = datetime.strptime(message.text, "%H:%M:%S")        
    except:
        await message.answer("Неправильный формат")
        return
    Setting.set_many({
        "start_delete": message.text
    })
    await state.finish()
    await message.answer("Успешно!")

# =======

@dp.message_handler(is_admin=True, commands=["test"])
async def _confirm_make_mail(message: Message, state: FSMContext):
    msg = await message.answer(f"Считаем..")
    users = get_users()
    active = 0
    count = 0
    for user in users:
        if count % 10 == 0:
            await msg.edit_text(f"Считаем.. {count}, {active}")
        count += 1
        try:
            if await bot.send_chat_action(user.id, "typing"):
                active += 1
            # await asyncio.sleep(0.2)
        except Exception as e:
            print(e)
            if "Retry" in e.__class__.__name__:
                print(e)

    await message.answer(f"Общее количество: {count}\nАктивных пользователей: {active}")


@dp.callback_query_handler(text="change_start", is_admin=True)
async def _change_start(call: CallbackQuery):
    if Setting.get_many(["send_start"])[0] == "0":
        Setting.set_many({"send_start":"1"})
        text = "Вы включили начальное сообщение!"
    else:
        Setting.set_many({"send_start":"0"})
        text = "Вы выключили начальное сообщение!"
    await call.message.edit_text(text, reply_markup=get_admin_markup())


@dp.callback_query_handler(text="change_default", is_admin=True)
async def _change_default(call: CallbackQuery):
    await StartMailingStates.msg.set()
    await call.answer()
    await call.message.answer("Отправьте сообщение для начального сообщения")

def get_start_mail_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Изменить клавиатуру ⌨️", callback_data="change_start_kb"))
    kb.add(InlineKeyboardButton("Закончить ✅", callback_data="end_start_mail"))
    return kb

@dp.message_handler(is_admin=True, state=StartMailingStates.msg, content_types=ContentType.ANY)
async def _confirm_make_mail(message: Message, state: FSMContext):
    await StartMailingStates.idle.set()
    await state.update_data(msg_id=message.message_id)
    kb = get_start_mail_kb()
    await message.answer("Меню действий", reply_markup=kb)

@dp.callback_query_handler(text="change_start_kb", state=StartMailingStates.idle)
async def _process_change_kb(call: CallbackQuery, state: FSMContext):
    await StartMailingStates.change_kb.set()
    await call.answer()
    await call.message.answer("""
Отправьте клавиатуру в формате
текст;ссылка
где каждая строчка это отдельная кнопка
Пример:
Google;google.com
Facebook;facebook.com
    """)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def send_msg(user_id, from_user, msg_id, kb, time):
    while True:
        try:
            sent_msg = await bot.copy_message(user_id, from_user, msg_id, reply_markup=kb)
            try:
                if time is not None:
                    date = datetime.now() + timedelta(seconds=time.second, minutes=time.minute, hours=time.hour)
                    scheduler.add_job(delete_msg, trigger=DateTrigger(date), args=(user_id, sent_msg.message_id), id=f"delete_msg_{user_id}_{msg_id}")
            except:
                pass
            return
        except aiogram.utils.exceptions.RetryAfter as e:
            await asyncio.sleep(e.timeout)
        except Exception:
            pass


@dp.message_handler(state=StartMailingStates.change_kb)
async def _process_change_kb_end(message: Message, state: FSMContext):
    text = message.text
    kb = InlineKeyboardMarkup(row_width=1)
    try:
        btns = text.split("\n")
        for btn in btns:
            name, link = btn.split(";")
            kb.add(InlineKeyboardButton(name, url=link))
        async with state.proxy() as data:
            new_msg_id = await bot.copy_message(message.from_user.id, message.from_user.id, data["msg_id"], reply_markup=kb)
            await StartMailingStates.idle.set()
            data["msg_id"] = new_msg_id.message_id
            data["kb"] = kb.as_json()
    except:
        await message.answer("Неправильный формат")
        return
    kb = get_start_mail_kb()
    await message.answer("Меню действий", reply_markup=kb)

async def send_start_msg(user_id):
    start_msg_id, start_from_user_id, start_kb, start_delete = Setting.get_many(["start_msg_id", "start_from_user_id", "start_kb", "start_delete"])
    delete_time = ""
    if start_delete != "0":
        try:
            delete_time = datetime.strptime(start_delete, "%H:%M:%S")
        except ValueError:
            for user_id in ADMINS:
                await bot.send_message(user_id, f"Неправильный формат удаления сообщения: {start_delete}")
    if not start_msg_id:
        for user_id in ADMINS:
            await bot.send_message(user_id, "Стартовое сообщение не настроено")
        return
    if start_kb:
        start_kb = json.loads(start_kb)["inline_keyboard"]
        start_kb = InlineKeyboardMarkup(inline_keyboard=start_kb) if start_kb else None
    try:
        sent_msg = await bot.copy_message(
            user_id,
            start_from_user_id,
            start_msg_id,
            reply_markup=start_kb
        )
        if delete_time:
            date = datetime.now() + timedelta(seconds=delete_time.second, minutes=delete_time.minute, hours=delete_time.hour)
            scheduler.add_job(delete_msg, trigger=DateTrigger(date), args=(user_id, sent_msg.message_id), id=f"delete_msg_{user_id}_{sent_msg.message_id}")

    except Exception as e:
        logging.error(e)

@dp.callback_query_handler(text="end_start_mail", state=StartMailingStates.idle, is_admin=True)
async def _make_mail(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    kb = data.get("kb")
    Setting.set_many({
        "start_msg_id": data["msg_id"],
        "start_from_user_id": call.from_user.id,
        "start_kb": kb
    })
    await state.finish()
    await call.answer()
    await call.message.answer("Стартовое сообщение успешно изменено. Оно выглядит так:")
    await send_start_msg(call.from_user.id)
# =======

@dp.message_handler(commands=["adm", "admin"], is_admin=True)
async def _start(message: Message):
    await message.answer("Админка открыта", reply_markup=get_admin_markup())


@dp.callback_query_handler(text="get_db", is_admin=True)
async def _export_users(call: CallbackQuery):
    count = count_users()

    with open("users.csv", "w", encoding="UTF8", newline="") as f:
        writer = csv.writer(f)

        writer.writerow(["id", "username", "created_at"])

        for user in get_users():
            writer.writerow(
                [user.id, user.username, user.created_at]
            )

    text_file = InputFile("users.csv", filename="users.csv")
    await call.answer()
    await call.message.answer_document(text_file)
    with open("database.sqlite3", "rb") as f:
        await call.message.answer_document(f)


@dp.callback_query_handler(text="clear_users", is_admin=True)
async def _users_count(call: CallbackQuery):
    msg = await call.message.answer(f"Удаляем неактивных..")
    users = get_users()
    active = 0
    non_active = 0
    count = 0
    for user in users:
        if count % 50 == 0:
            await msg.edit_text(f"Считаем.. {count} всего, {active} активных, {non_active} неактивных удалено")
        count += 1
        try:
            if await bot.send_chat_action(user.id, "typing"):
                active += 1
        except Exception as e:
            delete_user(user.id)
            non_active += 1

    await call.message.answer(f"Общее количество: {count}\nАктивных пользователей: {active}, удалено неактивных: {non_active}")

@dp.callback_query_handler(text="get_users", is_admin=True)
async def _users_count(call: CallbackQuery):
    msg = await call.message.answer(f"Считаем..")
    users = get_users()
    active = 0
    count = 0
    for user in users:
        if count % 10 == 0:
            await msg.edit_text(f"Считаем.. {count} всего, {active} активных")
        count += 1
        try:
            if await bot.send_chat_action(user.id, "typing"):
                active += 1
        except Exception as e:
            if "Retry" in e.__class__.__name__:
                print(e.__class__.__name__)

    await call.message.answer(f"Общее количество: {count}\nАктивных пользователей: {active}")

fast_user_count = {"count": 0, "active": 0}

async def check_is_active(user_id):
    try:
        if await bot.send_chat_action(user_id, "typing"):
            fast_user_count["active"] += 1
    except Exception:
        pass
    finally:
        fast_user_count["count"] += 1

@dp.callback_query_handler(text="get_users_fast", is_admin=True)
async def _users_count(call: CallbackQuery):
    await call.answer()
    msg = await call.message.answer(f"Считаем..")
    all_users = get_users()

    for users in chunks(all_users, 25):
        for user in users:
            asyncio.create_task(check_is_active(user.id))
        await asyncio.sleep(1)
        await msg.edit_text(f"Считаем... Всего {fast_user_count['count']}, активных: {fast_user_count['active']}")
    await call.message.answer(f"Общее количество: {fast_user_count['count']}\nАктивных пользователей: {fast_user_count['active']}")

@dp.callback_query_handler(text="make_mail", is_admin=True)
async def _make_mail(call: CallbackQuery, state: FSMContext):
    await MailingStates.msg.set()
    await call.answer()
    await call.message.answer("Отправьте сообщение для рассылки")

def get_mail_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Изменить клавиатуру ⌨️", callback_data="change_kb"))
    kb.add(InlineKeyboardButton("Добавить время удаления 📅", callback_data="add_delete_time"))
    kb.add(InlineKeyboardButton("Отменить рассылку ❌", callback_data="cancel_mail"))
    kb.add(InlineKeyboardButton("Подтвердить рассылку ✅", callback_data="confirm_mail"))
    return kb

@dp.message_handler(is_admin=True, state=MailingStates.msg, content_types=ContentType.ANY)
async def _confirm_make_mail(message: Message, state: FSMContext):
    await MailingStates.idle.set()
    await state.update_data(msg_id=message.message_id)
    kb = get_mail_kb()
    await message.answer("Меню действий", reply_markup=kb)

@dp.callback_query_handler(text="add_delete_time", state=MailingStates.idle)
async def _process_change_kb(call: CallbackQuery, state: FSMContext):
    await MailingStates.delete_time.set()
    await call.message.answer("Введите время, через которое удалить сообщение, в формате гг:мм:сс")

@dp.message_handler(is_admin=True, state=MailingStates.delete_time)
async def _confirm_make_mail(message: Message, state: FSMContext):
    try:
        time = datetime.strptime(message.text, "%H:%M:%S")        
    except:
        await message.answer("Неправильный формат")
        return
    await state.update_data(time=time)
    await MailingStates.idle.set()
    kb = get_mail_kb()
    await message.answer("Меню действий", reply_markup=kb)

@dp.callback_query_handler(text="change_kb", state=MailingStates.idle)
async def _process_change_kb(call: CallbackQuery, state: FSMContext):
    await MailingStates.change_kb.set()
    await call.answer()
    await call.message.answer("""
Отправьте клавиатуру в формате
текст;ссылка
где каждая строчка это отдельная кнопка
Пример:
Google;google.com
Facebook;facebook.com
    """)

@dp.message_handler(state=MailingStates.change_kb)
async def _process_change_kb_end(message: Message, state: FSMContext):
    text = message.text
    kb = InlineKeyboardMarkup(row_width=1)
    try:
        btns = text.split("\n")
        for btn in btns:
            name, link = btn.split(";")
            kb.add(InlineKeyboardButton(name, url=link))
        async with state.proxy() as data:
            new_msg_id = await bot.copy_message(message.from_user.id, message.from_user.id, data["msg_id"], reply_markup=kb)
            await MailingStates.idle.set()
            data["msg_id"] = new_msg_id.message_id
            data["kb"] = kb
    except:
        await message.answer("Неправильный формат")
        return
    kb = get_mail_kb()
    await message.answer("Меню действий", reply_markup=kb)
    

@dp.callback_query_handler(text="cancel_mail", state=MailingStates.idle)
async def _process_cancel_mail(call: CallbackQuery, state: FSMContext):
    await state.finish()
    await call.message.edit_text("Отменено", reply_markup=None)

@dp.callback_query_handler(text="confirm_mail", state=MailingStates.idle, is_admin=True)
async def _make_mail(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await MailingStates.amount.set()
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("Отправить всем", callback_data="send_all")
    )
    await call.message.answer("Укажите количество пользователей или нажмите кнопку чтобы отправить всем", reply_markup=kb)

async def delete_msg(chat_id, msg_id):
    try:
        await bot.delete_message(chat_id, msg_id)
    except:
        pass

from aiogram.utils.callback_data import CallbackData
fast_cb = CallbackData("fast_mail", "is_fast")

async def choose_fast_or_not(msg: Message):
    kb = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("Обычная", callback_data=fast_cb.new("no")),
        InlineKeyboardButton("Быстрая (бета)", callback_data=fast_cb.new("yes")),
    )
    await msg.answer("Выберите режим отправки рассылки\nОбратите внимание, что быстрая может работать некорректно и её надо протестировать", reply_markup=kb)

@dp.callback_query_handler(text="send_all", state=MailingStates.amount, is_admin=True)
async def _send_all_mail(call: CallbackQuery, state: FSMContext):
    await MailingStates.fast.set()
    await call.answer()
    await choose_fast_or_not(call.message)


@dp.message_handler(state=MailingStates.amount, is_admin=True)
async def _make_mail(message: Message, state: FSMContext):
    max_amount = message.text

    if not max_amount.isdigit():
        await message.answer("Это не число!")
        return

    max_amount = int(max_amount)
    if max_amount < 0:
        await message.answer("Введите число больше 0")
        return

    await MailingStates.fast.set()
    await state.update_data(max_amount=max_amount)
    await choose_fast_or_not(message)

fast_count = {
    "count": 0,
    "good": 0,
    "bad": 0
}

async def send_message(user_id: int, from_chat: int, msg_id: int, kb, time) -> bool:
    try:        
        sent_msg = await bot.copy_message(user_id, from_chat, msg_id, reply_markup=kb)
    except aiogram.utils.exceptions.RetryAfter as e:
        await asyncio.sleep(e.timeout)
        return await send_message(user_id, from_chat, msg_id, kb, time)
    except Exception as e:
        fast_count["bad"] += 1
    else:
        if time is not None:
            date = datetime.now() + timedelta(seconds=time.second, minutes=time.minute, hours=time.hour)
            scheduler.add_job(delete_msg, trigger=DateTrigger(date), args=(user_id, sent_msg.message_id), id=f"delete_msg_{user_id}_{msg_id}")
        fast_count["good"] += 1
    fast_count["count"] += 1


@dp.callback_query_handler(fast_cb.filter(), state=MailingStates.fast)
async def _process_mail(call: CallbackQuery, state: FSMContext, callback_data: dict):
    msg = await call.message.answer(f"Делаем рассылку..")
    data = await state.get_data()
    await state.finish()
    msg_id = data["msg_id"]
    kb = data.get("kb")
    max_amount = data.get("max_amount")
    has_limit = max_amount is not None
    from_user = call.from_user.id
    time: datetime = data.get("time")
    fast = True if callback_data.get("is_fast") == "yes" else False

    user_ids = get_user_ids()
    count = 0
    good = 0
    bad = 0
    all_amount = max_amount if has_limit else len(user_ids)
    fast_count["count"] = 0
    fast_count["good"] = 0
    fast_count["bad"] = 0

    # ====
    if fast:
        for users in chunks(user_ids, 25):
            for user in users:
                asyncio.create_task(send_message(user.id, from_user, msg_id, kb, time))
            await asyncio.sleep(1)
            try:
                await msg.edit_text(f"Отправлено: {fast_count['count']}, успешно: {fast_count['good']}, неудачно: {fast_count['bad']}")
            except:
                pass
            if has_limit and fast_count["good"] >= max_amount:
                break
        await call.message.answer(f"Результаты рассылки\nОтправлено: {fast_count['count']}, успешно: {fast_count['good']}, неудачно: {fast_count['bad']}")
    # ====
    else:
        for user_id in user_ids:
            if count % 50 == 0:
                await msg.edit_text(f"Отправлено: {count}, удачно {good}, всего надо {all_amount}")
            try:
                sent_msg = await bot.copy_message(user_id, from_user, msg_id, reply_markup=kb)
                if time is not None:
                    date = datetime.now() + timedelta(seconds=time.second, minutes=time.minute, hours=time.hour)
                    scheduler.add_job(delete_msg, trigger=DateTrigger(date), args=(user_id, sent_msg.message_id), id=f"delete_msg_{user_id}_{msg_id}")
                good += 1
            except Exception:
                bad += 1
            count += 1
            if has_limit and good >= max_amount:
                break
            await asyncio.sleep(0.05)

        await msg.edit_text(f"Всего: {count}\nУдачно: {good}\nНе пришло: {bad}")


@dp.chat_join_request_handler()
async def process_update(chat_member: types.ChatJoinRequest):
    if Setting.get_many(["send_start"])[0] == "1":
        await send_start_msg(chat_member.from_user.id)

if __name__ == '__main__':
    scheduler.start()
    executor.start_polling(dp, skip_updates=True)