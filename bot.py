import logging
import duckdb
import os
import asyncio
import random
import string
import psycopg2
from psycopg2.errors import DuplicateColumn
from datetime import date, datetime
from keep_alive import keep_alive
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ConversationHandler

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Get the tokens from environment variables
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("No BOT_TOKEN provided in environment variables")

DATABASE_URL = os.environ.get("DATABASE_URL")

# Admin ID for generating vouchers (Replace 0 with your actual Telegram ID if you want to hardcode it, or use Render env var)
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))

# Path to your remote Parquet dataset
DATASET_URL_PHONE = "hf://datasets/WipeX00/scrappeddata/idx_phone.*.parquet"
DATASET_URL_AADHAR = "hf://datasets/WipeX00/scrappeddata/idx_aadhar.*.parquet"
DATASET_URL_INDDATA = "hf://datasets/WipeX00/Inddatainonefile/users_data.parquet"
DATASET_URL_TIRUCALLER = "hf://datasets/WipeX00/tirucaller/idx_email.parquet"

# Initialize DuckDB and install httpfs extension for Hugging Face support
con = duckdb.connect('bot.duckdb')
con.execute("PRAGMA memory_limit='256MB';")
con.execute("PRAGMA threads=2;")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Configure HTTP retries to gracefully handle Hugging Face 429 rate limits
con.execute("SET http_retries=10;")
con.execute("SET http_retry_wait_ms=1000;")
con.execute("SET http_retry_backoff=2;")

# Setup Hugging Face Authentication if token is provided
hf_token = os.environ.get("HF_TOKEN")
if hf_token:
    con.execute(f"CREATE SECRET (TYPE HUGGINGFACE, TOKEN '{hf_token}');")

# States for the conversation
CHOOSING, WAITING_FOR_PHONE, WAITING_FOR_DOC, WAITING_FOR_VOUCHER, WAITING_FOR_MANAGE_USER_ID, WAITING_FOR_MANAGE_CREDITS, WAITING_FOR_EMAIL = range(7)

SEARCH_MESSAGES = [
    "🔍 Initializing neural network scan...",
    "💻 Bypassing mainframe firewalls...",
    "📡 Intercepting encrypted packets...",
    "🛡️ Accessing restricted databases...",
    "⚙️ Decrypting remote payloads...",
    "🛰️ Triangulating signal origin...",
    "🔐 Extracting secure hashes...",
    "🕵️ Scanning dark web repositories..."
]

# ================= Database & Credit System =================

def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set. Please add your PostgreSQL URL to your environment variables.")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    if not DATABASE_URL:
        logging.warning("DATABASE_URL is missing. Database will not be initialized.")
        return
        
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            credits INTEGER DEFAULT 0,
            last_bonus_date TEXT,
            is_blocked BOOLEAN DEFAULT FALSE
        )
    ''')
    
    # Safe schema migration for is_blocked (just in case it's missing)
    try:
        c.execute('ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT FALSE')
        conn.commit()
    except DuplicateColumn:
        conn.rollback()
        
    # Safe schema migration for bonus buckets
    try:
        c.execute('ALTER TABLE users ADD COLUMN bonus_credits INTEGER DEFAULT 0')
        conn.commit()
    except DuplicateColumn:
        conn.rollback()
        
    try:
        c.execute('ALTER TABLE users ADD COLUMN bonus_expiry TIMESTAMP')
        conn.commit()
    except DuplicateColumn:
        conn.rollback()

    c.execute('''
        CREATE TABLE IF NOT EXISTS vouchers (
            code TEXT PRIMARY KEY,
            value INTEGER,
            is_used BOOLEAN DEFAULT FALSE
        )
    ''')
    conn.commit()
    conn.close()

def _check_and_expire_bonus(user_id):
    """Internal helper to wipe expired bonus credits before checking balances."""
    conn = get_db_connection()
    c = conn.cursor()
    # Reset bonus_credits to 0 if the current time is past the expiry time
    c.execute('''
        UPDATE users 
        SET bonus_credits = 0 
        WHERE user_id = %s 
        AND bonus_expiry IS NOT NULL 
        AND NOW() > bonus_expiry
    ''', (user_id,))
    conn.commit()
    conn.close()

def get_user_data(user_id):
    # First, expire any old bonuses
    _check_and_expire_bonus(user_id)
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT credits, bonus_credits, is_blocked FROM users WHERE user_id = %s', (user_id,))
    row = c.fetchone()
    
    if row:
        conn.close()
        return {"credits": row[0], "bonus_credits": row[1], "is_blocked": bool(row[2])}
    else:
        # Create user with 0 credits if they don't exist
        c.execute('INSERT INTO users (user_id, credits, bonus_credits, is_blocked) VALUES (%s, 0, 0, FALSE)', (user_id,))
        conn.commit()
        conn.close()
        return {"credits": 0, "bonus_credits": 0, "is_blocked": False}

def get_user_credits(user_id):
    data = get_user_data(user_id)
    return data["credits"] + data["bonus_credits"]

def is_user_blocked(user_id):
    return get_user_data(user_id)["is_blocked"]

def set_user_block_status(user_id, status: bool):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE users SET is_blocked = %s WHERE user_id = %s', (status, user_id))
    conn.commit()
    conn.close()

def deduct_credit(user_id):
    # Expire old bonuses first to ensure we don't spend an expired credit
    _check_and_expire_bonus(user_id)
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT bonus_credits FROM users WHERE user_id = %s', (user_id,))
    row = c.fetchone()
    
    if row and row[0] > 0:
        # Deduct from bonus credits first
        c.execute('UPDATE users SET bonus_credits = bonus_credits - 1 WHERE user_id = %s', (user_id,))
    else:
        # Otherwise deduct from permanent credits
        c.execute('UPDATE users SET credits = credits - 1 WHERE user_id = %s', (user_id,))
        
    conn.commit()
    conn.close()

def add_credits(user_id, amount):
    # This is for admin adding/deducting PERMANENT credits, or voucher redemptions
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE users SET credits = credits + %s WHERE user_id = %s', (amount, user_id))
    conn.commit()
    conn.close()

def claim_daily_bonus(user_id):
    _check_and_expire_bonus(user_id)
    # Ensure user exists
    get_user_data(user_id)
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Check if they currently have a valid, unexpired bonus period active
    # (If NOW() > bonus_expiry, it would have been wiped by _check_and_expire_bonus)
    c.execute('SELECT bonus_expiry FROM users WHERE user_id = %s', (user_id,))
    row = c.fetchone()
    
    if row and row[0] is not None:
        # If it's not None, it means the expiry is in the future. They can't claim yet.
        conn.close()
        return False
        
    # Claim the bonus: Give 2 bonus credits and set expiry to exactly 24 hours from now
    c.execute('''
        UPDATE users 
        SET bonus_credits = 2, bonus_expiry = NOW() + INTERVAL '24 hours' 
        WHERE user_id = %s
    ''', (user_id,))
    conn.commit()
    conn.close()
    return True

def generate_voucher_code(value):
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('INSERT INTO vouchers (code, value, is_used) VALUES (%s, %s, FALSE)', (code, value))
    conn.commit()
    conn.close()
    return code

def redeem_voucher_code(user_id, code):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT value, is_used FROM vouchers WHERE code = %s', (code,))
    row = c.fetchone()
    
    if not row:
        conn.close()
        return "❌ Invalid voucher code."
        
    value, is_used = row
    if is_used:
        conn.close()
        return "❌ This voucher has already been redeemed."
        
    # Mark as used
    c.execute('UPDATE vouchers SET is_used = TRUE WHERE code = %s', (code,))
    conn.commit()
    conn.close()
    
    # Add permanent credits
    get_user_data(user_id) # ensure user exists
    add_credits(user_id, value)
    
    return f"✅ Success! You have redeemed {value} permanent credits."

# ================= Search Logic =================

def search_by_phone(phone_number: str):
    try:
        query = f"""
            SELECT name, fathersName, aadharNumber, phoneNumber, otherNumber, address, town, district, state, pincode
            FROM read_parquet('{DATASET_URL_PHONE}') 
            WHERE phoneNumber = ? 
            LIMIT 10
        """
        results = con.execute(query, [phone_number]).fetchall()
        
        if not results:
            fallback_query_1 = f"""
                SELECT mobile, name, fname, address, alt, circle, id, email
                FROM read_parquet('{DATASET_URL_INDDATA}')
                WHERE mobile = ?
                LIMIT 10
            """
            fallback_1 = con.execute(fallback_query_1, [phone_number]).fetchall()
            return {"primary": [], "fallback_1": fallback_1, "fallback_2": []}
            
        return {"primary": results, "fallback_1": [], "fallback_2": []}
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

def search_by_email(email_str: str):
    try:
        query_2 = f"""
            SELECT Number, Name, Address, Email, Gender, Carrier
            FROM read_parquet('{DATASET_URL_TIRUCALLER}')
            WHERE Email = ?
            LIMIT 10
        """
        res_2 = con.execute(query_2, [email_str]).fetchall()
        
        if not res_2:
            return {"fallback_1": [], "fallback_2": []}
            
        merged_results = []
        for row2 in res_2:
            number, name2, address2, email2, gender, carrier = row2
            if number:
                query_1 = f"""
                    SELECT mobile, name, fname, address, alt, circle, id, email
                    FROM read_parquet('{DATASET_URL_INDDATA}')
                    WHERE mobile = ?
                    LIMIT 1
                """
                res_1 = con.execute(query_1, [number]).fetchall()
                if res_1:
                    mobile, name1, fname, address1, alt, circle, doc_id, email1 = res_1[0]
                    merged_dict = {
                        'name': name1 or name2,
                        'fname': fname,
                        'phone': number,
                        'alt_phone': alt,
                        'email': email2 or email1,
                        'doc_id': doc_id,
                        'gender': gender,
                        'carrier': carrier,
                        'address': address1 if address1 and str(address1).lower() != 'none' else address2,
                        'circle': circle
                    }
                    merged_results.append(merged_dict)
                else:
                    merged_dict = {
                        'name': name2,
                        'fname': None,
                        'phone': number,
                        'alt_phone': None,
                        'email': email2,
                        'doc_id': None,
                        'gender': gender,
                        'carrier': carrier,
                        'address': address2,
                        'circle': None
                    }
                    merged_results.append(merged_dict)
            else:
                merged_dict = {
                    'name': name2,
                    'fname': None,
                    'phone': None,
                    'alt_phone': None,
                    'email': email2,
                    'doc_id': None,
                    'gender': gender,
                    'carrier': carrier,
                    'address': address2,
                    'circle': None
                }
                merged_results.append(merged_dict)
                
        return {"merged": merged_results}
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

def format_combined_results(data_dict):
    primary = data_dict.get("primary", [])
    fallback_1 = data_dict.get("fallback_1", [])
    fallback_2 = data_dict.get("fallback_2", [])
    merged = data_dict.get("merged", [])
    
    total = len(primary) + len(fallback_1) + len(fallback_2) + len(merged)
    if total == 0:
        return None
        
    response_lines = [f"Found {total} record(s):"]
    i = 1
    
    for row in primary:
        name, fathers_name, doc_id, phone, other_phone, address, town, district, state, pincode = row
        addr_parts = [p for p in [address, town, district, state, pincode] if p and str(p).lower() != 'none']
        full_address = ", ".join(str(p) for p in addr_parts) if addr_parts else "N/A"
        
        record = (
            f"\n--- Record {i} (Source: Primary) ---\n"
            f"👤 Name: {name or 'N/A'}\n"
            f"👨 Father's Name: {fathers_name or 'N/A'}\n"
            f"📱 Phone: {phone or 'N/A'}\n"
            f"📞 Alt Phone: {other_phone or 'N/A'}\n"
            f"🪪 Aadhar: {doc_id or 'N/A'}\n"
            f"📍 Address: {full_address}\n"
            f"🌍 Circle: {state or 'N/A'}"
        )
        response_lines.append(record)
        i += 1
        
    for row in fallback_1:
        mobile, name, fname, address, alt, circle, doc_id, email = row
        record = (
            f"\n--- Record {i} (Source: Alt 1) ---\n"
            f"👤 Name: {name or 'N/A'}\n"
            f"👨 Father's Name: {fname or 'N/A'}\n"
            f"📱 Phone: {mobile or 'N/A'}\n"
            f"📞 Alt Phone: {alt or 'N/A'}\n"
            f"📧 Email: {email or 'N/A'}\n"
            f"🪪 ID: {doc_id or 'N/A'}\n"
            f"📍 Address: {address or 'N/A'}\n"
            f"🌍 Circle: {circle or 'N/A'}"
        )
        response_lines.append(record)
        i += 1
        
    for row in fallback_2:
        number, name, address, email, gender, carrier = row
        record = (
            f"\n--- Record {i} (Source: Alt 2) ---\n"
            f"👤 Name: {name or 'N/A'}\n"
            f"📱 Phone: {number or 'N/A'}\n"
            f"📧 Email: {email or 'N/A'}\n"
            f"⚧ Gender: {gender or 'N/A'}\n"
            f"📡 Carrier: {carrier or 'N/A'}\n"
            f"📍 Address: {address or 'N/A'}"
        )
        response_lines.append(record)
        i += 1
        
    for row in merged:
        record = (
            f"\n--- Record {i} (Source: Merged Profile) ---\n"
            f"👤 Name: {row.get('name') or 'N/A'}\n"
            f"👨 Father's Name: {row.get('fname') or 'N/A'}\n"
            f"📱 Phone: {row.get('phone') or 'N/A'}\n"
            f"📞 Alt Phone: {row.get('alt_phone') or 'N/A'}\n"
            f"📧 Email: {row.get('email') or 'N/A'}\n"
            f"🪪 ID: {row.get('doc_id') or 'N/A'}\n"
            f"⚧ Gender: {row.get('gender') or 'N/A'}\n"
            f"📡 Carrier: {row.get('carrier') or 'N/A'}\n"
            f"📍 Address: {row.get('address') or 'N/A'}\n"
            f"🌍 Circle: {row.get('circle') or 'N/A'}"
        )
        response_lines.append(record)
        i += 1

    response = "\n".join(response_lines)
    if len(response) > 4000:
        response = response[:4000] + "\n... (results truncated)"
    return response

def search_by_doc_id(doc_id: str):
    try:
        query = f"""
            SELECT name, fathersName, aadharNumber, phoneNumber, otherNumber, address, town, district, state, pincode 
            FROM read_parquet('{DATASET_URL_AADHAR}') 
            WHERE aadharNumber = ? 
            LIMIT 10
        """
        return con.execute(query, [doc_id]).fetchall()
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

def format_results(results):
    response_lines = [f"Found {len(results)} record(s):"]
    for i, row in enumerate(results, 1):
        name, fathers_name, doc_id, phone, other_phone, address, town, district, state, pincode = row
        
        addr_parts = [p for p in [address, town, district, state, pincode] if p and str(p).lower() != 'none']
        full_address = ", ".join(str(p) for p in addr_parts) if addr_parts else "N/A"
        
        record = (
            f"\n--- Record {i} ---\n"
            f"👤 Name: {name or 'N/A'}\n"
            f"👨 Father's Name: {fathers_name or 'N/A'}\n"
            f"📱 Phone: {phone or 'N/A'}\n"
            f"📞 Alt Phone: {other_phone or 'N/A'}\n"
            f"🪪 Aadhar: {doc_id or 'N/A'}\n"
            f"📍 Address: {full_address}\n"
            f"🌍 Circle: {state or 'N/A'}"
        )
        response_lines.append(record)
        
    response = "\n".join(response_lines)
    if len(response) > 4000:
        response = response[:4000] + "\n... (results truncated)"
    return response

# ================= Handlers =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send message on `/start` with reply keyboard buttons."""
    user_id = update.effective_user.id
    
    keyboard = [
        [KeyboardButton("📱 Number Info"), KeyboardButton("🪪 Aadhar Info")],
        [KeyboardButton("📧 Email Search"), KeyboardButton("💰 Check Balance")],
        [KeyboardButton("🎁 Daily Bonus"), KeyboardButton("🎟️ Redeem Voucher")],
        [KeyboardButton("💳 Buy Credits")]
    ]
    
    if user_id == ADMIN_ID:
        keyboard.append([KeyboardButton("🛠️ Manage Users")])
        
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    text = "Welcome! I am the high-performance search bot.\nPlease choose what you want to do:"
    
    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup)
    
    return CHOOSING

async def choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Parses the user's choice from the reply keyboard."""
    choice = update.message.text
    user_id = update.message.from_user.id
    
    if choice == "📱 Number Info":
        if is_user_blocked(user_id):
            await update.message.reply_text("🚫 You have been blocked from using this bot.")
            return CHOOSING
        if get_user_credits(user_id) <= 0:
            await update.message.reply_text("❌ You have 0 credits. Please claim your Daily Bonus or Buy Credits to search.")
            return CHOOSING
        await update.message.reply_text("Please enter the phone number you want to search (Costs 1 credit):", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_PHONE
        
    elif choice == "🪪 Aadhar Info":
        if is_user_blocked(user_id):
            await update.message.reply_text("🚫 You have been blocked from using this bot.")
            return CHOOSING
        if get_user_credits(user_id) <= 0:
            await update.message.reply_text("❌ You have 0 credits. Please claim your Daily Bonus or Buy Credits to search.")
            return CHOOSING
        await update.message.reply_text("Please enter the Aadhar number you want to search (Costs 1 credit):", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_DOC
        
    elif choice == "📧 Email Search":
        if is_user_blocked(user_id):
            await update.message.reply_text("🚫 You have been blocked from using this bot.")
            return CHOOSING
        if get_user_credits(user_id) <= 0:
            await update.message.reply_text("❌ You have 0 credits. Please claim your Daily Bonus or Buy Credits to search.")
            return CHOOSING
        await update.message.reply_text("Please enter the Email address you want to search (Costs 1 credit):", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_EMAIL
        
    elif choice == "💰 Check Balance":
        credits = get_user_credits(user_id)
        await update.message.reply_text(f"💳 Your current balance is: {credits} credits.")
        return CHOOSING
        
    elif choice == "🎁 Daily Bonus":
        success = claim_daily_bonus(user_id)
        if success:
            await update.message.reply_text("🎉 You have successfully claimed your daily 2 free credits! (Valid for 24 hours)")
        else:
            await update.message.reply_text("❌ You have already claimed your daily bonus recently. Please wait exactly 24 hours from your last claim!")
        return CHOOSING
        
    elif choice == "💳 Buy Credits":
        await update.message.reply_text("🛒 To buy permanent credits, please contact the admin on Telegram: @WIPE_X")
        return CHOOSING
        
    elif choice == "🎟️ Redeem Voucher":
        await update.message.reply_text("Please enter your voucher code:", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_VOUCHER
        
    elif choice == "🛠️ Manage Users":
        if user_id != ADMIN_ID:
            return CHOOSING
        await update.message.reply_text("Please enter the Telegram User ID you want to manage:", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_MANAGE_USER_ID
        
    else:
        await update.message.reply_text("Please use the buttons provided at the bottom of your screen.")
        return CHOOSING

async def handle_phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if is_user_blocked(user_id):
        await update.message.reply_text("🚫 You have been blocked from using this bot. Search cancelled.")
        await start(update, context)
        return CHOOSING
        
    if get_user_credits(user_id) <= 0:
        await update.message.reply_text("❌ You have 0 credits. Search cancelled.")
        await start(update, context)
        return CHOOSING
        
    phone_number = update.message.text.strip()
    await update.message.reply_text(random.choice(SEARCH_MESSAGES))
    
    results = await asyncio.to_thread(search_by_phone, phone_number)
    
    if results and (results.get('primary') or results.get('fallback_1') or results.get('fallback_2')):
        deduct_credit(user_id)
        await update.message.reply_text(format_combined_results(results))
    else:
        await update.message.reply_text("No user found with that phone number. (No credits deducted)")
        
    await start(update, context)
    return CHOOSING

async def handle_doc_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if is_user_blocked(user_id):
        await update.message.reply_text("🚫 You have been blocked from using this bot. Search cancelled.")
        await start(update, context)
        return CHOOSING
        
    if get_user_credits(user_id) <= 0:
        await update.message.reply_text("❌ You have 0 credits. Search cancelled.")
        await start(update, context)
        return CHOOSING
        
    doc_id = update.message.text.strip()
    await update.message.reply_text(random.choice(SEARCH_MESSAGES))
    
    results = await asyncio.to_thread(search_by_doc_id, doc_id)
    
    if results:
        deduct_credit(user_id)
        await update.message.reply_text(format_results(results))
    else:
        await update.message.reply_text("No user found with that Aadhar number. (No credits deducted)")
        
    await start(update, context)
    return CHOOSING

async def handle_email_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if is_user_blocked(user_id):
        await update.message.reply_text("🚫 You have been blocked from using this bot. Search cancelled.")
        await start(update, context)
        return CHOOSING
        
    if get_user_credits(user_id) <= 0:
        await update.message.reply_text("❌ You have 0 credits. Search cancelled.")
        await start(update, context)
        return CHOOSING
        
    email_str = update.message.text.strip()
    await update.message.reply_text(random.choice(SEARCH_MESSAGES))
    
    results = await asyncio.to_thread(search_by_email, email_str)
    
    if results and results.get('merged'):
        deduct_credit(user_id)
        await update.message.reply_text(format_combined_results(results))
    else:
        await update.message.reply_text("No user found with that email address. (No credits deducted)")
        
    await start(update, context)
    return CHOOSING

async def handle_voucher_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    user = update.message.from_user
    
    result_msg = redeem_voucher_code(user.id, code)
    await update.message.reply_text(result_msg)
    
    # If redemption was successful, notify the admin
    if result_msg.startswith("✅"):
        username = f"@{user.username}" if user.username else "No Username"
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🔔 *Voucher Redeemed!*\n\n"
                    f"👤 *User:* {username}\n"
                    f"🆔 *User ID:* `{user.id}`\n"
                    f"🎟️ *Code:* `{code}`\n"
                    f"⏰ *Time:* {current_time}"
                ),
                parse_mode="Markdown"
            )
        except Exception as e:
            logging.error(f"Failed to send admin notification: {e}")
            
    await start(update, context)
    return CHOOSING

# ================= Admin User Management Logic =================

async def handle_manage_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
        context.user_data['manage_target_id'] = target_id
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID. Must be a number.")
        await start(update, context)
        return CHOOSING
        
    user_data = get_user_data(target_id)
    perm_credits = user_data['credits']
    bonus_credits = user_data['bonus_credits']
    total_credits = perm_credits + bonus_credits
    is_blocked = user_data['is_blocked']
    status_text = "🚫 Blocked" if is_blocked else "✅ Active"
    
    text = f"👤 **User ID:** `{target_id}`\n💰 **Total Credits:** {total_credits} _(Perm: {perm_credits}, Bonus: {bonus_credits})_\n🛡️ **Status:** {status_text}"
    
    block_button = InlineKeyboardButton("✅ Unblock User", callback_data=f"manage_unblock_{target_id}") if is_blocked else InlineKeyboardButton("🚫 Block User", callback_data=f"manage_block_{target_id}")
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Permanent Credits", callback_data=f"manage_add_{target_id}")],
        [InlineKeyboardButton("➖ Deduct Permanent Credits", callback_data=f"manage_deduct_{target_id}")],
        [block_button],
        [InlineKeyboardButton("❌ Cancel", callback_data="manage_cancel")]
    ]
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSING # Keep them in CHOOSING because the inline buttons will trigger a CallbackQueryHandler

async def manage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "manage_cancel":
        await query.edit_message_text("Management cancelled.")
        await start(update, context)
        return CHOOSING
        
    parts = data.split("_")
    action = parts[1]
    target_id = int(parts[2])
    
    if action == "block":
        set_user_block_status(target_id, True)
        await query.edit_message_text(f"✅ User `{target_id}` has been blocked.", parse_mode="Markdown")
        await start(update, context)
        return CHOOSING
    elif action == "unblock":
        set_user_block_status(target_id, False)
        await query.edit_message_text(f"✅ User `{target_id}` has been unblocked.", parse_mode="Markdown")
        await start(update, context)
        return CHOOSING
    elif action == "add":
        context.user_data['manage_target_id'] = target_id
        context.user_data['manage_action'] = "add"
        await query.edit_message_text(f"Enter the amount of PERMANENT credits to ADD to user `{target_id}`:", parse_mode="Markdown")
        return WAITING_FOR_MANAGE_CREDITS
    elif action == "deduct":
        context.user_data['manage_target_id'] = target_id
        context.user_data['manage_action'] = "deduct"
        await query.edit_message_text(f"Enter the amount of PERMANENT credits to DEDUCT from user `{target_id}`:", parse_mode="Markdown")
        return WAITING_FOR_MANAGE_CREDITS
        
    return CHOOSING

async def handle_manage_credits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_id = context.user_data.get('manage_target_id')
    action = context.user_data.get('manage_action')
    
    try:
        amount = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid amount. Must be a number.")
        await start(update, context)
        return CHOOSING
        
    if action == "add":
        add_credits(target_id, amount)
        await update.message.reply_text(f"✅ Added {amount} permanent credits to user `{target_id}`.", parse_mode="Markdown")
    elif action == "deduct":
        add_credits(target_id, -amount)
        await update.message.reply_text(f"✅ Deducted {amount} permanent credits from user `{target_id}`.", parse_mode="Markdown")
        
    await start(update, context)
    return CHOOSING

async def gen_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to generate voucher codes. Usage: /gen <amount>"""
    user_id = update.message.from_user.id
    
    if ADMIN_ID == 0 or user_id != ADMIN_ID:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return
        
    if not context.args:
        await update.message.reply_text("Usage: /gen <value>")
        return
        
    try:
        value = int(context.args[0])
        code = generate_voucher_code(value)
        await update.message.reply_text(f"✅ Generated Voucher!\n\nCode: `{code}`\nValue: {value} credits", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Value must be an integer.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled. Type /start to begin again.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# ================= Main =================

if __name__ == '__main__':
    # Initialize the database
    init_db()
    
    # Start the dummy web server so Render doesn't crash the web service
    keep_alive()
    
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("gen", gen_command))
    
    # Set up the conversation handler
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex('^(📱 Number Info|🪪 Aadhar Info|📧 Email Search|💰 Check Balance|🎁 Daily Bonus|🎟️ Redeem Voucher|💳 Buy Credits|🛠️ Manage Users)$'), choice_handler),
            CallbackQueryHandler(manage_callback, pattern="^manage_")
        ],
        states={
            CHOOSING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, choice_handler),
                CallbackQueryHandler(manage_callback, pattern="^manage_")
            ],
            WAITING_FOR_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_phone_input)],
            WAITING_FOR_DOC: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_doc_input)],
            WAITING_FOR_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email_input)],
            WAITING_FOR_VOUCHER: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_voucher_input)],
            WAITING_FOR_MANAGE_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manage_user_id)],
            WAITING_FOR_MANAGE_CREDITS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manage_credits)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    app.add_handler(conv_handler)

    print("Optimized interactive bot with Credit System & Postgres is starting...")
    app.run_polling()
