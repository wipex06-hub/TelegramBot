import logging
import duckdb
import os
import random
import string
import psycopg2
from psycopg2.errors import DuplicateColumn
from datetime import date
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

# Initialize DuckDB and install httpfs extension for Hugging Face support
con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# States for the conversation
CHOOSING, WAITING_FOR_PHONE, WAITING_FOR_DOC, WAITING_FOR_VOUCHER, WAITING_FOR_MANAGE_USER_ID, WAITING_FOR_MANAGE_CREDITS = range(6)

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
            last_bonus_date TEXT
        )
    ''')
    
    # Safe schema migration for is_blocked
    try:
        c.execute('ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT FALSE')
        conn.commit()
    except DuplicateColumn:
        conn.rollback() # Column already exists
        
    c.execute('''
        CREATE TABLE IF NOT EXISTS vouchers (
            code TEXT PRIMARY KEY,
            value INTEGER,
            is_used BOOLEAN DEFAULT FALSE
        )
    ''')
    conn.commit()
    conn.close()

def get_user_data(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT credits, is_blocked FROM users WHERE user_id = %s', (user_id,))
    row = c.fetchone()
    
    if row:
        conn.close()
        return {"credits": row[0], "is_blocked": bool(row[1])}
    else:
        # Create user with 0 credits if they don't exist
        c.execute('INSERT INTO users (user_id, credits, last_bonus_date, is_blocked) VALUES (%s, 0, NULL, FALSE)', (user_id,))
        conn.commit()
        conn.close()
        return {"credits": 0, "is_blocked": False}

def get_user_credits(user_id):
    return get_user_data(user_id)["credits"]

def is_user_blocked(user_id):
    return get_user_data(user_id)["is_blocked"]

def set_user_block_status(user_id, status: bool):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE users SET is_blocked = %s WHERE user_id = %s', (status, user_id))
    conn.commit()
    conn.close()

def deduct_credit(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE users SET credits = credits - 1 WHERE user_id = %s', (user_id,))
    conn.commit()
    conn.close()

def add_credits(user_id, amount):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE users SET credits = credits + %s WHERE user_id = %s', (amount, user_id))
    conn.commit()
    conn.close()

def claim_daily_bonus(user_id):
    today = str(date.today())
    # Ensure user exists
    get_user_credits(user_id) 
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT last_bonus_date FROM users WHERE user_id = %s', (user_id,))
    row = c.fetchone()
    
    if row and row[0] == today:
        conn.close()
        return False # Already claimed today
        
    c.execute('UPDATE users SET credits = credits + 2, last_bonus_date = %s WHERE user_id = %s', (today, user_id))
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
    
    # Ensure user exists and add credits
    get_user_credits(user_id)
    add_credits(user_id, value)
    
    return f"✅ Success! You have redeemed {value} credits."

# ================= Search Logic =================

def search_by_phone(phone_number: str):
    try:
        query = f"""
            SELECT name, fathersName, aadharNumber, phoneNumber, otherNumber, address, town, district, state, pincode
            FROM read_parquet('{DATASET_URL_PHONE}') 
            WHERE phoneNumber = ? 
            LIMIT 10
        """
        return con.execute(query, [phone_number]).fetchall()
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

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
        [KeyboardButton("💰 Check Balance"), KeyboardButton("🎁 Daily Bonus")],
        [KeyboardButton("🎟️ Redeem Voucher"), KeyboardButton("💳 Buy Credits")]
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
        
    elif choice == "💰 Check Balance":
        credits = get_user_credits(user_id)
        await update.message.reply_text(f"💳 Your current balance is: {credits} credits.")
        return CHOOSING
        
    elif choice == "🎁 Daily Bonus":
        success = claim_daily_bonus(user_id)
        if success:
            await update.message.reply_text("🎉 You have successfully claimed your daily 2 free credits!")
        else:
            await update.message.reply_text("❌ You have already claimed your daily bonus today. Come back tomorrow!")
        return CHOOSING
        
    elif choice == "💳 Buy Credits":
        await update.message.reply_text("🛒 To buy credits, please contact the admin on Telegram: @WIPE_X")
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
    await update.message.reply_text("Searching user data, please wait...")
    
    results = search_by_phone(phone_number)
    
    if results:
        deduct_credit(user_id)
        await update.message.reply_text(format_results(results))
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
    await update.message.reply_text("Searching user data, please wait...")
    
    results = search_by_doc_id(doc_id)
    
    if results:
        deduct_credit(user_id)
        await update.message.reply_text(format_results(results))
    else:
        await update.message.reply_text("No user found with that Aadhar number. (No credits deducted)")
        
    await start(update, context)
    return CHOOSING

async def handle_voucher_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    user_id = update.message.from_user.id
    
    result_msg = redeem_voucher_code(user_id, code)
    await update.message.reply_text(result_msg)
    
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
    credits = user_data['credits']
    is_blocked = user_data['is_blocked']
    status_text = "🚫 Blocked" if is_blocked else "✅ Active"
    
    text = f"👤 **User ID:** `{target_id}`\n💰 **Credits:** {credits}\n🛡️ **Status:** {status_text}"
    
    block_button = InlineKeyboardButton("✅ Unblock User", callback_data=f"manage_unblock_{target_id}") if is_blocked else InlineKeyboardButton("🚫 Block User", callback_data=f"manage_block_{target_id}")
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Credits", callback_data=f"manage_add_{target_id}"), InlineKeyboardButton("➖ Deduct Credits", callback_data=f"manage_deduct_{target_id}")],
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
        await query.edit_message_text(f"Enter the amount of credits to ADD to user `{target_id}`:", parse_mode="Markdown")
        return WAITING_FOR_MANAGE_CREDITS
    elif action == "deduct":
        context.user_data['manage_target_id'] = target_id
        context.user_data['manage_action'] = "deduct"
        await query.edit_message_text(f"Enter the amount of credits to DEDUCT from user `{target_id}`:", parse_mode="Markdown")
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
        await update.message.reply_text(f"✅ Added {amount} credits to user `{target_id}`.", parse_mode="Markdown")
    elif action == "deduct":
        add_credits(target_id, -amount)
        await update.message.reply_text(f"✅ Deducted {amount} credits from user `{target_id}`.", parse_mode="Markdown")
        
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
            MessageHandler(filters.Regex('^(📱 Number Info|🪪 Aadhar Info|💰 Check Balance|🎁 Daily Bonus|🎟️ Redeem Voucher|💳 Buy Credits|🛠️ Manage Users)$'), choice_handler),
            CallbackQueryHandler(manage_callback, pattern="^manage_")
        ],
        states={
            CHOOSING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, choice_handler),
                CallbackQueryHandler(manage_callback, pattern="^manage_")
            ],
            WAITING_FOR_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_phone_input)],
            WAITING_FOR_DOC: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_doc_input)],
            WAITING_FOR_VOUCHER: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_voucher_input)],
            WAITING_FOR_MANAGE_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manage_user_id)],
            WAITING_FOR_MANAGE_CREDITS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manage_credits)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    app.add_handler(conv_handler)

    print("Optimized interactive bot with Credit System & Postgres is starting...")
    app.run_polling()
