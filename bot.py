import logging
import duckdb
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, ConversationHandler

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

import os

# Get the token from environment variables (Required for Render)
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("No BOT_TOKEN provided in environment variables")

# Path to your remote Parquet dataset
DATASET_URL_PHONE = "hf://datasets/WipeX00/scrappeddata/idx_phone.*.parquet"
DATASET_URL_AADHAR = "hf://datasets/WipeX00/scrappeddata/idx_aadhar.*.parquet"

# Initialize DuckDB and install httpfs extension for Hugging Face support
con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# States for the conversation
CHOOSING, WAITING_FOR_PHONE, WAITING_FOR_DOC = range(3)

def search_by_phone(phone_number: str):
    """Optimized query using DuckDB to search a remote Parquet file."""
    try:
        query = f"""
            SELECT name, fathersName, aadharNumber, phoneNumber, address, town, district, state, pincode
            FROM read_parquet('{DATASET_URL_PHONE}') 
            WHERE phoneNumber = ? 
            LIMIT 10
        """
        return con.execute(query, [phone_number]).fetchall()
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

def search_by_doc_id(doc_id: str):
    """Optimized query using DuckDB to search by Document ID."""
    try:
        query = f"""
            SELECT name, fathersName, aadharNumber, phoneNumber, address, town, district, state, pincode 
            FROM read_parquet('{DATASET_URL_AADHAR}') 
            WHERE aadharNumber = ? 
            LIMIT 10
        """
        return con.execute(query, [doc_id]).fetchall()
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

def format_results(results):
    """Helper function to format search results nicely."""
    response_lines = [f"Found {len(results)} record(s):"]
    for i, row in enumerate(results, 1):
        name, fathers_name, doc_id, phone, address, town, district, state, pincode = row
        
        # Format full address
        addr_parts = [p for p in [address, town, district, state, pincode] if p and str(p).lower() != 'none']
        full_address = ", ".join(str(p) for p in addr_parts) if addr_parts else "N/A"
        
        record = (
            f"\n--- Record {i} ---\n"
            f"👤 Name: {name or 'N/A'}\n"
            f"👨 Father's Name: {fathers_name or 'N/A'}\n"
            f"📱 Phone: {phone or 'N/A'}\n"
            f"🪪 Aadhar: {doc_id or 'N/A'}\n"
            f"📍 Address: {full_address}\n"
            f"🌍 Circle: {state or 'N/A'}"
        )
        response_lines.append(record)
        
    response = "\n".join(response_lines)
    if len(response) > 4000:
        response = response[:4000] + "\n... (results truncated)"
    return response

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send message on `/start` with reply keyboard buttons."""
    keyboard = [
        [
            KeyboardButton("📱 Number Info"),
            KeyboardButton("🪪 Aadhar Info")
        ]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    text = "Welcome! I am the high-performance search bot.\nPlease choose what you want to search:"
    
    await update.message.reply_text(text, reply_markup=reply_markup)
    return CHOOSING

async def choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Parses the user's choice from the reply keyboard."""
    choice = update.message.text
    
    if choice == "📱 Number Info":
        await update.message.reply_text("Please enter the phone number you want to search:", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_PHONE
    elif choice == "🪪 Aadhar Info":
        await update.message.reply_text("Please enter the Aadhar number you want to search:", reply_markup=ReplyKeyboardRemove())
        return WAITING_FOR_DOC
    else:
        await update.message.reply_text("Please use the buttons provided at the bottom of your screen.")
        return CHOOSING

async def handle_phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the user's phone number input."""
    phone_number = update.message.text.strip()
    await update.message.reply_text("Searching user data, please wait...")
    
    results = search_by_phone(phone_number)
    
    if results:
        await update.message.reply_text(format_results(results))
    else:
        await update.message.reply_text("No user found with that phone number.")
        
    # Send the main menu again after the search is done
    await start(update, context)
    return CHOOSING

async def handle_doc_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the user's Aadhar number input."""
    doc_id = update.message.text.strip()
    await update.message.reply_text("Searching user data, please wait...")
    
    results = search_by_doc_id(doc_id)
    
    if results:
        await update.message.reply_text(format_results(results))
    else:
        await update.message.reply_text("No user found with that Aadhar number.")
        
    # Send the main menu again after the search is done
    await start(update, context)
    return CHOOSING

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels and ends the conversation."""
    await update.message.reply_text("Operation cancelled. Type /start to begin again.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Set up the conversation handler
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex('^(📱 Number Info|🪪 Aadhar Info)$'), choice_handler)
        ],
        states={
            CHOOSING: [MessageHandler(filters.TEXT & ~filters.COMMAND, choice_handler)],
            WAITING_FOR_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_phone_input)],
            WAITING_FOR_DOC: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_doc_input)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    app.add_handler(conv_handler)

    print("Optimized interactive bot is starting...")
    app.run_polling()
