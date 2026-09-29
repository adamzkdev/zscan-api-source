-- Accounts moved to Supabase (auth with Google/GitHub/email, row-level security): the tables
-- from 0019 go, except job_cursors (the alert scanner and Telegram bot positions).
DROP TABLE IF EXISTS alerts, telegram_links, watchlist, api_keys, sessions, login_tokens, users;
