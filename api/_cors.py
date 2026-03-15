CORS_HEADERS = [
    ('Access-Control-Allow-Origin', '*'),
    ('Access-Control-Allow-Methods', 'GET, POST, OPTIONS'),
    ('Access-Control-Allow-Headers', 'Content-Type, X-Telegram-Init-Data, X-Requested-With'),
]


def add_cors(handler):
    for k, v in CORS_HEADERS:
        handler.send_header(k, v)


def handle_options(handler):
    handler.send_response(204)
    add_cors(handler)
    handler.end_headers()
