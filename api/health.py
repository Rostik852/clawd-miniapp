from http.server import BaseHTTPRequestHandler
import json
from _cors import add_cors, handle_options


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        add_cors(self)
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())

    def log_message(self, format, *args):
        pass  # Suppress default access logs in serverless
