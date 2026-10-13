"""Serve the device latency lab on http://localhost:8290/, cross-origin isolated.

Cross-origin isolation gives the page the browser's finest performance.now() resolution, which every timestamp
it records is read from. localhost counts as a secure context, so the microphone works without TLS.
"""

import functools
import http.server
import pathlib
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8290


class IsolatedHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


def main() -> None:
    root = pathlib.Path(__file__).parent
    handler = functools.partial(IsolatedHandler, directory=str(root))
    with http.server.ThreadingHTTPServer(("127.0.0.1", PORT), handler) as server:
        print(f"Device latency lab: http://localhost:{PORT}/  (Ctrl-C stops it)", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
