import http.server
import socketserver
import urllib.parse
import json
import base64
import os

import secrets

# Note: In a real implementation, we would use a library like 'webauthn' or 'fido2'
# Here we simulate the registration flow to demonstrate the UX.

_CSRF_TOKEN = None

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>HVPDB Passkey Registration</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="csrf-token" content="{csrf_token}">
    <style>
        body { font-family: sans-serif; text-align: center; padding: 20px; }
        button { padding: 15px 30px; font-size: 18px; background: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; }
        #status { margin-top: 20px; color: #666; }
        .error { color: red; }
        .success { color: green; }
    </style>
</head>
<body>
    <h2>Register Passkey for {user}</h2>
    <p>Tap the button below to create a passkey on this device.</p>
    <button onclick="register()">Create Passkey</button>
    <div id="status"></div>

    <script>
        function bufferToBase64(buffer) {
            return btoa(String.fromCharCode(...new Uint8Array(buffer)))
                .replace(/\\+/g, "-").replace(/\\//g, "_").replace(/=/g, "");
        }

        async function register() {
            const status = document.getElementById('status');
            const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content');
            status.innerText = "Requesting credential creation...";
            status.className = "";

            try {
                // 1. Get Challenge from Server
                const res = await fetch('/webauthn/challenge');
                if (!res.ok) throw new Error("Failed to get challenge");
                const options = await res.json();
                
                // Convert base64 challenge to buffer
                options.challenge = Uint8Array.from(atob(options.challenge), c => c.charCodeAt(0));
                options.user.id = Uint8Array.from(atob(options.user.id), c => c.charCodeAt(0));

                // 2. Create Credential (WebAuthn API)
                const credential = await navigator.credentials.create({ publicKey: options });
                
                // 3. Send back to server
                const response = {
                    id: credential.id,
                    rawId: bufferToBase64(credential.rawId),
                    type: credential.type,
                    response: {
                        attestationObject: bufferToBase64(credential.response.attestationObject),
                        clientDataJSON: bufferToBase64(credential.response.clientDataJSON)
                    }
                };

                const verifyRes = await fetch('/webauthn/verify', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-CSRF-Token': csrfToken
                    },
                    body: JSON.stringify(response)
                });

                if (verifyRes.ok) {
                    status.innerText = "✅ Success! Passkey registered. You can close this tab.";
                    status.className = "success";
                } else {
                    const err = await verifyRes.text();
                    status.innerText = "❌ Registration failed: " + err;
                    status.className = "error";
                }

            } catch (e) {
                status.innerText = "❌ Error: " + e.message;
                status.className = "error";
                console.error(e);
            }
        }
    </script>
</body>
</html>
"""

class WebAuthnHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.username = kwargs.pop('username', 'user')
        self.allowed_origin = kwargs.pop('allowed_origin', None)
        super().__init__(*args, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/webauthn/register':
            query = urllib.parse.parse_qs(parsed.query)
            user = query.get('user', [self.username])[0]
            csrf_token = secrets.token_hex(16)
            
            # Simple CSRF storage (in memory for single-user CLI)
            global _CSRF_TOKEN
            _CSRF_TOKEN = csrf_token
            
            html = HTML_TEMPLATE.replace('{user}', user).replace('{csrf_token}', csrf_token)
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
            return
            
        if parsed.path == '/webauthn/challenge':
            # Generate dummy challenge options
            # In a real app, 'challenge' and 'user.id' must be random bytes
            challenge_b64 = base64.b64encode(os.urandom(32)).decode('utf-8')
            user_id_b64 = base64.b64encode(os.urandom(16)).decode('utf-8')
            
            # Validate Host header against allowed origin if provided
            host = self.headers.get('Host')
            if self.allowed_origin:
                 # Strip protocol if present in allowed_origin
                 expected = self.allowed_origin.replace('http://', '').replace('https://', '')
                 if host != expected:
                     self.send_error(403, "Origin mismatch")
                     return

            rp_id = host.split(':')[0]
            
            options = {
                "challenge": challenge_b64,
                "rp": {"name": "HVPDB CLI", "id": rp_id},
                "user": {
                    "id": user_id_b64,
                    "name": self.username,
                    "displayName": self.username
                },
                "pubKeyCredParams": [{"type": "public-key", "alg": -7}], # ES256
                "timeout": 60000,
                "attestation": "none"
            }
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(options).encode('utf-8'))
            return

        self.send_error(404)

    def do_POST(self):
        if self.path == '/webauthn/verify':
            # CSRF Check
            # In a real app, this would be in a header or body. 
            # For simplicity, let's assume client sends it in a custom header 'X-CSRF-Token'
            # (We need to update HTML/JS to send it)
            client_token = self.headers.get('X-CSRF-Token')
            global _CSRF_TOKEN
            if not client_token or client_token != _CSRF_TOKEN:
                 self.send_error(403, "CSRF Token Invalid")
                 return
            
            length = int(self.headers.get('Content-Length'))
            data = self.rfile.read(length)
            credential = json.loads(data)
            
            # Here we would verify the signature using fido2 library
            # For this CLI tool demo, we just print the public key info
            
            print("\n\n[bold green]✅ RECEIVED CREDENTIAL![/bold green]")
            print(f"Credential ID: {credential['id']}")
            print("Raw ID:", credential['rawId'])
            print("(Verification skipped in demo mode - requires HTTPS for real FIDO2)")
            
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
            # We could shut down the server here, but let's keep it running for a moment
            return

def run_server(host, port, user, allowed_origin=None):
    import functools
    handler = functools.partial(WebAuthnHandler, username=user, allowed_origin=allowed_origin)
    with socketserver.TCPServer((host, port), handler) as httpd:
        print(f"Serving at http://{host}:{port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping server.")
