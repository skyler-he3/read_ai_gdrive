#!/usr/bin/env bash
# setup.sh — One-command setup for move_meeting_notes (Google Drive API version)
# Run from the folder containing move_meeting_notes.py:
#   bash setup.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/move_meeting_notes.py"
CONFIG_PATH="$SCRIPT_DIR/move_meeting_notes.json"
LOG_PATH="$SCRIPT_DIR/move_meeting_notes.log"
CREDS_PATH="$SCRIPT_DIR/credentials.json"
PLIST_LABEL="local.move-meeting-notes"
PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_LABEL.plist"

echo ""
echo "=== Meeting Notes Auto-Mover Setup (Google Drive API) ==="
echo ""

# ── 1. Check Python ──────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Install it from https://python.org and re-run."
    exit 1
fi

# ── 2. Install Python dependencies ───────────────────────────────────────────
echo "Installing required Python packages..."
python3 -m pip install --quiet --upgrade \
    google-api-python-client \
    google-auth-httplib2 \
    google-auth-oauthlib
echo "Packages installed."
echo ""

# ── 3. Check for credentials.json ────────────────────────────────────────────
if [[ ! -f "$CREDS_PATH" ]]; then
    echo "──────────────────────────────────────────────────────"
    echo "  STEP: Download your Google Cloud credentials"
    echo "──────────────────────────────────────────────────────"
    echo ""
    echo "  1. Go to: https://console.cloud.google.com/"
    echo "  2. Create a new project (or select an existing one)"
    echo "  3. Go to: APIs & Services → Enable APIs"
    echo "     → Search 'Google Drive API' → Enable it"
    echo "  4. Go to: APIs & Services → Credentials"
    echo "     → Create Credentials → OAuth client ID"
    echo "     → Application type: Desktop app → Create"
    echo "  5. Click 'Download JSON' and save it as:"
    echo "     $CREDS_PATH"
    echo ""
    read -rp "Press Enter once you've placed credentials.json there: "

    if [[ ! -f "$CREDS_PATH" ]]; then
        echo "ERROR: credentials.json still not found. Exiting."
        exit 1
    fi
fi
echo "credentials.json found."
echo ""

# ── 4. Get the Clients folder ID ─────────────────────────────────────────────
echo "──────────────────────────────────────────────────────"
echo "  STEP: Get your Clients folder ID from Google Drive"
echo "──────────────────────────────────────────────────────"
echo ""
echo "  1. Open Google Drive in your browser"
echo "  2. Navigate into your Clients folder"
echo "  3. Copy the ID from the URL:"
echo "     https://drive.google.com/drive/folders/THIS_PART_IS_THE_ID"
echo ""
read -rp "Paste your Clients folder ID: " CLIENTS_FOLDER_ID

if [[ -z "$CLIENTS_FOLDER_ID" ]]; then
    echo "ERROR: No folder ID entered. Exiting."
    exit 1
fi

# ── 5. Write config ───────────────────────────────────────────────────────────
cat > "$CONFIG_PATH" <<EOF
{
  "CLIENTS_FOLDER_ID": "$CLIENTS_FOLDER_ID",
  "DRY_RUN": true
}
EOF
echo ""
echo "Config written to: $CONFIG_PATH"

# ── 6. Authenticate with Google (browser flow) ────────────────────────────────
echo ""
echo "Opening browser to authenticate with Google..."
echo "(Sign in with the account that has access to the Clients folder)"
echo ""
python3 "$SCRIPT_PATH"
echo ""

# ── 7. Write launchd plist ────────────────────────────────────────────────────
cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$PLIST_LABEL</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>$SCRIPT_PATH</string>
    </array>

    <key>StartInterval</key>
    <integer>300</integer>

    <key>RunAtLoad</key>
    <true/>

    <key>StandardOutPath</key>
    <string>$LOG_PATH</string>

    <key>StandardErrorPath</key>
    <string>$LOG_PATH</string>
</dict>
</plist>
EOF

launchctl unload "$PLIST_PATH" 2>/dev/null || true
launchctl load "$PLIST_PATH"

echo ""
echo "=== Setup complete ==="
echo ""
echo "The script will run every 5 minutes automatically via Google Drive API."
echo "Clients folder ID : $CLIENTS_FOLDER_ID"
echo ""
echo "NEXT: Ask each teammate to share their 'Read AI Meeting Notes'"
echo "      folder with your Google account (Editor access)."
echo ""
echo "When ready to go live, set DRY_RUN to false in:"
echo "  $CONFIG_PATH"
echo ""
echo "To watch the log:"
echo "  tail -f $LOG_PATH"
echo ""
echo "To stop the scheduler:"
echo "  launchctl unload $PLIST_PATH"
