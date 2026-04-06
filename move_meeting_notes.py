#!/usr/bin/env python3
"""
Move meeting notes (.docx) from teammates' shared 'Read AI Meeting Notes' folders
to the correct client subfolder in the shared Clients folder, using Google Drive API.

Flow:
  1. Authenticates you (admin) via browser once — token saved locally.
  2. Finds all 'Read AI Meeting Notes' folders shared with your account.
  3. For each .docx file, matches the client name against folders in Clients.
  4. Creates Team Documents/Meeting Notes under the client folder if needed.
  5. Skips duplicates using Drive's built-in MD5 checksum (no download needed).
  6. Moves matched files to the correct destination.
  7. Writes a Move Log.txt back into each teammate's folder so they can see activity.

Config : move_meeting_notes.json
Logs   : move_meeting_notes.log (admin)  +  Move Log.txt (per teammate, in their Drive folder)
Token  : token.json (auto-created on first run)
Creds  : credentials.json (download from Google Cloud Console)
"""

import io
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "move_meeting_notes.json"
LOG_FILE    = SCRIPT_DIR / "move_meeting_notes.log"
TOKEN_FILE  = SCRIPT_DIR / "token.json"
CREDS_FILE  = SCRIPT_DIR / "credentials.json"

SOURCE_FOLDER_NAME = "Read AI Meeting Notes"
DRIVE_LOG_FILENAME  = "Move Log.txt"
LOG_HEADER = (
    f"{'TIMESTAMP':<20}  {'FILE':<50}  MOVED TO\n"
    f"{'-'*20}  {'-'*50}  {'-'*60}"
)
SCOPES = ["https://www.googleapis.com/auth/drive"]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_service():
    if not CREDS_FILE.exists():
        log.error("credentials.json not found in %s", SCRIPT_DIR)
        log.error("Download it from Google Cloud Console and place it there.")
        sys.exit(1)

    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())

    return build("drive", "v3", credentials=creds)


# ── Drive helpers ─────────────────────────────────────────────────────────────

def list_files(service, query: str, fields: str = "files(id, name, md5Checksum, parents)") -> list:
    results = []
    page_token = None
    while True:
        resp = service.files().list(
            q=query,
            fields=f"nextPageToken, {fields}",
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def get_or_create_folder(service, name: str, parent_id: str) -> str:
    """Return the folder ID for name under parent_id, creating it if needed."""
    query = (
        f"name = '{name}' "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents "
        f"and trashed = false"
    )
    files = list_files(service, query, fields="files(id, name)")
    if files:
        return files[0]["id"]

    folder = service.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()
    log.info("CREATED folder '%s' under parent %s", name, parent_id)
    return folder["id"]


def list_folder_children(service, folder_id: str) -> list:
    query = f"'{folder_id}' in parents and trashed = false"
    return list_files(service, query)


def move_file(service, file_id: str, old_parent_id: str, new_parent_id: str, new_name: str) -> None:
    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parent_id,
        body={"name": new_name},
        fields="id, name, parents",
        supportsAllDrives=True,
    ).execute()


# ── Teammate Drive log ────────────────────────────────────────────────────────

def fetch_existing_drive_log(service, folder_id: str) -> tuple[str, str | None]:
    """Return (existing_text, file_id) of Move Log.txt in folder, or ('', None)."""
    files = list_files(
        service,
        query=f"name = '{DRIVE_LOG_FILENAME}' and '{folder_id}' in parents and trashed = false",
        fields="files(id, name)",
    )
    if not files:
        return "", None

    file_id = files[0]["id"]
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8"), file_id


def append_to_drive_log(service, folder_id: str, new_rows: list[str]) -> None:
    """Append new table rows to Move Log.txt, creating with header if it doesn't exist."""
    existing_text, file_id = fetch_existing_drive_log(service, folder_id)

    if existing_text.strip():
        # File exists — append new rows below existing content
        full_text = existing_text.rstrip("\n") + "\n" + "\n".join(new_rows) + "\n"
    else:
        # First time — write header + rows
        full_text = LOG_HEADER + "\n" + "\n".join(new_rows) + "\n"

    media = MediaIoBaseUpload(
        io.BytesIO(full_text.encode("utf-8")),
        mimetype="text/plain",
        resumable=False,
    )
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        service.files().create(
            body={"name": DRIVE_LOG_FILENAME, "parents": [folder_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()


# ── Core logic ────────────────────────────────────────────────────────────────

def extract_topic(stem: str) -> str:
    """Strip 'YYYY-MM-DD - ' prefix and return the meeting topic."""
    if len(stem) > 13 and stem[4] == "-" and stem[7] == "-" and stem[10:13] == " - ":
        return stem[13:]
    return stem


def find_client_match(topic: str, client_map: dict[str, str]) -> tuple[str, str] | tuple[None, None]:
    """Return (client_name, folder_id) for the best match, or (None, None)."""
    topic_lower = topic.lower()
    for name in sorted(client_map, key=len, reverse=True):
        if name.lower() in topic_lower:
            return name, client_map[name]
    return None, None


def is_duplicate(source_md5: str, dest_files: list) -> bool:
    """Return True if any file in dest_files shares the same MD5."""
    return any(f.get("md5Checksum") == source_md5 for f in dest_files)


def run(config: dict) -> None:
    clients_folder_id: str = config["CLIENTS_FOLDER_ID"]
    dry_run: bool = config.get("DRY_RUN", False)

    service = get_service()

    # ── Find all shared 'Read AI Meeting Notes' folders ──────────────────────
    source_folders = list_files(
        service,
        query=(
            f"name = '{SOURCE_FOLDER_NAME}' "
            f"and mimeType = 'application/vnd.google-apps.folder' "
            f"and sharedWithMe = true "
            f"and trashed = false"
        ),
        fields="files(id, name, owners)",
    )

    if not source_folders:
        log.info("No '%s' folders shared with you. Nothing to do.", SOURCE_FOLDER_NAME)
        return

    # ── Build client name → folder ID map ────────────────────────────────────
    client_folders = list_files(
        service,
        query=(
            f"'{clients_folder_id}' in parents "
            f"and mimeType = 'application/vnd.google-apps.folder' "
            f"and trashed = false"
        ),
        fields="files(id, name)",
    )
    client_map = {f["name"]: f["id"] for f in client_folders}

    if not client_map:
        log.warning("No client folders found in Clients folder (%s)", clients_folder_id)
        return

    log.info("========================================")
    log.info("Run started")
    log.info("Source folders : %d shared '%s' folder(s)", len(source_folders), SOURCE_FOLDER_NAME)
    log.info("Clients        : %s", ", ".join(sorted(client_map)))
    if dry_run:
        log.info("[DRY RUN] No files will be moved.")

    total_moved = total_skipped = 0

    for source_folder in source_folders:
        owner = source_folder.get("owners", [{}])[0].get("emailAddress", "unknown")
        folder_id = source_folder["id"]
        run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        log.info("--- Processing folder from: %s ---", owner)

        docx_files = [
            f for f in list_folder_children(service, folder_id)
            if f["name"].endswith(".docx")
        ]

        # Rows to append to Move Log.txt — only actual moves, no noise
        new_log_rows: list[str] = []

        if not docx_files:
            log.info("No .docx files found.")
            continue

        moved = skipped = 0

        for file in sorted(docx_files, key=lambda f: f["name"]):
            name = file["name"]
            topic = extract_topic(Path(name).stem)
            client_name, client_folder_id = find_client_match(topic, client_map)

            if client_name is None:
                log.info("SKIP    %s  (no client match)", name)
                skipped += 1
                continue

            if dry_run:
                log.info("MOVE    %s  ->  %s/Team Documents/Meeting Notes/", name, client_name)
                moved += 1
                continue

            # Ensure Team Documents/Meeting Notes/ exists
            team_docs_id     = get_or_create_folder(service, "Team Documents", client_folder_id)
            meeting_notes_id = get_or_create_folder(service, "Meeting Notes", team_docs_id)

            # Duplicate check via Drive MD5 — no download needed
            dest_files = list_folder_children(service, meeting_notes_id)
            source_md5 = file.get("md5Checksum", "")

            if source_md5 and is_duplicate(source_md5, dest_files):
                log.info("SKIP    %s  (duplicate content in destination)", name)
                skipped += 1
                continue

            # Resolve filename collision (same name, different content)
            dest_names = {f["name"] for f in dest_files}
            dest_name = name
            if dest_name in dest_names:
                ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                stem, ext = Path(name).stem, Path(name).suffix
                dest_name = f"{stem} ({ts}){ext}"

            move_file(service, file["id"], folder_id, meeting_notes_id, dest_name)

            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            dest_path = f"{client_name}/Team Documents/Meeting Notes/"
            log.info("MOVED   %s  ->  %s%s", name, dest_path, dest_name)
            new_log_rows.append(f"{ts:<20}  {name:<50}  {dest_path}{dest_name}")
            moved += 1

        log.info("Done. Moved: %d  |  Skipped: %d", moved, skipped)
        total_moved += moved
        total_skipped += skipped

        # Only write to Drive log if files were actually moved
        if new_log_rows and not dry_run:
            append_to_drive_log(service, folder_id, new_log_rows)
            log.info("Move Log.txt updated in %s's folder (%d row(s))", owner, len(new_log_rows))

    log.info("========================================")
    log.info("Total — Moved: %d  |  Skipped: %d", total_moved, total_skipped)


def main():
    if not CONFIG_FILE.exists():
        log.error("Config not found: %s", CONFIG_FILE)
        log.error("Run setup.sh first.")
        sys.exit(1)
    with CONFIG_FILE.open() as f:
        config = json.load(f)
    run(config)


if __name__ == "__main__":
    main()
