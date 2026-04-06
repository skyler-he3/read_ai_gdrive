#!/usr/bin/env python3
"""
Move meeting notes (.docx) from teammates' shared 'Read AI Meeting Notes' folders
to the correct client subfolder in the shared Clients folder, using Google Drive API.

Detection strategy:
  - Change detection : Polls Drive Changes API every 2 min — only acts when a new
                       .docx appears in a source folder. Near-instant response.
  - Weekly full scan : Every 7 days, scans all source folders completely as a
                       safety net to catch anything missed.

State is stored in state.json (page token + last full scan timestamp).

Config : move_meeting_notes.json
State  : state.json (auto-managed)
Logs   : move_meeting_notes.log (admin)  +  Move Log.txt (per teammate, in their Drive folder)
Token  : token.json (auto-created on first run)
Creds  : credentials.json (download from Google Cloud Console)
"""

import io
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
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
STATE_FILE  = SCRIPT_DIR / "state.json"

SOURCE_FOLDER_NAME  = "Read AI Meeting Notes"
DRIVE_LOG_FILENAME  = "Move Log.txt"
FULL_SCAN_INTERVAL  = timedelta(days=7)
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


# ── State ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"page_token": None, "last_full_scan": None}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def is_full_scan_due(state: dict) -> bool:
    if not state.get("last_full_scan"):
        return True
    last = datetime.fromisoformat(state["last_full_scan"])
    return datetime.now(timezone.utc) - last >= FULL_SCAN_INTERVAL


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
    return list_files(service, f"'{folder_id}' in parents and trashed = false")


def move_file(service, file_id: str, old_parent_id: str, new_parent_id: str, new_name: str) -> None:
    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parent_id,
        body={"name": new_name},
        fields="id, name, parents",
        supportsAllDrives=True,
    ).execute()


# ── Change detection ──────────────────────────────────────────────────────────

def get_start_page_token(service) -> str:
    resp = service.changes().getStartPageToken(
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return resp["startPageToken"]


def get_changed_files(service, page_token: str) -> tuple[list[dict], str]:
    """Return (list of changed file metadata, new page token)."""
    changed = []
    while True:
        resp = service.changes().list(
            pageToken=page_token,
            fields=(
                "nextPageToken, newStartPageToken, "
                "changes(removed, fileId, file(id, name, md5Checksum, parents, mimeType, trashed))"
            ),
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()

        for change in resp.get("changes", []):
            f = change.get("file")
            if f and not change.get("removed") and not f.get("trashed"):
                changed.append(f)

        page_token = resp.get("nextPageToken") or resp.get("newStartPageToken")
        if not resp.get("nextPageToken"):
            break

    return changed, page_token


# ── Teammate Drive log ────────────────────────────────────────────────────────

def fetch_existing_drive_log(service, folder_id: str) -> tuple[str, str | None]:
    files = list_files(
        service,
        query=f"name = '{DRIVE_LOG_FILENAME}' and '{folder_id}' in parents and trashed = false",
        fields="files(id, name)",
    )
    if not files:
        return "", None

    file_id = files[0]["id"]
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, service.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8"), file_id


def append_to_drive_log(service, folder_id: str, new_rows: list[str]) -> None:
    existing_text, file_id = fetch_existing_drive_log(service, folder_id)

    if existing_text.strip():
        full_text = existing_text.rstrip("\n") + "\n" + "\n".join(new_rows) + "\n"
    else:
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


# ── File processing ───────────────────────────────────────────────────────────

def extract_topic(stem: str) -> str:
    if len(stem) > 13 and stem[4] == "-" and stem[7] == "-" and stem[10:13] == " - ":
        return stem[13:]
    return stem


def find_client_match(topic: str, client_map: dict[str, str]) -> tuple[str, str] | tuple[None, None]:
    topic_lower = topic.lower()
    for name in sorted(client_map, key=len, reverse=True):
        if name.lower() in topic_lower:
            return name, client_map[name]
    return None, None


def is_duplicate(source_md5: str, dest_files: list) -> bool:
    return any(f.get("md5Checksum") == source_md5 for f in dest_files)


def process_files(
    service,
    files: list[dict],
    source_folder_id: str,
    owner: str,
    client_map: dict[str, str],
    dry_run: bool,
) -> int:
    """Process a list of .docx files from one source folder. Returns number moved."""
    new_log_rows: list[str] = []
    moved = 0

    for file in sorted(files, key=lambda f: f["name"]):
        name = file["name"]
        topic = extract_topic(Path(name).stem)
        client_name, client_folder_id = find_client_match(topic, client_map)

        if client_name is None:
            log.info("SKIP    %s  (no client match)", name)
            continue

        if dry_run:
            log.info("MOVE    %s  ->  %s/Team Documents/Meeting Notes/", name, client_name)
            moved += 1
            continue

        team_docs_id     = get_or_create_folder(service, "Team Documents", client_folder_id)
        meeting_notes_id = get_or_create_folder(service, "Meeting Notes", team_docs_id)

        dest_files = list_folder_children(service, meeting_notes_id)
        source_md5 = file.get("md5Checksum", "")

        if source_md5 and is_duplicate(source_md5, dest_files):
            log.info("SKIP    %s  (duplicate content)", name)
            continue

        dest_names = {f["name"] for f in dest_files}
        dest_name = name
        if dest_name in dest_names:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            stem, ext = Path(name).stem, Path(name).suffix
            dest_name = f"{stem} ({ts}){ext}"

        move_file(service, file["id"], source_folder_id, meeting_notes_id, dest_name)

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        dest_path = f"{client_name}/Team Documents/Meeting Notes/"
        log.info("MOVED   %s  ->  %s%s", name, dest_path, dest_name)
        new_log_rows.append(f"{ts:<20}  {name:<50}  {dest_path}{dest_name}")
        moved += 1

    if new_log_rows and not dry_run:
        append_to_drive_log(service, source_folder_id, new_log_rows)
        log.info("Move Log.txt updated for %s (%d row(s))", owner, len(new_log_rows))

    return moved


# ── Main run ──────────────────────────────────────────────────────────────────

def run(config: dict) -> None:
    clients_folder_id: str = config["CLIENTS_FOLDER_ID"]
    dry_run: bool = config.get("DRY_RUN", False)

    service  = get_service()
    state    = load_state()

    # ── Build client map ──────────────────────────────────────────────────────
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

    # ── Find all source folders ───────────────────────────────────────────────
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
    source_folder_ids = {f["id"]: f for f in source_folders}

    if not source_folder_ids:
        log.info("No '%s' folders shared with you.", SOURCE_FOLDER_NAME)
        save_state({**state, "page_token": get_start_page_token(service)})
        return

    # ── Initialise page token if first run ────────────────────────────────────
    if not state.get("page_token"):
        state["page_token"] = get_start_page_token(service)
        log.info("First run — initialising change token and running full scan.")
        save_state(state)

    # ── Weekly full scan ──────────────────────────────────────────────────────
    if is_full_scan_due(state):
        log.info("=== Weekly full scan ===")
        total = 0
        for folder in source_folders:
            owner     = folder.get("owners", [{}])[0].get("emailAddress", "unknown")
            folder_id = folder["id"]
            log.info("Scanning folder from: %s", owner)
            docx_files = [
                f for f in list_folder_children(service, folder_id)
                if f["name"].endswith(".docx")
            ]
            moved = process_files(service, docx_files, folder_id, owner, client_map, dry_run)
            total += moved
        log.info("Weekly scan done. Total moved: %d", total)
        state["last_full_scan"] = datetime.now(timezone.utc).isoformat()
        state["page_token"] = get_start_page_token(service)
        save_state(state)
        return

    # ── Change detection ──────────────────────────────────────────────────────
    changed_files, new_page_token = get_changed_files(service, state["page_token"])

    # Filter: only .docx files that live in one of our source folders
    files_by_folder: dict[str, list] = {}
    for f in changed_files:
        if not f["name"].endswith(".docx"):
            continue
        for parent_id in f.get("parents", []):
            if parent_id in source_folder_ids:
                files_by_folder.setdefault(parent_id, []).append(f)
                break

    if not files_by_folder:
        state["page_token"] = new_page_token
        save_state(state)
        return  # nothing new — silent exit

    log.info("=== New file(s) detected ===")
    total = 0
    for folder_id, files in files_by_folder.items():
        folder = source_folder_ids[folder_id]
        owner  = folder.get("owners", [{}])[0].get("emailAddress", "unknown")
        log.info("Processing %d new file(s) from: %s", len(files), owner)
        total += process_files(service, files, folder_id, owner, client_map, dry_run)

    log.info("Done. Moved: %d", total)
    state["page_token"] = new_page_token
    save_state(state)


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
