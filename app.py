import csv
from datetime import datetime
import os
from typing import Dict, Optional, Tuple

import dropbox
from dropbox.common import PathRoot

# === Configuration ===
TEAM_ACCESS_TOKEN = "sl.u.AGHdD5qvSKKFmIE024v149V0MTSYMlzczttXWWcMmLGmE9zFFp3NYYLkx1yz0PUuI-CDL8QNb0vPFB_Ft8KTOjNaIa8-hTxouy9vmel86b3IaeGXWxwKjMu5mvuIcVLNsxI0ToVvjU0GQ3ztKNMjH3nGv8lWJC9A-f3RYAg8TTB73d7U5Av_pK5Cp03xWvwdq4HnoIa3RFsD1IheICGnt7aaBpj1sbz9xFDFhsqD-yvQjZHlazCXOduMBt-q6SsbVt_FUL567O2dGaW54JsN_w0diOoTnnUmyc8nl93anjylQkh7QxOf-hJzDZqYfM2fjHfwQbo9yEZdLPb7qd2i9BXda7W4QBpWr4wP9scfK90al60QSv-nWKA91lZgQDjf3ot1wIMdxD9SyNpoKUBclxmUMs5mMdhj1j9iKhQV_y-NybKr5h4SHwh9b0i2nKgksZoSMxSncOfCqgpeUQWauTUrrfNBDJ_i4y8QKjm5DVV23pvV2ikv9E2z5hmzUFilf7pEcg_C2EyzkKhbk8dHBDa0NbK6YoZF4v3Ikon84WystJW8z8du01uvPxvQoZghL4P5P_hApSFciebYyH1FGKS0rxQ2KIav-4KXAuc83ikgVJHdnxt_-waIcSdgxISi3mXWAN5xguwVhxB71buO3EhEP7Kt8vV0y7qt7ZiekJFhHh_tu2i6ymFU4gMy9h-sbN3U-M4BxizDfAwPO67ljgJ_CiPj8ZZsPa0kRT8CPiwO72Iwb48SyCYn1xlVkQJ0qG7xv3YB1-tU0yalk4m78iseTTGo-BVpy4BUN0voPFaCzoDOKBHtAsvJOKd_JJbKXROvkl7FnRi4ITkgEIfu6ASfoXMIiOQrkltmxHTmbGg1ZB66J1nTLt-FTqMEKAbpqCdamc8EToMk2aFll063QMw0cXkaScpJoSWjNJsoLGfENndYl5RxDbR00_iLo1ku7jcZi6u-GkrGnbDFJEcJVmHxKn92wSiSJw3humkERihkXdsu3mCk10o1l-OTfhgxMQ24zEpNbbUGWuvXzO5mXQw2aZqhUMSrz0VaOm2rinwNyGf1rFWK7X5EBbTQ_lwI2LQxI1lBDxJFM1t0WBgQnMTc8sYbeCdgWjeLR3aq3cfbRrvYBjzoZK179AsLg_OzSH0"
MEMBER_ID = "dbmid:AAAqT9SYwUCR519R3Cerb7SVD5ghiUeHTiA"  # Replace with target user's team_member_id
# SEARCH_KEYWORD = "AFYA"  # ✅ Replace with the exact Dropbox folder path (case-sensitive)
TEAM_FOLDER_ID = "1929981360"
OUTPUT_CSV = "dropbox_metadata_afya.csv"
MAPPING_CSV = "mapping.csv"
CSV_BATCH_SIZE = 500
CSV_COLUMNS = [
    "path",
    "name",
    "id",
    "type",
    "size_mb",
    "client_modified",
    "server_modified",
    "rev",
    "last_modified_by_id",
    "last_modified_by_email",
    "last_modified_by_name",
    "created_at",
    "created_by_id",
    "created_by_email",
    "created_by_name",
]
SHAREPOINT_LIBRARY_ROOT = "/sites/YourSite/Shared Documents"  # Update to your library's server-relative path
SHAREPOINT_ADMIN_UPN = "sharepointadmin@yourtenant.onmicrosoft.com"  # Update to your SharePoint admin UPN
MAPPING_COLUMNS = ["FilePath", "AuthorUPN", "EditorUPN"]
class CsvBatchWriter:
    def __init__(self, filename: str, fieldnames, resume: bool = False):
        self.filename = filename
        self.fieldnames = fieldnames
        self.resume = resume
        self._init_file()

    def _init_file(self):
        if self.resume and os.path.exists(self.filename):
            return
        with open(self.filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()

    def write_rows(self, rows):
        if not rows:
            return
        try:
            with open(self.filename, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writerows(rows)
        except PermissionError as exc:
            raise PermissionError(
                f"Unable to write to '{self.filename}'. Close any application using the file and retry."
            ) from exc


# === Connect to Dropbox Team as specific user ===
team_dbx = dropbox.DropboxTeam(TEAM_ACCESS_TOKEN)

def get_namespace_id(team_folder_id):
    """Retrieve namespace ID for the specified team folder — works across all Dropbox Team configs."""
    try:
        info = _get_team_folder_metadata(team_folder_id)
        if not info:
            raise AttributeError("Team folder metadata not found.")

        possible_ids = [
            getattr(info, "team_shared_root_namespace_id", None),
            getattr(info, "content_team_root_namespace_id", None),
            getattr(info, "root_namespace_id", None),
        ]
        ns_id = next((x for x in possible_ids if x), None)

        # 2️⃣ If still not found — fall back to the namespaces list
        if not ns_id:
            print("🔍 Falling back to team_namespaces_list()...")
            namespaces = team_dbx.team_namespaces_list()
            info_name = getattr(info, "name", None)
            for ns in namespaces.namespaces:
                ns_name = getattr(ns, "name", None)
                if ns_name and info_name and ns_name == info_name:
                    ns_id = ns.namespace_id
                    break

        # 3️⃣ Handle pagination (if needed)
        while not ns_id and namespaces.has_more:
            namespaces = team_dbx.team_namespaces_list_continue(namespaces.cursor)
            for ns in namespaces.namespaces:
                ns_name = getattr(ns, "name", None)
                if ns_name and info_name and ns_name == info_name:
                    ns_id = ns.namespace_id
                    break

        if not ns_id:
            raise AttributeError("No namespace found matching this team folder name.")

        print(f"📁 Team Folder: {info.name}")
        print(f"🔗 Namespace ID: {ns_id}")
        return ns_id

    except Exception as e:
        print(f"⚠️ Error fetching namespace ID: {e}")
        return None


def format_ts(ts: Optional[datetime]) -> str:
    if not ts:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M:%S")


class MemberDirectory:
    """Resolve team_member_id values to human readable details."""

    def __init__(self, team_client: dropbox.DropboxTeam):
        self.team_client = team_client
        self._cache: Dict[str, Dict[str, str]] = {}
        self._populate_cache()

    def _populate_cache(self) -> None:
        try:
            result = self.team_client.team_members_list()
            self._store_members(result.members)
            while result.has_more:
                result = self.team_client.team_members_list_continue(result.cursor)
                self._store_members(result.members)
        except Exception as exc:
            print(f"⚠️ Unable to pre-load member directory: {exc}")

    def _store_members(self, members) -> None:
        for member in members:
            profile = member.profile
            name = getattr(profile, "name", None)
            given_name = getattr(name, "given_name", "") if name else getattr(profile, "given_name", "")
            surname = getattr(name, "surname", "") if name else getattr(profile, "surname", "")
            display_name = getattr(profile, "display_name", "")
            full_name = " ".join(filter(None, [given_name, surname])).strip()
            resolved_name = full_name if full_name else display_name
            self._cache[profile.team_member_id] = {
                "email": profile.email,
                "name": resolved_name,
            }

    def describe(self, member_id: Optional[str]) -> Dict[str, str]:
        if not member_id:
            return {}
        info = self._cache.get(member_id, {})
        return {
            "member_id": member_id,
            "email": info.get("email", ""),
            "name": info.get("name", ""),
        }


def resolve_member(directory: MemberDirectory, member_id: Optional[str]) -> Tuple[str, str, str]:
    info = directory.describe(member_id)
    return info.get("member_id", ""), info.get("email", ""), info.get("name", "")


def resolve_file_creation(dbx: dropbox.Dropbox, path_lower: str, directory: MemberDirectory) -> Tuple[str, str, str, str]:
    """Attempt to determine file creation metadata from the oldest available revision."""
    try:
        revisions = dbx.files_list_revisions(path_lower, limit=100, mode=dropbox.files.ListRevisionsMode.path)
        if revisions.entries:
            oldest = revisions.entries[-1]
            member_id, email, name = resolve_member(
                directory,
                getattr(oldest.sharing_info, "modified_by", None) if oldest.sharing_info else None,
            )
            return (
                format_ts(oldest.server_modified),
                member_id,
                email,
                name,
            )
    except dropbox.exceptions.ApiError as api_err:
        # Older plans or permissions may forbid revision history; ignore gracefully.
        print(f"ℹ️ Revision lookup skipped for {path_lower}: {api_err}")
    except Exception as exc:
        print(f"⚠️ Unexpected error while inspecting revisions for {path_lower}: {exc}")
    return "", "", "", ""


def list_all_entries(namespace_id: str, csv_writer: CsvBatchWriter, mapping_writer: CsvBatchWriter, skip_paths, starting_counts):
    """List all files and folders in the team folder using its namespace ID and stream to CSV."""
    total_entries = starting_counts["total"]
    file_count = starting_counts["files"]
    folder_count = starting_counts["folders"]
    pending_rows = []
    pending_mapping_rows = []
    interrupted = False

    try:
        members = team_dbx.team_members_list().members
        if not members:
            raise RuntimeError("No team members found for impersonation.")
        admin_id = members[0].profile.team_member_id
        dbx = team_dbx.as_user(admin_id).with_path_root(PathRoot.namespace_id(namespace_id))
        member_directory = MemberDirectory(team_dbx)

        result = dbx.files_list_folder(
            path="",
            recursive=True,
            include_non_downloadable_files=True,
            include_mounted_folders=True,
        )

        while True:
            for entry in result.entries:
                if entry.path_display in skip_paths:
                    continue
                base = {
                    "path": entry.path_display,
                    "name": entry.name,
                    "id": entry.id,
                    "type": entry.__class__.__name__.replace("Metadata", "").lower(),
                }

                if isinstance(entry, dropbox.files.FileMetadata):
                    file_count += 1
                    last_mod_id, last_mod_email, last_mod_name = resolve_member(
                        member_directory,
                        getattr(entry.sharing_info, "modified_by", None) if entry.sharing_info else None,
                    )
                    created_at, created_id, created_email, created_name = resolve_file_creation(
                        dbx,
                        entry.path_lower,
                        member_directory,
                    )
                    item = {
                        **base,
                        "size_mb": round(entry.size / 1024 / 1024, 2),
                        "client_modified": format_ts(entry.client_modified),
                        "server_modified": format_ts(entry.server_modified),
                        "rev": entry.rev,
                        "last_modified_by_id": last_mod_id,
                        "last_modified_by_email": last_mod_email,
                        "last_modified_by_name": last_mod_name,
                        "created_at": created_at,
                        "created_by_id": created_id,
                        "created_by_email": created_email,
                        "created_by_name": created_name,
                    }
                    mapping_rows = build_mapping_rows(
                        entry.path_display,
                        created_email,
                        last_mod_email,
                    )
                    pending_mapping_rows.extend(mapping_rows)
                elif isinstance(entry, dropbox.files.FolderMetadata):
                    folder_count += 1
                    print(f"📁 Completed folder #{folder_count}: {entry.path_display}")
                    last_mod_id = getattr(entry.sharing_info, "modified_by", None) if entry.sharing_info else None
                    mod_id, mod_email, mod_name = resolve_member(member_directory, last_mod_id)
                    item = {
                        **base,
                        "size_mb": "",
                        "client_modified": "",
                        "server_modified": "",
                        "rev": "",
                        "last_modified_by_id": mod_id,
                        "last_modified_by_email": mod_email,
                        "last_modified_by_name": mod_name,
                        "created_at": "",
                        "created_by_id": "",
                        "created_by_email": "",
                        "created_by_name": "",
                    }
                else:
                    # Skip deletions or other metadata types we are not interested in.
                    continue

                pending_rows.append(item)
                total_entries += 1

                if len(pending_rows) >= CSV_BATCH_SIZE:
                    csv_writer.write_rows(pending_rows)
                    pending_rows.clear()
                if len(pending_mapping_rows) >= CSV_BATCH_SIZE:
                    mapping_writer.write_rows(pending_mapping_rows)
                    pending_mapping_rows.clear()

            if result.has_more:
                result = dbx.files_list_folder_continue(result.cursor)
            else:
                break
    except dropbox.exceptions.AuthError as auth_err:
        interrupted = True
        print(f"⚠️ Stopping scan due to authentication error: {auth_err}")
    except Exception as e:
        interrupted = True
        print(f"⚠️ Error listing files: {e}")
    finally:
        if pending_rows:
            try:
                csv_writer.write_rows(pending_rows)
            except PermissionError as perm_err:
                print(perm_err)
        if pending_mapping_rows:
            try:
                mapping_writer.write_rows(pending_mapping_rows)
            except PermissionError as perm_err:
                print(perm_err)

    return {
        "total": total_entries,
        "files": file_count,
        "folders": folder_count,
        "new_entries": total_entries - starting_counts["total"],
        "interrupted": interrupted,
    }

def export_to_csv(items, filename):
    """Save metadata to CSV."""
    if not items:
        print("⚠️ No files found.")
        return
    keys = items[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)
    print(f"✅ Metadata exported to {filename} ({len(items)} items)")

def main():
    ns_id = get_namespace_id(TEAM_FOLDER_ID)
    if not ns_id:
        print("❌ Could not retrieve namespace ID.")
        return

    existing_paths, existing_counts = load_existing_records(OUTPUT_CSV)
    if existing_paths:
        print(f"⏩ Resuming with {existing_counts['total']} existing entries already in {OUTPUT_CSV}.")

    print("\n📂 Fetching all files from the team folder...")
    csv_writer = CsvBatchWriter(OUTPUT_CSV, CSV_COLUMNS, resume=bool(existing_paths))
    mapping_writer = CsvBatchWriter(MAPPING_CSV, MAPPING_COLUMNS, resume=os.path.exists(MAPPING_CSV))
    stats = list_all_entries(ns_id, csv_writer, mapping_writer, existing_paths, existing_counts)
    summary = (
        f"📄 Total entries exported: {stats['total']} "
        f"(files: {stats['files']}, folders: {stats['folders']}, new: {stats['new_entries']})"
    )
    if stats.get("interrupted"):
        summary += " — scan stopped early due to authentication issues."
    print(summary)


def _get_team_folder_metadata(team_folder_id):
    """Return TeamFolderMetadata object for the given team_folder_id."""
    try:
        result = team_dbx.team_team_folder_list()
        while True:
            for folder in result.team_folders:
                if folder.team_folder_id == team_folder_id:
                    return folder
            if not result.has_more:
                break
            result = team_dbx.team_team_folder_list_continue(result.cursor)
    except Exception as exc:
        print(f"⚠️ Error retrieving team folder list: {exc}")
    return None


def load_existing_records(filename: str):
    if not os.path.exists(filename):
        return set(), {"total": 0, "files": 0, "folders": 0}
    try:
        with open(filename, "r", encoding="utf-8", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            seen_paths = set()
            total = files = folders = 0
            for row in reader:
                path = row.get("path")
                entry_type = row.get("type")
                if not path:
                    continue
                seen_paths.add(path)
                total += 1
                if entry_type == "file":
                    files += 1
                elif entry_type == "folder":
                    folders += 1
            return seen_paths, {"total": total, "files": files, "folders": folders}
    except PermissionError:
        print(
            f"⚠️ Cannot read existing CSV '{filename}'. Close any application locking the file to resume."
        )
        return set(), {"total": 0, "files": 0, "folders": 0}


def build_mapping_rows(dropbox_path: str, author_email: str, editor_email: str):
    sharepoint_path = build_sharepoint_path(dropbox_path)
    author_upn = normalize_upn(author_email)
    editor_upn = normalize_upn(editor_email) or author_upn

    return [
        {
            "FilePath": sharepoint_path,
            "AuthorUPN": author_upn,
            "EditorUPN": editor_upn if editor_upn else author_upn,
        }
    ]


def build_sharepoint_path(dropbox_path: str) -> str:
    relative_path = dropbox_path.lstrip("/")
    if not relative_path:
        return SHAREPOINT_LIBRARY_ROOT
    return "/".join(
        [SHAREPOINT_LIBRARY_ROOT.rstrip("/"), relative_path]
    )


def normalize_upn(email: Optional[str]) -> str:
    email = (email or "").strip()
    if email:
        return email
    return SHAREPOINT_ADMIN_UPN

if __name__ == "__main__":
    main()