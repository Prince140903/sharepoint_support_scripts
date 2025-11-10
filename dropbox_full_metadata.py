import csv
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple

import dropbox
from dropbox.common import PathRoot
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata, FolderMetadata, ListRevisionsMode

# === Configuration ===
TEAM_ACCESS_TOKEN = "sl.u.AGGCcf5maXJ3qk4LeUyMZMkPj5Fg5MUyF6EwrOBHERXoCUX8tHWtkbO86oc-5BPm_1HiUnm83m7S9L5VU4ud53mHjHMTouJXuaaXCIfzo8mJgTgRRB-euF4mGFnuNfEHaqQSZQke39oxK5BjCpLguFIqL2WcxuMOcxHy3OuSzH6U-iXvvZypLZZXba5a4q0VEE49WMmv49MPqsFtedDSmJMs_PG18Q90qGaLgNlMhFAup7eDHDD50TNfVxurvOIYyBkDvk7_sMd1rr3snIe5W4zfVDUI98lb7svrTht3-qgF_KdmIvDuIPHRn08G9nPctzaotfh04VxjAaJCmjJ-53J_vpCTjSBv_mGuiX6sT3PtRmM-EiNbnFgUkVYT4q4fRH_l0-fwjWzVdnpaCb7QBb2ABNAx-7nRocD6EO8mLIahKzMS174RZM1nS-ZS6VBiA2RoRhhDAd9DB_Hr2AIodnEvhIBi6jTC3hraDYKJgQvwGfDLroNiLie_CwfJ16CXkDODKwGuxW_oJfv9JeIGjUaX8wVWUg6GedmpmS1AZnU81RCRF7nrov_q-P9hTfNQsRFa0ntUw2xE_g9R60yMVr7yZ_-j3-B-_gL5kUxoFqnATrpMg_kt5trPjSJWd462J0lJ6IEHpa4440FtM8CriNBSDKxWJBxrAVS1I2jbGjNSOde6_jK17YENO7Qm20xuU1aryyPLCecN5OOs9WnC_7BWrqE9ZbwUMreG2XZB9tvaCsb_P5CAJP1bAaHSRQmnfhj__zjQukPmebA6UH5ZqqlnOdGeAIJgWSjBR-Ic17aGKKtqB59Wzqboy91qjyQb4ou87f4qFr6wiy4tre135ntXwZW3Vs4Z_Q_BAkueQJQdGIkisJg_FKvh6m1rFXO0mZGMHFVBEzjcBAu8i5GneXmt9eZLjimg2l06hRZjyYiMlN6y5QauYjqJMXcaPhxRxxp5FHPz5-IPzbjW1jtkzLaNb9V3AaAZmADTXowtOvdO-7zUcAHcg_DFg_Brbows1F7X4AeGL6JfeDfO9_MvDlPJE5Az_icaGijYESrg4RoLVryVdhptGm14c0g8fJVRe4XwbRX9bbu_Vq8GIWm41Sna2eePX6AaeS_DuXjEzE0OKTGTtlGW0uA6UcaQ7tomLeA"
TEAM_FOLDER_ID = "1929981360"
OUTPUT_CSV = "dropbox_full_metadata.csv"
REVISION_LOOKUP_LIMIT = 100
CSV_COLUMNS = [
    "path",
    "name",
    "id",
    "type",
    "size_mb",
    "last_client_modified",
    "last_server_modified",
    "created_client_modified",
    "created_server_modified",
    "created_source",
    "rev",
    "last_modified_by_id",
    "last_modified_by_email",
    "last_modified_by_name",
    "created_by_id",
    "created_by_email",
    "created_by_name",
]

# Shared client (mirror app.py behavior)
team_dbx = dropbox.DropboxTeam(TEAM_ACCESS_TOKEN)


def format_ts(ts: Optional[datetime]) -> str:
    if not ts:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class MemberInfo:
    member_id: str
    email: str = ""
    name: str = ""


class MemberDirectory:
    def __init__(self, team_client: dropbox.DropboxTeam):
        self.team_client = team_client
        self.cache: Dict[str, MemberInfo] = {}
        self._preload_members()

    def _store_member(self, profile) -> None:
        member_id = getattr(profile, "team_member_id", "")
        if not member_id:
            return
        name = getattr(profile, "name", None)
        given_name = getattr(name, "given_name", "") if name else getattr(profile, "given_name", "")
        surname = getattr(name, "surname", "") if name else getattr(profile, "surname", "")
        display = getattr(profile, "display_name", "")
        full_name = " ".join(filter(None, [given_name, surname])).strip() or display
        self.cache[member_id] = MemberInfo(
            member_id=member_id,
            email=getattr(profile, "email", ""),
            name=full_name,
        )

    def _preload_members(self) -> None:
        try:
            result = self.team_client.team_members_list()
            self._store_members(result.members)
            while result.has_more:
                result = self.team_client.team_members_list_continue(result.cursor)
                self._store_members(result.members)
        except Exception as exc:
            print(f"⚠️ Unable to preload team members: {exc}")

    def _store_members(self, members) -> None:
        for member in members:
            self._store_member(member.profile)

    def describe(self, member_id: Optional[str]) -> MemberInfo:
        if not member_id:
            return MemberInfo(member_id="")
        return self.cache.get(member_id, MemberInfo(member_id=member_id))


def get_namespace_id(team_folder_id: str) -> Optional[str]:
    try:
        info = _get_team_folder_metadata(team_folder_id)
        if info:
            possible = [
                getattr(info, "team_shared_root_namespace_id", None),
                getattr(info, "content_team_root_namespace_id", None),
                getattr(info, "root_namespace_id", None),
            ]
            namespace = next((ns for ns in possible if ns), None)
            if namespace:
                print(f"📁 Team Folder: {info.name}")
                print(f"🔗 Namespace ID: {namespace}")
                return namespace

        print("🔍 Falling back to team_namespaces_list()...")
        namespaces = team_dbx.team_namespaces_list()
        info_name = getattr(info, "name", None) if info else None
        for ns in namespaces.namespaces:
            ns_name = getattr(ns, "name", None)
            if ns_name and info_name and ns_name == info_name:
                print(f"📁 Team Folder: {info_name}")
                print(f"🔗 Namespace ID: {ns.namespace_id}")
                return ns.namespace_id
        while True:
            if not namespaces.has_more:
                break
            namespaces = team_dbx.team_namespaces_list_continue(namespaces.cursor)
            for ns in namespaces.namespaces:
                ns_name = getattr(ns, "name", None)
                if ns_name and info_name and ns_name == info_name:
                    print(f"📁 Team Folder: {info_name}")
                    print(f"🔗 Namespace ID: {ns.namespace_id}")
                    return ns.namespace_id
    except Exception as exc:
        print(f"⚠️ Error retrieving namespace ID: {exc}")
    return None


def resolve_creation_info(
    dbx: dropbox.Dropbox, entry: FileMetadata, directory: MemberDirectory
) -> Tuple[Optional[datetime], Optional[datetime], MemberInfo, str]:
    try:
        revisions = dbx.files_list_revisions(
            entry.path_lower,
            limit=REVISION_LOOKUP_LIMIT,
            mode=ListRevisionsMode.path,
        )
        if revisions.entries:
            oldest = revisions.entries[-1]
            created_by_id = getattr(oldest.sharing_info, "modified_by", None) if oldest.sharing_info else None
            created_by = directory.describe(created_by_id)
            return (
                getattr(oldest, "client_modified", None),
                getattr(oldest, "server_modified", None),
                created_by,
                "revision",
            )
    except ApiError as api_err:
        print(f"ℹ️ Skipping revision lookup for {entry.path_display}: {api_err.error}")
    except Exception as exc:
        print(f"⚠️ Error resolving creation info for {entry.path_display}: {exc}")
    # Fallback to current metadata timestamps
    created_by = directory.describe(
        getattr(entry.sharing_info, "modified_by", None) if entry.sharing_info else None
    )
    return entry.client_modified, entry.server_modified, created_by, "fallback_current"


def resolve_last_modified(entry: FileMetadata, directory: MemberDirectory) -> MemberInfo:
    last_mod_id = getattr(entry.sharing_info, "modified_by", None) if entry.sharing_info else None
    return directory.describe(last_mod_id)


def list_entries(namespace_id: str):
    members = team_dbx.team_members_list().members
    if not members:
        raise RuntimeError("No team members available for impersonation.")
    admin_id = members[0].profile.team_member_id
    dbx = team_dbx.as_user(admin_id).with_path_root(PathRoot.namespace_id(namespace_id))
    directory = MemberDirectory(team_dbx)

    result = dbx.files_list_folder(
        path="",
        recursive=True,
        include_non_downloadable_files=True,
        include_mounted_folders=True,
    )

    while True:
        for entry in result.entries:
            if isinstance(entry, FileMetadata):
                created_client, created_server, created_by, created_source = resolve_creation_info(
                    dbx, entry, directory
                )
                last_modified_by = resolve_last_modified(entry, directory)
                last_client = entry.client_modified
                last_server = entry.server_modified
                if created_server and last_server and created_server > last_server:
                    # Ensure creation timestamp never exceeds latest server modified
                    created_server = last_server
                yield {
                    "path": entry.path_display,
                    "name": entry.name,
                    "id": entry.id,
                    "type": "file",
                    "size_mb": round(entry.size / 1024 / 1024, 2),
                    "last_client_modified": format_ts(last_client),
                    "last_server_modified": format_ts(last_server),
                    "created_client_modified": format_ts(created_client),
                    "created_server_modified": format_ts(created_server),
                    "created_source": created_source,
                    "rev": entry.rev,
                    "last_modified_by_id": last_modified_by.member_id,
                    "last_modified_by_email": last_modified_by.email,
                    "last_modified_by_name": last_modified_by.name,
                    "created_by_id": created_by.member_id,
                    "created_by_email": created_by.email,
                    "created_by_name": created_by.name,
                }
            elif isinstance(entry, FolderMetadata):
                owner = directory.describe(getattr(entry.sharing_info, "modified_by", None))
                yield {
                    "path": entry.path_display,
                    "name": entry.name,
                    "id": entry.id,
                    "type": "folder",
                    "size_mb": "",
                    "last_client_modified": "",
                    "last_server_modified": "",
                    "created_client_modified": "",
                    "created_server_modified": "",
                    "created_source": "",
                    "rev": "",
                    "last_modified_by_id": owner.member_id,
                    "last_modified_by_email": owner.email,
                    "last_modified_by_name": owner.name,
                    "created_by_id": "",
                    "created_by_email": "",
                    "created_by_name": "",
                }
        if result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
        else:
            break


def main():
    namespace_id = get_namespace_id(TEAM_FOLDER_ID)
    if not namespace_id:
        print("❌ Could not determine namespace ID for the provided team folder.")
        return

    resume = os.path.exists(OUTPUT_CSV)
    seen_paths = set()
    if resume:
        try:
            with open(OUTPUT_CSV, "r", encoding="utf-8", newline="") as existing:
                reader = csv.DictReader(existing)
                for row in reader:
                    path = row.get("path")
                    if path:
                        seen_paths.add(path)
        except Exception as exc:
            print(f"⚠️ Unable to read existing CSV, starting fresh: {exc}")
            resume = False
            seen_paths.clear()

    if resume:
        print(f"⏩ Resuming with {len(seen_paths)} existing entries already in {OUTPUT_CSV}.")
    else:
        print("⏩ Starting fresh export.")

    print("📂 Gathering Dropbox metadata ...")
    total = files = folders = skipped = 0
    with open(OUTPUT_CSV, "a" if resume else "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        if not resume:
            writer.writeheader()
        for row in list_entries(namespace_id):
            if row["path"] in seen_paths:
                skipped += 1
                if skipped <= 5 or skipped % 1000 == 0:
                    print(f"[skip] {row.get('path')}")
                continue
            writer.writerow(row)
            csvfile.flush()
            seen_paths.add(row["path"])
            total += 1
            if row.get("type") == "file":
                files += 1
            elif row.get("type") == "folder":
                folders += 1
            if row.get("type") == "folder":
                print(f"[{total}] FOLDER {row.get('path')}")
            else:
                print(f"[{total}] FILE   {row.get('path')}")
    if total == 0:
        print(f"✅ No new entries found. Existing rows kept ({len(seen_paths)} total).")
    else:
        print(f"✅ Metadata exported to {OUTPUT_CSV} (+{total} new rows | files: {files}, folders: {folders})")


def _get_team_folder_metadata(team_folder_id: str):
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


if __name__ == "__main__":
    main()

