import dropbox

# === CONFIGURATION ===
TEAM_ACCESS_TOKEN = "sl.u.AGHdD5qvSKKFmIE024v149V0MTSYMlzczttXWWcMmLGmE9zFFp3NYYLkx1yz0PUuI-CDL8QNb0vPFB_Ft8KTOjNaIa8-hTxouy9vmel86b3IaeGXWxwKjMu5mvuIcVLNsxI0ToVvjU0GQ3ztKNMjH3nGv8lWJC9A-f3RYAg8TTB73d7U5Av_pK5Cp03xWvwdq4HnoIa3RFsD1IheICGnt7aaBpj1sbz9xFDFhsqD-yvQjZHlazCXOduMBt-q6SsbVt_FUL567O2dGaW54JsN_w0diOoTnnUmyc8nl93anjylQkh7QxOf-hJzDZqYfM2fjHfwQbo9yEZdLPb7qd2i9BXda7W4QBpWr4wP9scfK90al60QSv-nWKA91lZgQDjf3ot1wIMdxD9SyNpoKUBclxmUMs5mMdhj1j9iKhQV_y-NybKr5h4SHwh9b0i2nKgksZoSMxSncOfCqgpeUQWauTUrrfNBDJ_i4y8QKjm5DVV23pvV2ikv9E2z5hmzUFilf7pEcg_C2EyzkKhbk8dHBDa0NbK6YoZF4v3Ikon84WystJW8z8du01uvPxvQoZghL4P5P_hApSFciebYyH1FGKS0rxQ2KIav-4KXAuc83ikgVJHdnxt_-waIcSdgxISi3mXWAN5xguwVhxB71buO3EhEP7Kt8vV0y7qt7ZiekJFhHh_tu2i6ymFU4gMy9h-sbN3U-M4BxizDfAwPO67ljgJ_CiPj8ZZsPa0kRT8CPiwO72Iwb48SyCYn1xlVkQJ0qG7xv3YB1-tU0yalk4m78iseTTGo-BVpy4BUN0voPFaCzoDOKBHtAsvJOKd_JJbKXROvkl7FnRi4ITkgEIfu6ASfoXMIiOQrkltmxHTmbGg1ZB66J1nTLt-FTqMEKAbpqCdamc8EToMk2aFll063QMw0cXkaScpJoSWjNJsoLGfENndYl5RxDbR00_iLo1ku7jcZi6u-GkrGnbDFJEcJVmHxKn92wSiSJw3humkERihkXdsu3mCk10o1l-OTfhgxMQ24zEpNbbUGWuvXzO5mXQw2aZqhUMSrz0VaOm2rinwNyGf1rFWK7X5EBbTQ_lwI2LQxI1lBDxJFM1t0WBgQnMTc8sYbeCdgWjeLR3aq3cfbRrvYBjzoZK179AsLg_OzSH0"
MEMBER_ID = "dbmid:AAAqT9SYwUCR519R3Cerb7SVD5ghiUeHTiA"                       # Replace with target user's team_member_id
OUTPUT_FILE = "dropbox_all_folders.txt"              # Where to save the folder list

# === CONNECT TO DROPBOX TEAM ===
team_dbx = dropbox.DropboxTeam(TEAM_ACCESS_TOKEN)
dbx = team_dbx.as_user(MEMBER_ID)

def list_all_folders(path=""):
    """Recursively list all folders visible to the impersonated user."""
    folders = []
    try:
        result = dbx.files_list_folder(path, recursive=True)
        while True:
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FolderMetadata):
                    folders.append(entry.path_display)
            if result.has_more:
                result = dbx.files_list_folder_continue(result.cursor)
            else:
                break
    except dropbox.exceptions.ApiError as e:
        print(f"⚠️ Dropbox API Error: {e}")
    except Exception as e:
        print(f"⚠️ General Error: {e}")
    return folders

def save_to_file(folder_list, filename):
    """Save folder paths to a text file."""
    with open(filename, "w", encoding="utf-8") as f:
        for folder in sorted(folder_list):
            f.write(folder + "\n")
    print(f"✅ Folder list saved to '{filename}' ({len(folder_list)} folders).")

def main():
    print("📂 Fetching all folders visible to this Dropbox user...")
    folders = list_all_folders("")
    if not folders:
        print("⚠️ No folders found — check if the user has access or if the path is correct.")
    else:
        for folder in folders[:10]:
            print(f"📁 {folder}")
        if len(folders) > 10:
            print(f"...and {len(folders) - 10} more.")
    save_to_file(folders, OUTPUT_FILE)

if __name__ == "__main__":
    main()
