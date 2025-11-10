import csv
import dropbox

TEAM_TOKEN = "sl.u.AGHdD5qvSKKFmIE024v149V0MTSYMlzczttXWWcMmLGmE9zFFp3NYYLkx1yz0PUuI-CDL8QNb0vPFB_Ft8KTOjNaIa8-hTxouy9vmel86b3IaeGXWxwKjMu5mvuIcVLNsxI0ToVvjU0GQ3ztKNMjH3nGv8lWJC9A-f3RYAg8TTB73d7U5Av_pK5Cp03xWvwdq4HnoIa3RFsD1IheICGnt7aaBpj1sbz9xFDFhsqD-yvQjZHlazCXOduMBt-q6SsbVt_FUL567O2dGaW54JsN_w0diOoTnnUmyc8nl93anjylQkh7QxOf-hJzDZqYfM2fjHfwQbo9yEZdLPb7qd2i9BXda7W4QBpWr4wP9scfK90al60QSv-nWKA91lZgQDjf3ot1wIMdxD9SyNpoKUBclxmUMs5mMdhj1j9iKhQV_y-NybKr5h4SHwh9b0i2nKgksZoSMxSncOfCqgpeUQWauTUrrfNBDJ_i4y8QKjm5DVV23pvV2ikv9E2z5hmzUFilf7pEcg_C2EyzkKhbk8dHBDa0NbK6YoZF4v3Ikon84WystJW8z8du01uvPxvQoZghL4P5P_hApSFciebYyH1FGKS0rxQ2KIav-4KXAuc83ikgVJHdnxt_-waIcSdgxISi3mXWAN5xguwVhxB71buO3EhEP7Kt8vV0y7qt7ZiekJFhHh_tu2i6ymFU4gMy9h-sbN3U-M4BxizDfAwPO67ljgJ_CiPj8ZZsPa0kRT8CPiwO72Iwb48SyCYn1xlVkQJ0qG7xv3YB1-tU0yalk4m78iseTTGo-BVpy4BUN0voPFaCzoDOKBHtAsvJOKd_JJbKXROvkl7FnRi4ITkgEIfu6ASfoXMIiOQrkltmxHTmbGg1ZB66J1nTLt-FTqMEKAbpqCdamc8EToMk2aFll063QMw0cXkaScpJoSWjNJsoLGfENndYl5RxDbR00_iLo1ku7jcZi6u-GkrGnbDFJEcJVmHxKn92wSiSJw3humkERihkXdsu3mCk10o1l-OTfhgxMQ24zEpNbbUGWuvXzO5mXQw2aZqhUMSrz0VaOm2rinwNyGf1rFWK7X5EBbTQ_lwI2LQxI1lBDxJFM1t0WBgQnMTc8sYbeCdgWjeLR3aq3cfbRrvYBjzoZK179AsLg_OzSH0"  # team access token
INPUT = "dropbox_metadata_afya.csv"
OUTPUT = "mapping.csv"
FALLBACK = "sharepoint.admin@ducorpgroup.com"
LIBRARY_PREFIX = "/sites/Testing-site/Shared Documents"

team_dbx = dropbox.DropboxTeam(TEAM_TOKEN)


def lookup_emails(account_ids):
    ids = [acc_id for acc_id in account_ids if acc_id and acc_id.startswith("dbid:")]
    if not ids:
        return {}, set()

    lookup = {}
    unresolved = set()

    try:
        members = team_dbx.team_members_list().members
        if not members:
            raise RuntimeError("No team members available to impersonate.")
        admin_id = members[0].profile.team_member_id
        dbx_user = team_dbx.as_user(admin_id)

        chunk_size = 300
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            try:
                resp = dbx_user.users_get_account_batch(chunk)
                for acct in resp:
                    lookup[f"dbid:{acct.account_id}"] = acct.email
            except dropbox.exceptions.ApiError as exc:
                error = getattr(exc, "error", None)
                if error and error.is_no_account():
                    unresolved.add(error.get_no_account())
                else:
                    print(f"⚠️ Unexpected error for chunk starting {chunk[0]}: {exc}")
    except Exception as exc:
        print(f"⚠️ Failed to resolve account IDs: {exc}")

    return lookup, unresolved
    
def build_mapping():
    with open(INPUT, newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        ids = set()
        rows = []
        for row in reader:
            if row.get("type") != "file":
                continue
            ids.update(filter(None, [row.get("created_by_id"), row.get("last_modified_by_id")]))
            rows.append(row)

    email_lookup, unresolved_ids = lookup_emails(ids)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as dst:
        writer = csv.DictWriter(dst, fieldnames=["FilePath", "AuthorUPN", "EditorUPN"])
        writer.writeheader()
        for row in rows:
            path = (row.get("path") or "").strip()
            if not path:
                continue
            author = email_lookup.get(row.get("created_by_id"), (row.get("created_by_email") or "").strip()) or FALLBACK
            editor = email_lookup.get(row.get("last_modified_by_id"), (row.get("last_modified_by_email") or "").strip()) or author
            writer.writerow({
                "FilePath": LIBRARY_PREFIX.rstrip("/") + path,
                "AuthorUPN": author,
                "EditorUPN": editor,
            })

    if unresolved_ids:
        with open("unresolved_dbids.txt", "w", encoding="utf-8") as report:
            for missing in sorted(unresolved_ids):
                report.write(missing + "\n")
        print(f"⚠️ Unresolved Dropbox IDs recorded in unresolved_dbids.txt: {len(unresolved_ids)}")
    else:
        print("✅ All Dropbox IDs resolved to emails.")

build_mapping()