import csv

INPUT = r"dropbox_metadata_afya.csv"
OUTPUT = r"mapping.csv"
FALLBACK = "sharepoint.admin@ducorpgroup.com"
LIBRARY_PREFIX = "/sites/Testing-site/Shared Documents"  # update to your library root

with open(INPUT, newline="", encoding="utf-8") as src, \
     open(OUTPUT, "w", newline="", encoding="utf-8") as dst:
    reader = csv.DictReader(src)
    writer = csv.DictWriter(dst, fieldnames=["FilePath", "AuthorUPN", "EditorUPN"])
    writer.writeheader()
    for row in reader:
        path = (row.get("path") or "").strip()
        if not path or row.get("type") != "file":
            continue
        author = (row.get("created_by_email") or "").strip() or FALLBACK
        editor = (row.get("last_modified_by_email") or "").strip() or author
        writer.writerow({
            "FilePath": LIBRARY_PREFIX.rstrip("/") + path,
            "AuthorUPN": author,
            "EditorUPN": editor,
        })