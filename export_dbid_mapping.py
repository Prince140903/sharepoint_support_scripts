import csv

SOURCE_CSV = "dropbox_metadata_afya.csv"
OUTPUT_CSV = "dbid_mapping.csv"


def main():
    unique = {}

    with open(SOURCE_CSV, newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        for row in reader:
            path = (row.get("path") or "").strip()
            if not path or row.get("type") != "file":
                continue

            for source_col, label in (("created_by_id", "created_by"), ("last_modified_by_id", "last_modified_by")):
                dbid = (row.get(source_col) or "").strip()
                if not dbid or not dbid.startswith("dbid:"):
                    continue
                if dbid not in unique:
                    unique[dbid] = {
                        "dbid": dbid,
                        "sample_path": path,
                        "source": label,
                    }

    records = sorted(unique.values(), key=lambda x: x["dbid"])

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as dst:
        writer = csv.DictWriter(dst, fieldnames=["dbid", "sample_path", "source"])
        writer.writeheader()
        writer.writerows(records)

    print(f"✅ Wrote {len(records)} unique dbids to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

