import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "jobs.db"


NEW_COLUMNS = {
    "job_type": "TEXT NOT NULL DEFAULT 'dummy'",
    "job_config": "TEXT NOT NULL DEFAULT '{}'",
    "checkpoint_path": "TEXT",
    "checkpoint_updated_at": "REAL",
    "checkpoint_size_bytes": "INTEGER",
}


def get_existing_columns(cursor):
    cursor.execute("PRAGMA table_info(jobs)")
    rows = cursor.fetchall()

    return {
        row[1]
        for row in rows
    }


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}"
        )

    print("=" * 60)
    print("PyTorch job database migration")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print()

    connection = sqlite3.connect(DB_PATH)

    try:
        cursor = connection.cursor()

        existing_columns = get_existing_columns(
            cursor
        )

        print("Existing columns:")

        for column in sorted(existing_columns):
            print(f"  - {column}")

        print()

        added = []

        for column_name, definition in NEW_COLUMNS.items():

            if column_name in existing_columns:
                print(
                    f"[SKIP] {column_name} already exists"
                )
                continue

            sql = (
                f"ALTER TABLE jobs "
                f"ADD COLUMN {column_name} {definition}"
            )

            print(
                f"[ADD] {column_name}"
            )

            cursor.execute(sql)

            added.append(column_name)

        connection.commit()

        print()
        print("=" * 60)

        if added:
            print(
                "Migration completed successfully."
            )

            print("Added:")

            for column in added:
                print(f"  - {column}")

        else:
            print(
                "Database already migrated."
            )

        print("=" * 60)

        print()
        print("Final jobs table:")

        cursor.execute(
            "PRAGMA table_info(jobs)"
        )

        for row in cursor.fetchall():

            column_id = row[0]
            name = row[1]
            datatype = row[2]
            not_null = row[3]
            default_value = row[4]

            print(
                f"{column_id:>2}  "
                f"{name:<28} "
                f"{datatype:<10} "
                f"default={default_value} "
                f"not_null={not_null}"
            )

    finally:
        connection.close()


if __name__ == "__main__":
    main()
