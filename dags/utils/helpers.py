import hashlib
from datetime import datetime


def encode_to_md5_hash(string_to_hash: str) -> str:
    return hashlib.md5(string_to_hash.encode()).hexdigest()


def categorize_fact(fact: str) -> str:
    return "with_numbers" if any(char.isdigit() for char in fact) else "without_numbers"


def get_formatted_report(
    total_processed: int, inserted: int, updated: int, deleted: int
) -> str:
    return (
        f"ETL Report for {datetime.now().strftime('%Y-%m-%d')}:\n"
        f"Processed {total_processed} records.\n"
        f"{inserted} records were inserted.\n"
        f"{updated} records were updated.\n"
        f"{deleted} records were marked as deleted.\n"
        f"{'There were updates in the source dataset.' if inserted + updated + deleted > 0 else 'No updates were detected in the source dataset.'}"
    )
