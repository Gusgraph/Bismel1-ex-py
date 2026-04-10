from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.firestore_runtime_store import build_prime_stocks_runtime_bootstrap_documents
from app.shared.config import get_settings


DEFAULT_PROJECT_ID = "servgraph"
DEFAULT_DATABASE_ID = "bismel1-01"


def seed_prime_stocks_runtime_documents(
    *,
    client: Any,
    collection_name: str,
    product_document: str,
    documents: dict[str, dict[str, Any]],
) -> list[str]:
    root = client.collection(collection_name).document(product_document)
    written_paths: list[str] = []
    for relative_path, payload in documents.items():
        collection_name_part, document_name = relative_path.split("/", maxsplit=1)
        root.collection(collection_name_part).document(document_name).set(payload, merge=True)
        written_paths.append(f"{collection_name}/{product_document}/{relative_path}")
    return written_paths


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed the Prime Stocks Firestore runtime/control document set.",
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--database", default=DEFAULT_DATABASE_ID)
    parser.add_argument("--collection", default="runtime_products")
    parser.add_argument("--product-document", default="prime_stocks")
    args = parser.parse_args()

    try:
        from google.cloud import firestore
    except ImportError as exc:
        raise SystemExit("google-cloud-firestore is required to seed Firestore runtime documents.") from exc

    base_settings = get_settings()
    settings = replace(
        base_settings,
        firestore_project_id=args.project,
        firestore_database_id=args.database,
        firestore_runtime_collection=args.collection,
        firestore_product_document=args.product_document,
        prime_stocks_runtime_enabled=True,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_dry_run=False,
        prime_stocks_asset_type="stock",
    )
    documents = build_prime_stocks_runtime_bootstrap_documents(settings)
    client = firestore.Client(project=args.project, database=args.database)
    written_paths = seed_prime_stocks_runtime_documents(
        client=client,
        collection_name=args.collection,
        product_document=args.product_document,
        documents=documents,
    )

    print(f"Seeded Prime Stocks runtime documents into project={args.project} database={args.database}")
    for path in written_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
