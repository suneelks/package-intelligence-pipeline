"""Simple forward-only SQL migration runner.

Each file in src/nuget_pipeline/db/migrations/ named NNN_*.sql is applied
in lexicographic order. A schema_migrations table records applied versions
to guarantee idempotency.
"""

import asyncio
import hashlib
import re
from pathlib import Path

from psycopg import AsyncConnection

from nuget_pipeline.db.connection import close_pool, connection
from nuget_pipeline.utils.logging import configure_logging, get_logger

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
MIGRATION_RE = re.compile(r"^(\d+)_.+\.sql$")

log = get_logger(__name__)


async def ensure_migrations_table(conn: AsyncConnection) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    await conn.commit()


async def applied_versions(conn: AsyncConnection) -> set[str]:
    async with conn.cursor() as cur:
        await cur.execute("SELECT version FROM schema_migrations")
        rows = await cur.fetchall()
    return {row["version"] for row in rows}


def discover_migrations() -> list[tuple[str, Path]]:
    if not MIGRATIONS_DIR.exists():
        return []
    out: list[tuple[str, Path]] = []
    for path in sorted(MIGRATIONS_DIR.iterdir()):
        m = MIGRATION_RE.match(path.name)
        if m:
            out.append((m.group(1), path))
    return out


async def apply_migration(conn: AsyncConnection, version: str, path: Path) -> None:
    sql = path.read_text()
    checksum = hashlib.sha256(sql.encode()).hexdigest()
    log.info("migration.apply", version=version, filename=path.name)
    async with conn.cursor() as cur:
        await cur.execute(sql)
        await cur.execute(
            "INSERT INTO schema_migrations (version, filename, checksum) VALUES (%s, %s, %s)",
            (version, path.name, checksum),
        )
    await conn.commit()


async def run() -> None:
    configure_logging()
    try:
        async with connection() as conn:
            await ensure_migrations_table(conn)
            applied = await applied_versions(conn)
            pending = [(v, p) for v, p in discover_migrations() if v not in applied]

            if not pending:
                log.info("migration.up_to_date", applied=len(applied))
                return

            for version, path in pending:
                await apply_migration(conn, version, path)

            log.info("migration.done", applied=len(applied) + len(pending))
    finally:
        await close_pool()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
