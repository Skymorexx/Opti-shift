"""SQLite helper utilities for Opt-shft prototype."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from werkzeug.security import generate_password_hash

DB_PATH = Path("opt-shift.db")
DEFAULT_ROTATION_PERIOD = "daily"
VALID_ROTATION_PERIODS = {
    "daily",
    "weekly",
    "biweekly",
    "monthly",
}

DEFAULT_UNIT_NAME = "Varsayilan Unitesi"
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "admin123"


def _ensure_units_table(conn: sqlite3.Connection) -> None:
    """Create units table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """
    )


def _ensure_unit_accounts_table(conn: sqlite3.Connection) -> None:
    """Create tenant account table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS unit_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            unit_id INTEGER NOT NULL,
            FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_unit_accounts_unit_id ON unit_accounts(unit_id)"
    )


def _ensure_default_unit(conn: sqlite3.Connection) -> int:
    """Ensure at least one unit exists and return its id."""
    row = conn.execute(
        "SELECT id FROM units ORDER BY id ASC LIMIT 1"
    ).fetchone()
    if row:
        return int(row["id"])
    cursor = conn.execute(
        "INSERT INTO units (name) VALUES (?)",
        (DEFAULT_UNIT_NAME,),
    )
    return int(cursor.lastrowid)


def _ensure_default_admin(conn: sqlite3.Connection, unit_id: int) -> None:
    """Provision a bootstrap admin account if none exist."""
    row = conn.execute("SELECT COUNT(1) AS count FROM unit_accounts").fetchone()
    if row and int(row["count"]) > 0:
        return
    password_hash = generate_password_hash(DEFAULT_ADMIN_PASSWORD)
    conn.execute(
        """
        INSERT INTO unit_accounts (username, password_hash, unit_id)
        VALUES (?, ?, ?)
        """,
        (DEFAULT_ADMIN_USERNAME, password_hash, unit_id),
    )


def _ensure_table_has_unit_column(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    index_name: Optional[str] = None,
    default_unit_id: Optional[int] = None,
) -> None:
    """Add a unit_id column when missing and backfill existing rows."""
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    column_names = {col[1] for col in columns}
    if "unit_id" not in column_names:
        conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN unit_id INTEGER REFERENCES units(id)"
        )
    if default_unit_id is not None:
        conn.execute(
            f"UPDATE {table_name} SET unit_id = ? WHERE unit_id IS NULL",
            (default_unit_id,),
        )
    if index_name:
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}(unit_id)"
        )

def _normalize_rotation_period(value: Optional[str]) -> str:
    """Normalize rotation period strings to a limited allow-list."""
    if not value:
        return DEFAULT_ROTATION_PERIOD
    candidate = value.strip().lower()
    if candidate in VALID_ROTATION_PERIODS:
        return candidate
    return DEFAULT_ROTATION_PERIOD


def get_connection() -> sqlite3.Connection:
    """Create a new sqlite3 connection with sensible defaults."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    """Ensure required tables exist."""
    with get_connection() as conn:
        _ensure_units_table(conn)
        _ensure_unit_accounts_table(conn)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staff (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                title TEXT NOT NULL,
                seniority TEXT,
                min_night_duties_per_month INTEGER,
                max_night_duties_per_month INTEGER,
                unit_id INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clinics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                display_order INTEGER,
                required_assistants INTEGER DEFAULT 1,
                rotation_period TEXT DEFAULT 'daily',
                sorumlu_uzman_id INTEGER,
                unit_id INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS duty_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                duration_hours INTEGER NOT NULL CHECK(duration_hours > 0),
                duty_category TEXT,
                required_staff_count INTEGER DEFAULT 1,
                unit_id INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
            )
            """
        )
        _ensure_staff_allows_null_seniority(conn)
        _ensure_duty_type_category(conn)
        _ensure_clinic_rotation_period(conn)
        _ensure_clinic_seniority_rules_table(conn)
        _ensure_assignment_history_table(conn)
        _ensure_leave_requests_table(conn)

        default_unit_id = _ensure_default_unit(conn)

        _ensure_table_has_unit_column(
            conn,
            "staff",
            index_name="idx_staff_unit_id",
            default_unit_id=default_unit_id,
        )
        _ensure_table_has_unit_column(
            conn,
            "clinics",
            index_name="idx_clinics_unit_id",
            default_unit_id=default_unit_id,
        )
        _ensure_table_has_unit_column(
            conn,
            "duty_types",
            index_name="idx_duty_types_unit_id",
            default_unit_id=default_unit_id,
        )
        _ensure_table_has_unit_column(
            conn,
            "clinic_seniority_rules",
            index_name="idx_clinic_rules_unit_id",
            default_unit_id=default_unit_id,
        )
        _ensure_table_has_unit_column(
            conn,
            "assignment_history",
            index_name="idx_assignment_history_unit_id",
            default_unit_id=default_unit_id,
        )
        _ensure_table_has_unit_column(
            conn,
            "leave_requests",
            index_name="idx_leave_requests_unit_id",
            default_unit_id=default_unit_id,
        )

        _normalize_clinic_display_order(conn, unit_id=None)
        _ensure_default_admin(conn, default_unit_id)
        conn.commit()




def _ensure_staff_allows_null_seniority(conn: sqlite3.Connection) -> None:
    """Ensure seniority column allows NULL and staff table has night duty limit columns."""
    columns = conn.execute("PRAGMA table_info(staff)").fetchall()
    column_names = {col[1] for col in columns}
    seniority_info = next((col for col in columns if col[1] == "seniority"), None)
    needs_rebuild = bool(seniority_info and seniority_info[3]) or "min_night_duties_per_month" not in column_names or "max_night_duties_per_month" not in column_names
    if not needs_rebuild:
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS staff__migrate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            title TEXT NOT NULL,
            seniority TEXT,
            min_night_duties_per_month INTEGER,
            max_night_duties_per_month INTEGER
        )
        """
    )

    select_min = "min_night_duties_per_month" if "min_night_duties_per_month" in column_names else "NULL"
    select_max = "max_night_duties_per_month" if "max_night_duties_per_month" in column_names else "NULL"

    conn.execute(
        f"""
        INSERT INTO staff__migrate (
            id, name, title, seniority, min_night_duties_per_month, max_night_duties_per_month
        )
        SELECT
            id,
            name,
            title,
            seniority,
            {select_min},
            {select_max}
        FROM staff
        """
    )
    conn.execute("DROP TABLE staff")
    conn.execute("ALTER TABLE staff__migrate RENAME TO staff")


def _ensure_clinic_display_order(conn: sqlite3.Connection) -> None:
    """Add clinic metadata columns if missing and normalize existing rows."""
    columns = conn.execute("PRAGMA table_info(clinics)").fetchall()
    column_names = {col["name"] for col in columns}
    if "display_order" not in column_names:
        conn.execute("ALTER TABLE clinics ADD COLUMN display_order INTEGER")
    if "required_assistants" not in column_names:
        conn.execute("ALTER TABLE clinics ADD COLUMN required_assistants INTEGER")
    if "sorumlu_uzman_id" not in column_names:
        conn.execute("ALTER TABLE clinics ADD COLUMN sorumlu_uzman_id INTEGER")
    conn.execute(
        "UPDATE clinics "
        "SET required_assistants = CASE WHEN required_assistants IS NULL OR required_assistants < 1 THEN 1 ELSE required_assistants END"
    )
    _normalize_clinic_display_order(conn, unit_id=None)


def _normalize_clinic_display_order(
    conn: sqlite3.Connection,
    unit_id: Optional[int],
) -> None:
    """Ensure clinic display_order values are sequential per unit."""
    columns = conn.execute("PRAGMA table_info(clinics)").fetchall()
    column_names = {col["name"] for col in columns}
    has_unit_column = "unit_id" in column_names

    if has_unit_column and unit_id is None:
        unit_rows = conn.execute(
            "SELECT DISTINCT unit_id FROM clinics WHERE unit_id IS NOT NULL"
        ).fetchall()
        for row in unit_rows:
            _normalize_clinic_display_order(conn, int(row["unit_id"]))
        return

    if has_unit_column and unit_id is not None:
        rows = conn.execute(
            """
            SELECT id FROM clinics
            WHERE unit_id = ?
            ORDER BY COALESCE(display_order, id) ASC, id ASC
            """,
            (unit_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id FROM clinics ORDER BY COALESCE(display_order, id) ASC, id ASC"
        ).fetchall()

    for index, row in enumerate(rows, start=1):
        conn.execute(
            "UPDATE clinics SET display_order = ? WHERE id = ?",
            (index, row["id"]),
        )


def _ensure_clinic_rotation_period(conn: sqlite3.Connection) -> None:
    """Ensure clinics expose rotation period metadata with sane defaults."""
    columns = conn.execute("PRAGMA table_info(clinics)").fetchall()
    column_names = {col["name"] for col in columns}
    if "rotation_period" not in column_names:
        conn.execute("ALTER TABLE clinics ADD COLUMN rotation_period TEXT")
    conn.execute(
        "UPDATE clinics "
        "SET rotation_period = :default_value "
        "WHERE rotation_period IS NULL OR TRIM(rotation_period) = ''",
        {"default_value": DEFAULT_ROTATION_PERIOD},
    )
    if "sorumlu_uzman_id" not in column_names:
        conn.execute("ALTER TABLE clinics ADD COLUMN sorumlu_uzman_id INTEGER")


def _ensure_clinic_seniority_rules_table(conn: sqlite3.Connection) -> None:
    """Create clinic seniority rules table if necessary."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clinic_seniority_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            clinic_id INTEGER NOT NULL,
            required_seniority TEXT NOT NULL,
            required_count INTEGER NOT NULL CHECK(required_count >= 0),
            unit_id INTEGER NOT NULL DEFAULT 1,
            UNIQUE (clinic_id, required_seniority),
            FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE CASCADE,
            FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
        )
        """
    )


def _ensure_leave_requests_table(conn: sqlite3.Connection) -> None:
    """Ensure leave requests table exists for tracking staff absences."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leave_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_id INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            reason TEXT,
            unit_id INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE,
            FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
        )
        """
    )


def _ensure_assignment_history_table(conn: sqlite3.Connection) -> None:
    """Create assignment history table to persist monthly schedules."""
    columns = conn.execute("PRAGMA table_info(assignment_history)").fetchall()
    if not columns:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assignment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                staff_id INTEGER NOT NULL,
                clinic_id INTEGER,
                assignment_date TEXT NOT NULL,
                plan_month_year TEXT NOT NULL,
                day_type TEXT,
                unit_id INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (staff_id) REFERENCES staff(id),
                FOREIGN KEY (clinic_id) REFERENCES clinics(id),
                FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
            )
            """
        )
        return

    column_names = {col["name"] for col in columns}
    clinic_info = next((col for col in columns if col["name"] == "clinic_id"), None)
    needs_rebuild = False
    if clinic_info and clinic_info["notnull"]:
        needs_rebuild = True
    if "day_type" not in column_names:
        needs_rebuild = True

    if needs_rebuild:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assignment_history__migrate (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                staff_id INTEGER NOT NULL,
                clinic_id INTEGER,
                assignment_date TEXT NOT NULL,
                plan_month_year TEXT NOT NULL,
                day_type TEXT,
                unit_id INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (staff_id) REFERENCES staff(id),
                FOREIGN KEY (clinic_id) REFERENCES clinics(id),
                FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
            )
            """
        )
        day_type_select = "day_type" if "day_type" in column_names else "'weekday'"
        unit_select = "unit_id" if "unit_id" in column_names else str(_ensure_default_unit(conn))
        conn.execute(
            f"""
            INSERT INTO assignment_history__migrate (id, staff_id, clinic_id, assignment_date, plan_month_year, day_type, unit_id)
            SELECT id, staff_id, clinic_id, assignment_date, plan_month_year, {day_type_select}, {unit_select}
            FROM assignment_history
            """
        )
        conn.execute("DROP TABLE assignment_history")
        conn.execute("ALTER TABLE assignment_history__migrate RENAME TO assignment_history")
    elif "day_type" not in column_names:
        conn.execute("ALTER TABLE assignment_history ADD COLUMN day_type TEXT")


def _ensure_duty_type_category(conn: sqlite3.Connection) -> None:
    """Ensure duty_types have a category column with sensible defaults."""
    columns = conn.execute("PRAGMA table_info(duty_types)").fetchall()
    column_names = {col["name"] for col in columns}
    if "duty_category" not in column_names:
        conn.execute("ALTER TABLE duty_types ADD COLUMN duty_category TEXT")
    if "required_staff_count" not in column_names:
        conn.execute("ALTER TABLE duty_types ADD COLUMN required_staff_count INTEGER")
    conn.execute(
        "UPDATE duty_types "
        "SET duty_category = COALESCE(NULLIF(TRIM(duty_category), ''), 'nobet')"
    )
    conn.execute(
        "UPDATE duty_types "
        "SET required_staff_count = CASE "
        "WHEN required_staff_count IS NULL OR required_staff_count < 1 THEN 1 "
        "ELSE required_staff_count "
        "END"
    )


def create_unit(name: str) -> int:
    """Create a new medical unit and return its ID."""
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO units (name) VALUES (?)",
            (name.strip(),),
        )
        conn.commit()
        return int(cursor.lastrowid)


def list_units() -> Iterable[Mapping[str, Any]]:
    """Return all registered units."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name FROM units ORDER BY name ASC"
        ).fetchall()
    return rows


def get_unit_by_id(unit_id: int) -> Optional[Mapping[str, Any]]:
    """Fetch a single unit by ID."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, name FROM units WHERE id = ?",
            (unit_id,),
        ).fetchone()
    return row


def create_unit_account(username: str, password_hash: str, unit_id: int) -> int:
    """Create a login account for a unit."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO unit_accounts (username, password_hash, unit_id)
            VALUES (?, ?, ?)
            """,
            (username.strip().lower(), password_hash, unit_id),
        )
        conn.commit()
        return int(cursor.lastrowid)


def get_account_by_username(username: str) -> Optional[Mapping[str, Any]]:
    """Fetch an account row by username."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT unit_accounts.id, unit_accounts.username, unit_accounts.password_hash, unit_accounts.unit_id, units.name AS unit_name
            FROM unit_accounts
            JOIN units ON units.id = unit_accounts.unit_id
            WHERE unit_accounts.username = ?
            """,
            (username.strip().lower(),),
        ).fetchone()
    return row


def list_staff(unit_id: int) -> Iterable[Mapping[str, Optional[str]]]:
    """Return all staff rows ordered by id."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, title, seniority, min_night_duties_per_month, max_night_duties_per_month
            FROM staff
            WHERE unit_id = ?
            ORDER BY id ASC
            """,
            (unit_id,),
        ).fetchall()
    return rows


def add_staff(
    name: str,
    title: str,
    seniority: Optional[str],
    *,
    min_night: Optional[int] = None,
    max_night: Optional[int] = None,
    unit_id: int,
) -> int:
    """Insert a staff record and return the new row ID."""
    min_value = min_night if min_night is not None and min_night >= 0 else None
    max_value = max_night if max_night is not None and max_night >= 0 else None
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO staff (
                name,
                title,
                seniority,
                min_night_duties_per_month,
                max_night_duties_per_month,
                unit_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                name.strip(),
                title.strip(),
                seniority.strip() if seniority else None,
                min_value,
                max_value,
                unit_id,
            ),
        )
        conn.commit()
        return cursor.lastrowid


def delete_staff(staff_id: int, unit_id: int) -> None:
    """Remove a staff record by ID."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM staff WHERE id = ? AND unit_id = ?",
            (staff_id, unit_id),
        )
        conn.commit()


def get_staff_by_id(staff_id: int, unit_id: int) -> Optional[Mapping[str, Optional[str]]]:
    """Fetch a single staff row by primary key."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, name, title, seniority, min_night_duties_per_month, max_night_duties_per_month
            FROM staff
            WHERE id = ? AND unit_id = ?
            """,
            (staff_id, unit_id),
        ).fetchone()
    return row


def update_staff_preferences(
    staff_id: int,
    *,
    seniority: Optional[str],
    min_night: Optional[int],
    max_night: Optional[int],
    unit_id: int,
) -> None:
    """Update an assistant doctor's seniority and night duty preferences."""
    normalized_seniority: Optional[str] = None
    if seniority:
        candidate = seniority.strip().lower()
        if candidate in {"kidemli", "ara", "comez"}:
            normalized_seniority = candidate

    min_value = min_night if (min_night is not None and min_night >= 0) else None
    max_value = max_night if (max_night is not None and max_night >= 0) else None

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE staff
            SET seniority = ?, min_night_duties_per_month = ?, max_night_duties_per_month = ?
            WHERE id = ? AND unit_id = ?
            """,
            (normalized_seniority, min_value, max_value, staff_id, unit_id),
        )
        conn.commit()


def list_clinics(unit_id: int) -> Iterable[Mapping[str, Optional[str]]]:
    """Return all clinics."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, display_order, required_assistants, rotation_period, sorumlu_uzman_id
            FROM clinics
            WHERE unit_id = ?
            ORDER BY COALESCE(display_order, id) ASC, id ASC
            """,
            (unit_id,),
        ).fetchall()
    return rows


def add_clinic(
    name: str,
    required_assistants: Optional[int] = None,
    sorumlu_uzman_id: Optional[int] = None,
    rotation_period: Optional[str] = None,
    *,
    unit_id: int,
) -> int:
    """Insert a clinic and return new ID."""
    with get_connection() as conn:
        max_order_row = conn.execute(
            "SELECT MAX(display_order) FROM clinics WHERE unit_id = ?",
            (unit_id,),
        ).fetchone()
        max_order = max_order_row[0] if max_order_row else None
        next_order = (int(max_order) + 1) if max_order is not None else 1
        assistants = required_assistants if required_assistants and required_assistants > 0 else 1
        rotation = _normalize_rotation_period(rotation_period)
        cursor = conn.execute(
            """
            INSERT INTO clinics (name, display_order, required_assistants, rotation_period, sorumlu_uzman_id, unit_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name.strip(), next_order, assistants, rotation, sorumlu_uzman_id, unit_id),
        )
        conn.commit()
        return cursor.lastrowid


def update_clinic_required_assistants(
    clinic_id: int,
    required_assistants: int,
    sorumlu_uzman_id: Optional[int] = None,
    rotation_period: Optional[str] = None,
    *,
    unit_id: int,
) -> None:
    """Update clinic staffing requirements and responsible specialist."""
    required = required_assistants if required_assistants > 0 else 1
    rotation = _normalize_rotation_period(rotation_period)
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE clinics
            SET required_assistants = ?, sorumlu_uzman_id = ?, rotation_period = ?
            WHERE id = ? AND unit_id = ?
            """,
            (required, sorumlu_uzman_id, rotation, clinic_id, unit_id),
        )
        conn.commit()


def delete_clinic(clinic_id: int, unit_id: int) -> None:
    """Delete a clinic and renormalize ordering."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM clinics WHERE id = ? AND unit_id = ?",
            (clinic_id, unit_id),
        )
        _normalize_clinic_display_order(conn, unit_id=unit_id)
        conn.commit()


def reorder_clinic(clinic_id: int, offset: int, *, unit_id: int) -> bool:
    """Move a clinic up or down in the display order."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id FROM clinics
            WHERE unit_id = ?
            ORDER BY COALESCE(display_order, id) ASC, id ASC
            """,
            (unit_id,),
        ).fetchall()
        ids = [row["id"] for row in rows]
        try:
            index = ids.index(clinic_id)
        except ValueError:
            return False

        new_index = index + offset
        if new_index < 0 or new_index >= len(ids):
            return False

        ids[index], ids[new_index] = ids[new_index], ids[index]
        for order, cid in enumerate(ids, start=1):
            conn.execute(
                "UPDATE clinics SET display_order = ? WHERE id = ? AND unit_id = ?",
                (order, cid, unit_id),
            )
        conn.commit()
        return True


def list_clinic_seniority_rules(
    unit_id: int,
    clinic_id: Optional[int] = None,
) -> Iterable[Mapping[str, Optional[str]]]:
    """Fetch clinic seniority rules; optionally limit to a single clinic."""
    query = (
        "SELECT id, clinic_id, required_seniority, required_count "
        "FROM clinic_seniority_rules "
        "WHERE unit_id = ? "
    )
    params: List[Any] = [unit_id]
    if clinic_id is not None:
        query += "AND clinic_id = ? "
        params.append(clinic_id)
    query += "ORDER BY clinic_id ASC, id ASC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return rows


def add_clinic_seniority_rule(
    clinic_id: int,
    required_seniority: str,
    count: int,
    *,
    unit_id: int,
) -> int:
    """Insert a seniority rule for a clinic."""
    seniority = (required_seniority or "").strip().lower()
    if seniority not in {"comez", "ara", "kidemli"}:
        raise ValueError("Gecersiz kidem seviyesi.")
    try:
        normalized_count = int(count)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive parsing
        raise ValueError("Gecersiz adet degeri.") from exc
    normalized_count = max(0, normalized_count)
    with get_connection() as conn:
        clinic_row = conn.execute(
            "SELECT 1 FROM clinics WHERE id = ? AND unit_id = ?",
            (clinic_id, unit_id),
        ).fetchone()
        if not clinic_row:
            raise ValueError("Klinik bu tenant icin bulunamadi.")
        cursor = conn.execute(
            """
            INSERT INTO clinic_seniority_rules (clinic_id, required_seniority, required_count, unit_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(clinic_id, required_seniority) DO UPDATE SET required_count=excluded.required_count
            """,
            (clinic_id, seniority, normalized_count, unit_id),
        )
        conn.commit()
        return cursor.lastrowid


def delete_clinic_seniority_rule(rule_id: int, unit_id: int) -> None:
    """Delete a seniority rule row."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM clinic_seniority_rules WHERE id = ? AND unit_id = ?",
            (rule_id, unit_id),
        )
        conn.commit()


def list_leave_requests(unit_id: int) -> Iterable[Mapping[str, Optional[str]]]:
    """Return all leave requests ordered by start date."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, staff_id, start_date, end_date, reason
            FROM leave_requests
            WHERE unit_id = ?
            ORDER BY start_date ASC, end_date ASC, id ASC
            """,
            (unit_id,),
        ).fetchall()
    return rows


def add_leave_request(
    staff_id: int,
    start_date: str,
    end_date: str,
    reason: Optional[str] = None,
    *,
    unit_id: int,
) -> int:
    """Insert a leave request and return its new ID."""
    normalized_reason = reason.strip() if reason and reason.strip() else None
    with get_connection() as conn:
        staff_row = conn.execute(
            "SELECT 1 FROM staff WHERE id = ? AND unit_id = ?",
            (staff_id, unit_id),
        ).fetchone()
        if not staff_row:
            raise ValueError("Personel bu tenant icin bulunamadi.")
        cursor = conn.execute(
            """
            INSERT INTO leave_requests (staff_id, start_date, end_date, reason, unit_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (staff_id, start_date, end_date, normalized_reason, unit_id),
        )
        conn.commit()
        return cursor.lastrowid


def delete_leave_request(request_id: int, unit_id: int) -> None:
    """Remove a leave request."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM leave_requests WHERE id = ? AND unit_id = ?",
            (request_id, unit_id),
        )
        conn.commit()


def list_assignment_history(
    unit_id: int,
    plan_month_year: Optional[str] = None,
) -> Iterable[Mapping[str, Optional[str]]]:
    """Return assignment history rows, optionally filtered by plan period."""
    query = (
        "SELECT id, staff_id, clinic_id, assignment_date, plan_month_year, day_type "
        "FROM assignment_history "
        "WHERE unit_id = ? "
    )
    params: List[Any] = [unit_id]
    if plan_month_year:
        query += "AND plan_month_year = ? "
        params.append(plan_month_year)
    query += "ORDER BY assignment_date ASC, clinic_id ASC, staff_id ASC, id ASC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return rows


def replace_assignment_history(
    unit_id: int,
    plan_month_year: str,
    entries: Iterable[Tuple[int, Optional[int], str, Optional[str]]],
) -> None:
    """Replace assignment history for a given plan period with provided entries."""
    normalized_period = plan_month_year.strip()
    formatted_entries: List[Tuple[int, Optional[int], str, str]] = []
    for entry in entries:
        if len(entry) == 4:
            staff_id, clinic_id, assignment_date, day_type = entry
        elif len(entry) == 3:
            staff_id, clinic_id, assignment_date = entry
            day_type = "weekday"
        else:
            continue
        normalized_day_type = (day_type or "").strip().lower()
        if normalized_day_type not in {"weekday", "weekend"}:
            normalized_day_type = "weekday"
        formatted_entries.append(
            (int(staff_id), clinic_id, assignment_date, normalized_day_type)
        )

    with get_connection() as conn:
        conn.execute(
            "DELETE FROM assignment_history WHERE plan_month_year = ? AND unit_id = ?",
            (normalized_period, unit_id),
        )
        if formatted_entries:
            conn.executemany(
                """
                INSERT INTO assignment_history (staff_id, clinic_id, assignment_date, plan_month_year, day_type, unit_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        staff_id,
                        clinic_id,
                        assignment_date,
                        normalized_period,
                        day_type,
                        unit_id,
                    )
                    for staff_id, clinic_id, assignment_date, day_type in formatted_entries
                ],
            )
        conn.commit()


def list_duty_types(unit_id: int) -> Iterable[Mapping[str, Optional[str]]]:
    """Return all duty types."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, duration_hours, duty_category, required_staff_count
            FROM duty_types
            WHERE unit_id = ?
            ORDER BY id ASC
            """,
            (unit_id,),
        ).fetchall()
    return rows


def add_duty_type(
    name: str,
    duration_hours: int,
    duty_category: str = "nobet",
    required_staff_count: Optional[int] = None,
    *,
    unit_id: int,
) -> int:
    """Insert a duty type."""
    normalized_category = (duty_category or "nobet").strip().lower()
    if normalized_category not in {"mesa", "nobet"}:
        normalized_category = "nobet"
    required = required_staff_count if required_staff_count and required_staff_count > 0 else 1
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO duty_types (name, duration_hours, duty_category, required_staff_count, unit_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name.strip(), duration_hours, normalized_category, required, unit_id),
        )
        conn.commit()
        return cursor.lastrowid

