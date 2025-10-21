"""SQLite helper utilities for Opt-shft prototype."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Tuple

DB_PATH = Path("opt-shift.db")
DEFAULT_ROTATION_PERIOD = "daily"
VALID_ROTATION_PERIODS = {
    "daily",
    "weekly",
    "biweekly",
    "monthly",
}


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staff (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                title TEXT NOT NULL,
                seniority TEXT,
                min_night_duties_per_month INTEGER,
                max_night_duties_per_month INTEGER
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
                sorumlu_uzman_id INTEGER
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
                required_staff_count INTEGER DEFAULT 1
            )
            """
        )
        _ensure_staff_allows_null_seniority(conn)
        _ensure_clinic_display_order(conn)
        _ensure_duty_type_category(conn)
        _ensure_clinic_rotation_period(conn)
        _ensure_clinic_seniority_rules_table(conn)
        _ensure_assignment_history_table(conn)
        _ensure_leave_requests_table(conn)
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
    _normalize_clinic_display_order(conn)


def _normalize_clinic_display_order(conn: sqlite3.Connection) -> None:
    """Ensure clinic display_order values are sequential."""
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
            UNIQUE (clinic_id, required_seniority),
            FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE CASCADE
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
            FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE
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
                FOREIGN KEY (staff_id) REFERENCES staff(id),
                FOREIGN KEY (clinic_id) REFERENCES clinics(id)
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
                FOREIGN KEY (staff_id) REFERENCES staff(id),
                FOREIGN KEY (clinic_id) REFERENCES clinics(id)
            )
            """
        )
        day_type_select = "day_type" if "day_type" in column_names else "'weekday'"
        conn.execute(
            f"""
            INSERT INTO assignment_history__migrate (id, staff_id, clinic_id, assignment_date, plan_month_year, day_type)
            SELECT id, staff_id, clinic_id, assignment_date, plan_month_year, {day_type_select}
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


def list_staff() -> Iterable[Mapping[str, Optional[str]]]:
    """Return all staff rows ordered by id."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, title, seniority, min_night_duties_per_month, max_night_duties_per_month FROM staff ORDER BY id ASC"
        ).fetchall()
    return rows


def add_staff(
    name: str,
    title: str,
    seniority: Optional[str],
    *,
    min_night: Optional[int] = None,
    max_night: Optional[int] = None,
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
                max_night_duties_per_month
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                name.strip(),
                title.strip(),
                seniority.strip() if seniority else None,
                min_value,
                max_value,
            ),
        )
        conn.commit()
        return cursor.lastrowid


def delete_staff(staff_id: int) -> None:
    """Remove a staff record by ID."""
    with get_connection() as conn:
        conn.execute("DELETE FROM staff WHERE id = ?", (staff_id,))
        conn.commit()


def get_staff_by_id(staff_id: int) -> Optional[Mapping[str, Optional[str]]]:
    """Fetch a single staff row by primary key."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, name, title, seniority, min_night_duties_per_month, max_night_duties_per_month "
            "FROM staff WHERE id = ?",
            (staff_id,),
        ).fetchone()
    return row


def update_staff_preferences(
    staff_id: int,
    *,
    seniority: Optional[str],
    min_night: Optional[int],
    max_night: Optional[int],
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
            WHERE id = ?
            """,
            (normalized_seniority, min_value, max_value, staff_id),
        )
        conn.commit()


def list_clinics() -> Iterable[Mapping[str, Optional[str]]]:
    """Return all clinics."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, display_order, required_assistants, rotation_period, sorumlu_uzman_id FROM clinics "
            "ORDER BY COALESCE(display_order, id) ASC, id ASC"
        ).fetchall()
    return rows


def add_clinic(
    name: str,
    required_assistants: Optional[int] = None,
    sorumlu_uzman_id: Optional[int] = None,
    rotation_period: Optional[str] = None,
) -> int:
    """Insert a clinic and return new ID."""
    with get_connection() as conn:
        max_order_row = conn.execute("SELECT MAX(display_order) FROM clinics").fetchone()
        max_order = max_order_row[0] if max_order_row else None
        next_order = (int(max_order) + 1) if max_order is not None else 1
        assistants = required_assistants if required_assistants and required_assistants > 0 else 1
        rotation = _normalize_rotation_period(rotation_period)
        cursor = conn.execute(
            "INSERT INTO clinics (name, display_order, required_assistants, rotation_period, sorumlu_uzman_id) VALUES (?, ?, ?, ?, ?)",
            (name.strip(), next_order, assistants, rotation, sorumlu_uzman_id),
        )
        conn.commit()
        return cursor.lastrowid


def update_clinic_required_assistants(
    clinic_id: int,
    required_assistants: int,
    sorumlu_uzman_id: Optional[int] = None,
    rotation_period: Optional[str] = None,
) -> None:
    """Update clinic staffing requirements and responsible specialist."""
    required = required_assistants if required_assistants > 0 else 1
    rotation = _normalize_rotation_period(rotation_period)
    with get_connection() as conn:
        conn.execute(
            "UPDATE clinics SET required_assistants = ?, sorumlu_uzman_id = ?, rotation_period = ? WHERE id = ?",
            (required, sorumlu_uzman_id, rotation, clinic_id),
        )
        conn.commit()


def delete_clinic(clinic_id: int) -> None:
    """Delete a clinic and renormalize ordering."""
    with get_connection() as conn:
        conn.execute("DELETE FROM clinics WHERE id = ?", (clinic_id,))
        _normalize_clinic_display_order(conn)
        conn.commit()


def reorder_clinic(clinic_id: int, offset: int) -> bool:
    """Move a clinic up or down in the display order."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id FROM clinics ORDER BY COALESCE(display_order, id) ASC, id ASC"
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
                "UPDATE clinics SET display_order = ? WHERE id = ?",
                (order, cid),
            )
        conn.commit()
        return True


def list_clinic_seniority_rules(clinic_id: Optional[int] = None) -> Iterable[Mapping[str, Optional[str]]]:
    """Fetch clinic seniority rules; optionally limit to a single clinic."""
    query = (
        "SELECT id, clinic_id, required_seniority, required_count "
        "FROM clinic_seniority_rules "
    )
    params: tuple = ()
    if clinic_id is not None:
        query += "WHERE clinic_id = ? "
        params = (clinic_id,)
    query += "ORDER BY clinic_id ASC, id ASC"
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return rows


def add_clinic_seniority_rule(clinic_id: int, required_seniority: str, count: int) -> int:
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
        cursor = conn.execute(
            """
            INSERT INTO clinic_seniority_rules (clinic_id, required_seniority, required_count)
            VALUES (?, ?, ?)
            ON CONFLICT(clinic_id, required_seniority) DO UPDATE SET required_count=excluded.required_count
            """,
            (clinic_id, seniority, normalized_count),
        )
        conn.commit()
        return cursor.lastrowid


def delete_clinic_seniority_rule(rule_id: int) -> None:
    """Delete a seniority rule row."""
    with get_connection() as conn:
        conn.execute("DELETE FROM clinic_seniority_rules WHERE id = ?", (rule_id,))
        conn.commit()


def list_leave_requests() -> Iterable[Mapping[str, Optional[str]]]:
    """Return all leave requests ordered by start date."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, staff_id, start_date, end_date, reason
            FROM leave_requests
            ORDER BY start_date ASC, end_date ASC, id ASC
            """
        ).fetchall()
    return rows


def add_leave_request(
    staff_id: int,
    start_date: str,
    end_date: str,
    reason: Optional[str] = None,
) -> int:
    """Insert a leave request and return its new ID."""
    normalized_reason = reason.strip() if reason and reason.strip() else None
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO leave_requests (staff_id, start_date, end_date, reason)
            VALUES (?, ?, ?, ?)
            """,
            (staff_id, start_date, end_date, normalized_reason),
        )
        conn.commit()
        return cursor.lastrowid


def delete_leave_request(request_id: int) -> None:
    """Remove a leave request."""
    with get_connection() as conn:
        conn.execute("DELETE FROM leave_requests WHERE id = ?", (request_id,))
        conn.commit()


def list_assignment_history(plan_month_year: Optional[str] = None) -> Iterable[Mapping[str, Optional[str]]]:
    """Return assignment history rows, optionally filtered by plan period."""
    query = (
        "SELECT id, staff_id, clinic_id, assignment_date, plan_month_year, day_type "
        "FROM assignment_history "
    )
    params: tuple = ()
    if plan_month_year:
        query += "WHERE plan_month_year = ? "
        params = (plan_month_year,)
    query += "ORDER BY assignment_date ASC, clinic_id ASC, staff_id ASC, id ASC"
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return rows


def replace_assignment_history(plan_month_year: str, entries: Iterable[Tuple[int, Optional[int], str, Optional[str]]]) -> None:
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
            "DELETE FROM assignment_history WHERE plan_month_year = ?",
            (normalized_period,),
        )
        if formatted_entries:
            conn.executemany(
                """
                INSERT INTO assignment_history (staff_id, clinic_id, assignment_date, plan_month_year, day_type)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (staff_id, clinic_id, assignment_date, normalized_period, day_type)
                    for staff_id, clinic_id, assignment_date, day_type in formatted_entries
                ],
            )
        conn.commit()


def list_duty_types() -> Iterable[Mapping[str, Optional[str]]]:
    """Return all duty types."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, duration_hours, duty_category, required_staff_count "
            "FROM duty_types ORDER BY id ASC"
        ).fetchall()
    return rows


def add_duty_type(
    name: str,
    duration_hours: int,
    duty_category: str = "nobet",
    required_staff_count: Optional[int] = None,
) -> int:
    """Insert a duty type."""
    normalized_category = (duty_category or "nobet").strip().lower()
    if normalized_category not in {"mesa", "nobet"}:
        normalized_category = "nobet"
    required = required_staff_count if required_staff_count and required_staff_count > 0 else 1
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO duty_types (name, duration_hours, duty_category, required_staff_count) VALUES (?, ?, ?, ?)",
            (name.strip(), duration_hours, normalized_category, required),
        )
        conn.commit()
        return cursor.lastrowid

