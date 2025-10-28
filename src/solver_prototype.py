"""
Constraint programming prototype for Opt-shft scheduling.

This script demonstrates how Google OR-Tools CP-SAT solver can be used to:
  * Assign personnel to duty slots.
  * Enforce a 48-hour rest period after overnight/on-call duties.
  * Incorporate seniority-based weighting into the objective function.

Run directly with: `python src/solver_prototype.py`
"""

from __future__ import annotations

import calendar
import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from ortools.sat.python import cp_model

try:
    import holidays

    HOLIDAYS_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    holidays = None  # type: ignore
    HOLIDAYS_AVAILABLE = False

# Seniority settings keep things configurable in one place.
SENIORITY_LEVELS = ("kidemli", "ara", "comez", "uzman")
SENIORITY_WEIGHTS: Dict[str, int] = {
    "kidemli": 1,
    "ara": 2,
    "comez": 3,
    "uzman": 0,
}
SENIORITY_TARGETS: Dict[str, int] = {
    "kidemli": 2,  # Prefer senior staff covering more complex rota slots.
    "ara": 1,
    "comez": 1,
    "uzman": 0,
}

ROTATION_PERIOD_TO_DAYS = {
    "daily": 1,
    "weekly": 7,
    "biweekly": 14,
    "monthly": 0,  # Interpret as "entire month / single block".
}


def normalize_rotation_period(value: Optional[str]) -> str:
    """Normalize rotation period identifiers to a predefined set."""
    if value is None:
        return "daily"
    candidate = value.strip().lower()
    if candidate in ROTATION_PERIOD_TO_DAYS:
        return candidate
    return "daily"


def rotation_period_to_block_size(value: Optional[str]) -> int:
    """Convert a rotation period string into a day span; 0 means full period."""
    normalized = normalize_rotation_period(value)
    return ROTATION_PERIOD_TO_DAYS[normalized]


@dataclass(frozen=True)
class Person:
    """Represents a staff member that can be scheduled."""

    identifier: str
    display_name: str
    seniority: str  # Expected to be one of SENIORITY_LEVELS.
    title: Optional[str] = None
    allowed_duty_types: Tuple[str, ...] = ("*",)
    min_night_duties: Optional[int] = None
    max_night_duties: Optional[int] = None
    education_year: Optional[int] = None
    night_duty_exempt: bool = False

    def weight(self) -> int:
        return SENIORITY_WEIGHTS[self.seniority]

    def preferred_load(self) -> int:
        return SENIORITY_TARGETS[self.seniority]


@dataclass(frozen=True)
class DutySlot:
    """Represents a schedulable slot (clinic shift or on-call duty)."""

    identifier: str
    duty_type: str  # e.g. "clinic", "night", "full"
    start: dt.datetime
    duration_hours: int
    label: Optional[str] = None

    @property
    def end(self) -> dt.datetime:
        return self.start + dt.timedelta(hours=self.duration_hours)

    @property
    def requires_extended_rest(self) -> bool:
        """Only long duties demand a 48-hour rest period afterwards."""
        return self.duration_hours >= 16


class SchedulingPrototype:
    """Builds and solves the constraint programming model."""

    def __init__(
        self,
        people: Iterable[Person],
        slots: Iterable[DutySlot],
        rest_buffer_hours: int = 48,
        enforce_person_limits: bool = False,
        clinic_rotation_days: Optional[Mapping[int, int]] = None,
        clinic_seniority_rules: Optional[Mapping[int, Mapping[str, int]]] = None,
        clinic_forbidden_people: Optional[Mapping[int, Sequence[str]]] = None,
        duty_seniority_rules: Optional[Mapping[int, Mapping[str, int]]] = None,
        duty_senorty_rules: Optional[Mapping[int, Mapping[str, int]]] = None,
        repeat_history: Optional[Mapping[int, Sequence[str]]] = None,
        weekend_history_counts: Optional[Mapping[str, int]] = None,
        leave_calendar: Optional[Mapping[str, Sequence[Tuple[dt.date, dt.date]]]] = None,
        objective_mode: str = "seniority",
    ):
        self.people: List[Person] = list(people)
        self.slots: List[DutySlot] = list(slots)
        self.rest_buffer = dt.timedelta(hours=rest_buffer_hours)
        self.enforce_person_limits = enforce_person_limits
        self.weekend_slot_indices: Set[int] = {
            idx
            for idx, slot in enumerate(self.slots)
            if slot.duty_type == "duty" and slot.start.weekday() >= 5
        }
        self.weekend_slot_count = len(self.weekend_slot_indices)
        self.clinic_rotation_days: Dict[int, int] = {}
        if clinic_rotation_days:
            for clinic_key, raw_days in clinic_rotation_days.items():
                try:
                    clinic_id = int(clinic_key)
                except (TypeError, ValueError):
                    continue
                try:
                    days_value = int(raw_days)
                except (TypeError, ValueError):
                    continue
                self.clinic_rotation_days[clinic_id] = max(0, days_value)
        self.clinic_seniority_rules: Dict[int, Dict[str, int]] = {}
        if clinic_seniority_rules:
            for clinic_key, rules in clinic_seniority_rules.items():
                try:
                    clinic_id = int(clinic_key)
                except (TypeError, ValueError):
                    continue
                normalized_rules: Dict[str, int] = {}
                for seniority_key, count_value in (rules or {}).items():
                    key = (seniority_key or "").strip().lower()
                    if key not in SENIORITY_LEVELS:
                        continue
                    try:
                        count_int = int(count_value)
                    except (TypeError, ValueError):
                        continue
                    if count_int < 0:
                        continue
                    normalized_rules[key] = count_int
                if normalized_rules:
                    self.clinic_seniority_rules[clinic_id] = normalized_rules
        self.clinic_forbidden_people: Dict[int, Set[str]] = {}
        if clinic_forbidden_people:
            for clinic_key, identifiers in clinic_forbidden_people.items():
                try:
                    clinic_id = int(clinic_key)
                except (TypeError, ValueError):
                    continue
                normalized_people: Set[str] = {
                    str(identifier).strip()
                    for identifier in identifiers or []
                    if isinstance(identifier, str) and str(identifier).strip()
                }
                if normalized_people:
                    self.clinic_forbidden_people[clinic_id] = normalized_people
        self.duty_seniority_rules: Dict[int, Dict[str, int]] = {}
        rules_source = duty_seniority_rules or duty_senorty_rules or {}
        if rules_source:
            for duty_key, rules in rules_source.items():
                try:
                    duty_id = int(duty_key)
                except (TypeError, ValueError):
                    continue
                normalized_rules: Dict[str, int] = {}
                for seniority_key, count_value in (rules or {}).items():
                    key = (seniority_key or "").strip().lower()
                    if key not in SENIORITY_LEVELS:
                        continue
                    try:
                        count_int = int(count_value)
                    except (TypeError, ValueError):
                        continue
                    if count_int < 0:
                        continue
                    normalized_rules[key] = count_int
                if normalized_rules:
                    self.duty_seniority_rules[duty_id] = normalized_rules
        self.person_leave_windows: Dict[str, List[Tuple[dt.date, dt.date]]] = {}
        if leave_calendar:
            for identifier, windows in leave_calendar.items():
                if not identifier:
                    continue
                normalized_windows: List[Tuple[dt.date, dt.date]] = []
                for window in windows or []:
                    if not isinstance(window, tuple) or len(window) != 2:
                        continue
                    start_entry, end_entry = window
                    if not isinstance(start_entry, dt.date) or not isinstance(end_entry, dt.date):
                        continue
                    start_date = start_entry
                    end_date = end_entry
                    if end_date < start_date:
                        start_date, end_date = end_date, start_date
                normalized_windows.append((start_date, end_date))
                if normalized_windows:
                    self.person_leave_windows[identifier] = normalized_windows
        self.weekend_history_counts: Dict[str, int] = {}
        if weekend_history_counts:
            for identifier, count in weekend_history_counts.items():
                if not identifier:
                    continue
                try:
                    normalized_count = int(count)
                except (TypeError, ValueError):
                    continue
                if normalized_count > 0:
                    self.weekend_history_counts[identifier] = max(0, normalized_count)
        self.weekend_penalty_weight = 3
        self.repeat_penalty_weight = 5
        self.clinic_repeat_history: Dict[int, Set[str]] = {}
        if repeat_history:
            for clinic_key, identifiers in repeat_history.items():
                try:
                    clinic_id = int(clinic_key)
                except (TypeError, ValueError):
                    continue
                normalized_people = {
                    identifier.strip()
                    for identifier in identifiers or []
                    if isinstance(identifier, str) and identifier.strip()
                }
                if normalized_people:
                    self.clinic_repeat_history[clinic_id] = normalized_people
        self.repeat_penalty_variables: List[cp_model.IntVar] = []
        self.fallback_penalty_vars: List[cp_model.IntVar] = []
        self.fallback_penalty_weight = max(10, len(self.slots))
        allowed_modes = {"seniority", "balanced"}
        self.objective_mode = objective_mode if objective_mode in allowed_modes else "seniority"
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        if not self.people:
            raise ValueError("At least one person is required.")
        if not self.slots:
            raise ValueError("At least one duty slot is required.")
        unknown_levels = {p.seniority for p in self.people if p.seniority not in SENIORITY_LEVELS}
        if unknown_levels:
            raise ValueError(f"Unknown seniority levels: {sorted(unknown_levels)}")

    @staticmethod
    def _is_assistant(person: Person) -> bool:
        title = (person.title or "").strip().lower()
        return title.startswith("asst") or person.education_year is not None

    def _person_on_leave_during_slot(self, person_identifier: str, slot: DutySlot) -> bool:
        """Return True if the slot overlaps with a leave window for the person."""
        windows = self.person_leave_windows.get(person_identifier)
        if not windows:
            return False
        slot_start = slot.start
        slot_end = slot.end
        for start_date, end_date in windows:
            leave_start = dt.datetime.combine(start_date, dt.time.min)
            leave_end = dt.datetime.combine(end_date, dt.time.max)
            if slot_start <= leave_end and slot_end >= leave_start:
                return True
        return False

    def _clinic_assignment_repeat(self, person_identifier: str, slot: DutySlot) -> bool:
        """Check whether assigning this slot repeats previous clinic duty for the person."""
        if slot.duty_type != "clinic":
            return False
        clinic_id, _position = self._parse_clinic_slot_identifier(slot.identifier)
        if clinic_id is None:
            return False
        repeated_people = self.clinic_repeat_history.get(clinic_id)
        if not repeated_people:
            return False
        return person_identifier in repeated_people

    def _build_assignment_variables(
        self, model: cp_model.CpModel
    ) -> Dict[Tuple[int, int], cp_model.IntVar]:
        """Creates boolean variables for eligible person-slot pairs."""
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar] = {}
        for p_idx, person in enumerate(self.people):
            for s_idx, slot in enumerate(self.slots):
                allowed = person.allowed_duty_types
                if "*" not in allowed and slot.duty_type not in allowed:
                    continue
                if slot.duty_type == "clinic":
                    clinic_id, _position = self._parse_clinic_slot_identifier(slot.identifier)
                    allow_specialist = False
                    if clinic_id is not None:
                        rules = self.clinic_seniority_rules.get(clinic_id, {})
                        allow_specialist = bool(rules.get("uzman"))
                        forbidden_people = self.clinic_forbidden_people.get(clinic_id)
                        if forbidden_people and person.identifier in forbidden_people:
                            continue
                    if not allow_specialist and not self._is_assistant(person):
                        continue
                if self._person_on_leave_during_slot(person.identifier, slot):
                    continue
                var_name = f"assign_p{p_idx}_s{s_idx}"
                var = model.NewBoolVar(var_name)
                assignment_vars[(p_idx, s_idx)] = var
                if self._clinic_assignment_repeat(person.identifier, slot):
                    self.repeat_penalty_variables.append(var)
        return assignment_vars

    def _build_person_totals(
        self,
        model: cp_model.CpModel,
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar],
    ) -> Tuple[List[cp_model.IntVar], List[cp_model.IntVar], List[cp_model.IntVar], int, int]:
        """Create helper variables that track per-person slot counts and total hours."""
        total_slots = len(self.slots)
        total_hours = sum(int(slot.duration_hours) for slot in self.slots)
        load_vars: List[cp_model.IntVar] = []
        hour_vars: List[cp_model.IntVar] = []
        weekend_vars: List[cp_model.IntVar] = []
        for p_idx in range(len(self.people)):
            paired_assignments = [
                (s_idx, assignment_vars[(p_idx, s_idx)])
                for s_idx in range(total_slots)
                if (p_idx, s_idx) in assignment_vars
            ]
            load_var = model.NewIntVar(0, total_slots, f"load_p{p_idx}")
            if paired_assignments:
                model.Add(load_var == sum(var for _idx, var in paired_assignments))
            else:
                model.Add(load_var == 0)
            load_vars.append(load_var)

            hours_upper = total_hours
            hour_var = model.NewIntVar(0, hours_upper, f"hours_p{p_idx}")
            if paired_assignments:
                model.Add(
                    hour_var
                    == sum(
                        int(self.slots[s_idx].duration_hours) * var
                        for s_idx, var in paired_assignments
                    )
                )
            else:
                model.Add(hour_var == 0)
            hour_vars.append(hour_var)
            weekend_upper = self.weekend_slot_count
            weekend_var = model.NewIntVar(0, weekend_upper, f"weekend_p{p_idx}")
            if paired_assignments and self.weekend_slot_indices:
                model.Add(
                    weekend_var
                    == sum(
                        var
                        for s_idx, var in paired_assignments
                        if s_idx in self.weekend_slot_indices
                    )
                )
            else:
                model.Add(weekend_var == 0)
            weekend_vars.append(weekend_var)
        return load_vars, hour_vars, weekend_vars, total_slots, total_hours

    def _enforce_slot_coverage(
        self,
        model: cp_model.CpModel,
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar],
    ) -> None:
        """Every slot must be filled by exactly one eligible person."""
        for s_idx, _slot in enumerate(self.slots):
            candidate_vars = [
                assignment_vars[(p_idx, s_idx)]
                for p_idx in range(len(self.people))
                if (p_idx, s_idx) in assignment_vars
            ]
            if not candidate_vars:
                raise ValueError(
                    f"No eligible personnel found for slot '{self.slots[s_idx].identifier}'. "
                    "Adjust allowed duty types to make the problem feasible."
                )
            model.Add(sum(candidate_vars) == 1)

    @staticmethod
    def _parse_clinic_slot_identifier(identifier: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract clinic id and position index from slot identifier strings."""
        if not identifier.startswith("clinic_"):
            return None, None
        parts = identifier.split("_")
        if len(parts) < 3:
            return None, None
        try:
            clinic_id = int(parts[1])
        except ValueError:
            return None, None
        if len(parts) >= 4:
            try:
                position_idx = int(parts[3])
            except ValueError:
                position_idx = 1
        else:
            position_idx = 1
        return clinic_id, position_idx

    def _collect_clinic_slot_groups(self) -> Dict[int, Dict[int, List[Tuple[int, DutySlot]]]]:
        """Group clinic slots by clinic id and assistant position index."""
        groups: Dict[int, Dict[int, List[Tuple[int, DutySlot]]]] = {}
        for s_idx, slot in enumerate(self.slots):
            if slot.duty_type != "clinic":
                continue
            clinic_id, position_idx = self._parse_clinic_slot_identifier(slot.identifier)
            if clinic_id is None or position_idx is None:
                continue
            position_map = groups.setdefault(clinic_id, {})
            position_map.setdefault(position_idx, []).append((s_idx, slot))

        for position_map in groups.values():
            for slot_list in position_map.values():
                slot_list.sort(key=lambda item: item[1].start)
        return groups

    @staticmethod
    def _parse_duty_slot_identifier(identifier: str) -> Optional[int]:
        """Extract duty type id from duty slot identifiers."""
        if not identifier.startswith("duty_"):
            return None
        parts = identifier.split("_", 2)
        if len(parts) < 2:
            return None
        try:
            duty_id = int(parts[1])
        except ValueError:
            return None
        return duty_id

    def _collect_duty_slot_groups(self) -> Dict[int, Dict[str, List[int]]]:
        """Group duty slots by duty type and calendar day."""
        groups: Dict[int, Dict[str, List[int]]] = {}
        for s_idx, slot in enumerate(self.slots):
            if slot.duty_type != "duty":
                continue
            duty_id = self._parse_duty_slot_identifier(slot.identifier)
            if duty_id is None:
                continue
            date_key = slot.start.date().isoformat()
            date_map = groups.setdefault(duty_id, {})
            date_map.setdefault(date_key, []).append(s_idx)
        return groups


    def _enforce_clinic_rotation_and_seniority(
        self,
        model: cp_model.CpModel,
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar],
    ) -> None:
        """Ensure clinic slots follow rotation periods and honour seniority counts."""
        if not self.clinic_rotation_days and not self.clinic_seniority_rules:
            # No additional constraints defined.
            return

        grouped_slots = self._collect_clinic_slot_groups()
        if not grouped_slots:
            return

        for clinic_id, position_map in grouped_slots.items():
            rotation_days = self.clinic_rotation_days.get(clinic_id, 1)
            clinic_rules = self.clinic_seniority_rules.get(clinic_id, {})
            # Determine the earliest slot date for this clinic to anchor rotation windows.
            base_date: Optional[dt.date] = None
            for slot_list in position_map.values():
                for _, slot in slot_list:
                    slot_date = slot.start.date()
                    if base_date is None or slot_date < base_date:
                        base_date = slot_date
            if base_date is None:
                continue

            block_representatives: Dict[int, List[int]] = defaultdict(list)

            for slot_list in position_map.values():
                if not slot_list:
                    continue
                blocks: Dict[int, List[Tuple[int, DutySlot]]] = defaultdict(list)
                for s_idx, slot in slot_list:
                    if rotation_days <= 0:
                        block_key = 0
                    else:
                        delta_days = (slot.start.date() - base_date).days
                        block_key = delta_days // rotation_days
                    blocks[block_key].append((s_idx, slot))

                for block_key, grouped in blocks.items():
                    # Keep original order to maintain deterministic representative selection.
                    grouped.sort(key=lambda item: item[1].start)
                    representative_idx = grouped[0][0]
                    block_representatives[block_key].append(representative_idx)
                    for s_idx, _slot in grouped[1:]:
                        for p_idx in range(len(self.people)):
                            var_ref = assignment_vars.get((p_idx, representative_idx))
                            var_other = assignment_vars.get((p_idx, s_idx))
                            if var_ref is None or var_other is None:
                                continue
                            model.Add(var_other == var_ref)

            if not clinic_rules:
                continue

            for block_key, representative_indices in block_representatives.items():
                if not representative_indices:
                    continue
                for seniority_key, required_count in clinic_rules.items():
                    if required_count <= 0:
                        continue
                    exact_vars: List[cp_model.IntVar] = []
                    fallback_vars: List[cp_model.IntVar] = []
                    for rep_idx in representative_indices:
                        for p_idx, person in enumerate(self.people):
                            var = assignment_vars.get((p_idx, rep_idx))
                            if var is None:
                                continue
                            if person.seniority == seniority_key:
                                exact_vars.append(var)
                            elif self._is_assistant(person):
                                fallback_vars.append(var)
                    if not exact_vars and not fallback_vars:
                        model.Add(0 == required_count)
                        continue
                    total_vars = exact_vars + fallback_vars
                    model.Add(cp_model.LinearExpr.Sum(total_vars) == required_count)
                    fallback_usage = model.NewIntVar(0, required_count, f"fallback_clinic_{clinic_id}_{block_key}_{seniority_key}")
                    model.Add(
                        fallback_usage
                        == required_count - cp_model.LinearExpr.Sum(exact_vars)
                    )
                    self.fallback_penalty_vars.append(fallback_usage)

    def _enforce_duty_seniority_rules(
        self,
        model: cp_model.CpModel,
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar],
    ) -> None:
        """Apply seniority requirements for duty slots on each day."""
        if not self.duty_seniority_rules:
            return

        grouped_slots = self._collect_duty_slot_groups()
        if not grouped_slots:
            return

        for duty_id, date_map in grouped_slots.items():
            duty_rules = self.duty_seniority_rules.get(duty_id, {})
            if not duty_rules:
                continue
            for date_key, slot_indices in date_map.items():
                if not slot_indices:
                    continue
                for seniority_key, required_count in duty_rules.items():
                    if required_count <= 0:
                        continue
                    exact_vars: List[cp_model.IntVar] = []
                    fallback_vars: List[cp_model.IntVar] = []
                    for s_idx in slot_indices:
                        for p_idx, person in enumerate(self.people):
                            var = assignment_vars.get((p_idx, s_idx))
                            if var is None:
                                continue
                            if person.seniority == seniority_key:
                                exact_vars.append(var)
                            elif self._is_assistant(person):
                                fallback_vars.append(var)
                    if not exact_vars and not fallback_vars:
                        model.Add(0 == required_count)
                        continue
                    total_vars = exact_vars + fallback_vars
                    model.Add(cp_model.LinearExpr.Sum(total_vars) == required_count)
                    fallback_usage = model.NewIntVar(
                        0,
                        required_count,
                        f"fallback_duty_{duty_id}_{date_key}_{seniority_key}",
                    )
                    model.Add(
                        fallback_usage
                        == required_count - cp_model.LinearExpr.Sum(exact_vars)
                    )
                    self.fallback_penalty_vars.append(fallback_usage)

    def _enforce_non_overlap_and_rest(
        self,
        model: cp_model.CpModel,
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar],
    ) -> None:
        """Prevent assigning conflicting duties to the same person."""
        slot_pairs = self._compute_conflicting_slot_pairs()
        for p_idx, _person in enumerate(self.people):
            for s_a, s_b in slot_pairs:
                var_a = assignment_vars.get((p_idx, s_a))
                var_b = assignment_vars.get((p_idx, s_b))
                if var_a is not None and var_b is not None:
                    model.Add(var_a + var_b <= 1)

    def _enforce_person_limits(
        self,
        model: cp_model.CpModel,
        assignment_vars: Dict[Tuple[int, int], cp_model.IntVar],
    ) -> None:
        """Apply per-person minimum/maximum assignment limits if configured."""
        if not self.enforce_person_limits:
            return
        total_slots = len(self.slots)
        for p_idx, person in enumerate(self.people):
            person_vars = [
                assignment_vars[(p_idx, s_idx)]
                for s_idx in range(total_slots)
                if (p_idx, s_idx) in assignment_vars
            ]
            if not person_vars:
                continue
            if person.min_night_duties is not None:
                model.Add(sum(person_vars) >= max(person.min_night_duties, 0))
            if person.max_night_duties is not None:
                model.Add(sum(person_vars) <= max(person.max_night_duties, 0))

    def _compute_conflicting_slot_pairs(self) -> List[Tuple[int, int]]:
        """Pre-compute all slot index pairs that cannot be held by one person."""
        conflicting_pairs: List[Tuple[int, int]] = []
        for i, slot_a in enumerate(self.slots):
            for j in range(i + 1, len(self.slots)):
                slot_b = self.slots[j]
                if self._slots_overlap(slot_a, slot_b) or self._violates_rest(slot_a, slot_b):
                    conflicting_pairs.append((i, j))
        return conflicting_pairs

    def _slots_overlap(self, slot_a: DutySlot, slot_b: DutySlot) -> bool:
        latest_start = max(slot_a.start, slot_b.start)
        earliest_end = min(slot_a.end, slot_b.end)
        return latest_start < earliest_end

    def _violates_rest(self, slot_a: DutySlot, slot_b: DutySlot) -> bool:
        """Checks the 48-hour rest rule for overnight/full duties."""
        if not (slot_a.requires_extended_rest and slot_b.requires_extended_rest):
            return False

        # Order slots chronologically.
        earlier, later = sorted((slot_a, slot_b), key=lambda x: x.start)
        rest_window_end = earlier.end + self.rest_buffer
        return later.start < rest_window_end

    def _add_seniority_objective(
        self,
        model: cp_model.CpModel,
        load_vars: Sequence[cp_model.IntVar],
        weekend_vars: Sequence[cp_model.IntVar],
        total_slots: int,
    ) -> None:
        """Softly steer towards seniority-driven workloads via absolute deviation."""
        objective_terms: List[cp_model.IntVar] = []

        for p_idx, person in enumerate(self.people):
            load = load_vars[p_idx]
            preferred = person.preferred_load()
            diff = model.NewIntVar(-total_slots, total_slots, f"seniority_diff_p{p_idx}")
            model.Add(diff == load - preferred)

            abs_diff = model.NewIntVar(0, total_slots, f"seniority_abs_diff_p{p_idx}")
            model.AddAbsEquality(abs_diff, diff)

            weight = person.weight()
            weighted_deviation = model.NewIntVar(0, total_slots * weight, f"seniority_weighted_diff_p{p_idx}")
            model.Add(weighted_deviation == abs_diff * weight)
            objective_terms.append(weighted_deviation)

        weekend_terms = self._build_weekend_fairness_terms(model, weekend_vars)
        objective_expr = cp_model.LinearExpr.Sum(
            [term for term in objective_terms]
            + [self.weekend_penalty_weight * term for term in weekend_terms]
        ) if (objective_terms or weekend_terms) else 0
        if self.fallback_penalty_vars:
            fallback_expr = cp_model.LinearExpr.Sum(self.fallback_penalty_vars)
            objective_expr = objective_expr + self.fallback_penalty_weight * fallback_expr
        if self.repeat_penalty_variables:
            penalty_expr = cp_model.LinearExpr.Sum(self.repeat_penalty_variables)
            objective_expr = objective_expr + self.repeat_penalty_weight * penalty_expr
        model.Minimize(objective_expr)

    def _add_balanced_objective(
        self,
        model: cp_model.CpModel,
        load_vars: Sequence[cp_model.IntVar],
        hour_vars: Sequence[cp_model.IntVar],
        weekend_vars: Sequence[cp_model.IntVar],
        total_slots: int,
        total_hours: int,
    ) -> None:
        """Drive the solver towards equal duty counts and hours across assistants."""
        if not load_vars:
            model.Minimize(0)
            return

        num_people = len(self.people)
        total_slots = max(total_slots, 0)
        total_hours = max(total_hours, 0)
        average_duration = max(1, total_hours // max(1, total_slots)) if total_slots else 1

        abs_slot_terms: List[cp_model.IntVar] = []
        abs_hour_terms: List[cp_model.IntVar] = []

        slot_bound = total_slots * num_people
        hour_bound = total_hours * max(1, num_people)

        for p_idx in range(num_people):
            load_var = load_vars[p_idx]
            hour_var = hour_vars[p_idx]

            slot_diff = model.NewIntVar(-slot_bound, slot_bound, f"balanced_slot_diff_p{p_idx}")
            model.Add(slot_diff == load_var * num_people - total_slots)
            slot_abs = model.NewIntVar(0, slot_bound, f"balanced_slot_abs_p{p_idx}")
            model.AddAbsEquality(slot_abs, slot_diff)
            abs_slot_terms.append(slot_abs)

            hour_diff = model.NewIntVar(-hour_bound, hour_bound, f"balanced_hour_diff_p{p_idx}")
            model.Add(hour_diff == hour_var * num_people - total_hours)
            hour_abs = model.NewIntVar(0, hour_bound, f"balanced_hour_abs_p{p_idx}")
            model.AddAbsEquality(hour_abs, hour_diff)
            abs_hour_terms.append(hour_abs)

        objective_expr: List[cp_model.IntVar] = []
        count_weight = max(1, average_duration)
        for term in abs_slot_terms:
            objective_expr.append(count_weight * term)
        objective_expr.extend(abs_hour_terms)
        weekend_terms = self._build_weekend_fairness_terms(model, weekend_vars)
        objective_sum = cp_model.LinearExpr.Sum(
            [expr for expr in objective_expr]
            + [self.weekend_penalty_weight * term for term in weekend_terms]
        ) if (objective_expr or weekend_terms) else 0
        if self.fallback_penalty_vars:
            fallback_expr = cp_model.LinearExpr.Sum(self.fallback_penalty_vars)
            objective_sum = objective_sum + self.fallback_penalty_weight * fallback_expr
        if self.repeat_penalty_variables:
            penalty_expr = cp_model.LinearExpr.Sum(self.repeat_penalty_variables)
            objective_sum = objective_sum + self.repeat_penalty_weight * penalty_expr
        model.Minimize(objective_sum)

    def _build_weekend_fairness_terms(
        self,
        model: cp_model.CpModel,
        weekend_vars: Sequence[cp_model.IntVar],
    ) -> List[cp_model.IntVar]:
        """Generate absolute deviation terms for weekend fairness balancing."""
        if not self.weekend_slot_indices or not weekend_vars:
            return []
        num_people = len(self.people)
        if num_people == 0:
            return []
        weekend_slots = self.weekend_slot_count
        total_history = sum(
            self.weekend_history_counts.get(person.identifier, 0)
            for person in self.people
        )
        total_final = total_history + weekend_slots
        if total_final == 0:
            return []
        scaled_bound = total_final * max(1, num_people)
        terms: List[cp_model.IntVar] = []
        for p_idx, person in enumerate(self.people):
            history_count = self.weekend_history_counts.get(person.identifier, 0)
            weekend_var = weekend_vars[p_idx]
            diff = model.NewIntVar(-scaled_bound, scaled_bound, f"weekend_diff_p{p_idx}")
            model.Add(diff == (history_count + weekend_var) * num_people - total_final)
            abs_diff = model.NewIntVar(0, scaled_bound, f"weekend_abs_p{p_idx}")
            model.AddAbsEquality(abs_diff, diff)
            terms.append(abs_diff)
        return terms

    def solve(self) -> cp_model.CpSolver:
        """Builds the full model and returns the configured solver after solving."""
        model = cp_model.CpModel()
        assignment_vars = self._build_assignment_variables(model)
        self._enforce_slot_coverage(model, assignment_vars)
        self._enforce_clinic_rotation_and_seniority(model, assignment_vars)
        self._enforce_duty_seniority_rules(model, assignment_vars)
        self._enforce_non_overlap_and_rest(model, assignment_vars)
        self._enforce_person_limits(model, assignment_vars)
        load_vars, hour_vars, weekend_vars, total_slots, total_hours = self._build_person_totals(model, assignment_vars)
        if self.objective_mode == "balanced":
            self._add_balanced_objective(
                model,
                load_vars,
                hour_vars,
                weekend_vars,
                total_slots,
                total_hours,
            )
        else:
            self._add_seniority_objective(model, load_vars, weekend_vars, total_slots)

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 10
        solver.parameters.num_search_workers = 8

        status = solver.Solve(model)
        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            raise RuntimeError(f"Solver failed to find a solution. Status code: {status}")

        self.assignment_vars = assignment_vars  # type: ignore[assignment]
        self.solver = solver  # type: ignore[assignment]
        self.solve_status = status  # type: ignore[assignment]
        return solver

    def _require_solution(self) -> Tuple[cp_model.CpSolver, Dict[Tuple[int, int], cp_model.IntVar]]:
        if not hasattr(self, "solver") or not hasattr(self, "assignment_vars"):
            raise RuntimeError("Solve the model before requesting results.")
        return self.solver, self.assignment_vars  # type: ignore[attr-defined]

    def get_assignments(self) -> List[Dict[str, Any]]:
        solver, assignment_vars = self._require_solution()
        assignments: List[Dict[str, Any]] = []
        for s_idx, slot in enumerate(self.slots):
            assigned_person: Person | None = None
            for p_idx, person in enumerate(self.people):
                var = assignment_vars.get((p_idx, s_idx))
                if var is not None and solver.BooleanValue(var):
                    assigned_person = person
                    break

            assignments.append(
                {
                    "slot_id": slot.identifier,
                    "duty_type": slot.duty_type,
                    "label": slot.label or slot.identifier,
                    "start": slot.start.isoformat(),
                    "duration_hours": slot.duration_hours,
                    "requires_extended_rest": slot.requires_extended_rest,
                    "person_id": assigned_person.identifier if assigned_person else None,
                    "person_name": assigned_person.display_name if assigned_person else None,
                    "person_title": assigned_person.title if assigned_person else None,
                    "person_seniority": assigned_person.seniority if assigned_person else None,
                }
            )
        return assignments

    def get_person_loads(self) -> List[Dict[str, Any]]:
        solver, assignment_vars = self._require_solution()
        loads: List[Dict[str, Any]] = []
        total_slots = len(self.slots)
        slot_hours = [int(slot.duration_hours) for slot in self.slots]
        for p_idx, person in enumerate(self.people):
            load = 0
            total_hours = 0
            weekend_count = 0
            for s_idx in range(total_slots):
                var = assignment_vars.get((p_idx, s_idx))
                if var is not None and solver.BooleanValue(var):
                    load += 1
                    total_hours += slot_hours[s_idx]
                    if s_idx in self.weekend_slot_indices:
                        weekend_count += 1
            target = person.preferred_load()
            loads.append(
                {
                    "person_id": person.identifier,
                    "person_name": person.display_name,
                    "title": person.title,
                    "seniority": person.seniority,
                    "assigned_slots": load,
                    "target_slots": target,
                    "deviation": load - target,
                    "assigned_hours": total_hours,
                     "weekend_assigned": weekend_count,
                     "weekend_history": self.weekend_history_counts.get(person.identifier, 0),
                    "min_limit": person.min_night_duties,
                    "max_limit": person.max_night_duties,
                }
            )
        return loads

    def format_solution(self) -> str:
        solver, assignment_vars = self._require_solution()

        lines: List[str] = ["=== Schedule ==="]
        for s_idx, slot in enumerate(self.slots):
            slot_label = slot.label or slot.identifier
            assigned_person = None
            for p_idx, person in enumerate(self.people):
                var = assignment_vars.get((p_idx, s_idx))
                if var is not None and solver.BooleanValue(var):
                    assigned_person = person
                    break
            if assigned_person is None:
                lines.append(f"- {slot.identifier} ({slot_label}): unassigned")
            else:
                lines.append(
                    f"- {slot.identifier} ({slot.duty_type}, {slot.start:%Y-%m-%d %H:%M}, "
                    f"{slot.duration_hours}h, {slot_label}) -> {assigned_person.display_name} [{assigned_person.seniority}]"
                )

        lines.append("\n=== Load By Person ===")
        for info in self.get_person_loads():
            title_label = info.get("title") or "-"
            lines.append(
                f"- {info['person_name']:15s} | title={title_label:9s} | seniority={info['seniority']:6s} "
                f"| assigned={info['assigned_slots']} | target={info['target_slots']} "
                f"| deviation={info['deviation']:+d} | hours={info.get('assigned_hours', 0)} "
                f"| weekend={info.get('weekend_assigned', 0)} history={info.get('weekend_history', 0)}"
            )
        return "\n".join(lines)

    def pretty_print_solution(self) -> None:
        """Logs the assignments and seniority loads to stdout."""
        print(self.format_solution())


def build_demo_slots() -> List[DutySlot]:
    """Creates sample duty slots for demonstration purposes."""
    base_start = dt.datetime(2025, 1, 6, 8, 0)  # Monday 08:00

    return [
        DutySlot(
            identifier="clinic_slot_1",
            duty_type="clinic",
            start=base_start,
            duration_hours=8,
            label="Poliklinik 1",
        ),
        DutySlot(
            identifier="clinic_slot_2",
            duty_type="clinic",
            start=base_start,
            duration_hours=8,
            label="Poliklinik 2",
        ),
        DutySlot(
            identifier="night_duty",
            duty_type="night",
            start=base_start + dt.timedelta(hours=16),  # Same day 24:00
            duration_hours=16,
            label="Gece Nöbeti",
        ),
        DutySlot(
            identifier="clinic_slot_3",
            duty_type="clinic",
            start=base_start + dt.timedelta(days=1),
            duration_hours=8,
            label="Poliklinik 3",
        ),
        DutySlot(
            identifier="full_day_duty",
            duty_type="full",
            start=base_start + dt.timedelta(days=2),
            duration_hours=24,
            label="24 Saat Nöbet",
        ),
    ]


def people_from_records(records: Sequence[Mapping[str, Any]]) -> List[Person]:
    """Transform DB staff records into Person instances."""
    people: List[Person] = []
    for row in records:
        row_dict = dict(row)
        title = (row_dict.get("title") or "").strip()
        raw_seniority = (row_dict.get("seniority") or "").strip().lower()
        if title == "Uzm. Dr.":
            seniority_key = "uzman"
        else:
            seniority_key = raw_seniority if raw_seniority in SENIORITY_LEVELS else "ara"
        identifier = f"staff_{row_dict.get('id')}"
        display_name = str(row_dict.get("name") or "Bilinmeyen")
        raw_min = row_dict.get("min_night_duties_per_month")
        raw_max = row_dict.get("max_night_duties_per_month")
        try:
            min_limit = int(raw_min) if raw_min is not None else None
        except (TypeError, ValueError):
            min_limit = None
        try:
            max_limit = int(raw_max) if raw_max is not None else None
        except (TypeError, ValueError):
            max_limit = None
        if min_limit is not None and min_limit < 0:
            min_limit = None
        if max_limit is not None and max_limit < 0:
            max_limit = None
        if min_limit is not None and max_limit is not None and min_limit > max_limit:
            min_limit, max_limit = None, None
        education_raw = row_dict.get("education_year")
        try:
            education_year = int(education_raw) if education_raw is not None else None
        except (TypeError, ValueError):
            education_year = None
        night_raw = row_dict.get("night_duty_exempt")
        try:
            night_flag = bool(int(night_raw))
        except (TypeError, ValueError):
            night_flag = bool(night_raw)
        people.append(
            Person(
                identifier=identifier,
                display_name=display_name,
                seniority=seniority_key,
                title=title or None,
                min_night_duties=min_limit,
                max_night_duties=max_limit,
                education_year=education_year,
                night_duty_exempt=night_flag,
            )
        )
    return people


def slots_from_records(
    clinics: Sequence[Mapping[str, Any]],
    duty_types: Sequence[Mapping[str, Any]],
    *,
    year: Optional[int] = None,
    month: Optional[int] = None,
    period_start: Optional[dt.date] = None,
    plan_type: str = "clinic",
) -> List[DutySlot]:
    """Generate duty slots from clinic and duty type definitions for a full calendar month."""
    slots: List[DutySlot] = []
    normalized_plan = (plan_type or "clinic").strip().lower()

    if year is None or month is None:
        if period_start is not None:
            base_day = period_start
            month_days = [base_day + dt.timedelta(days=offset) for offset in range(7)]
            reference_day = month_days[0]
            year = reference_day.year
            month = reference_day.month
        else:
            today = dt.date.today()
            year = year or today.year
            month = month or today.month

    try:
        _, days_in_month = calendar.monthrange(year, month)  # type: ignore[arg-type]
    except calendar.IllegalMonthError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid month value: {month}") from exc
    first_day = dt.date(year, month, 1)
    month_days = [first_day + dt.timedelta(days=offset) for offset in range(days_in_month)]

    holiday_calendar = None
    if HOLIDAYS_AVAILABLE:
        try:
            holiday_calendar = holidays.Turkey(years=[year])
        except TypeError:  # pragma: no cover - API compatibility
            holiday_calendar = holidays.Turkey()

    include_clinic_slots = normalized_plan != "nobet"

    if include_clinic_slots:
        for clinic_row in clinics:
            clinic = dict(clinic_row)
            clinic_id = clinic.get('id')
            clinic_name = clinic.get('name') or 'Klinik'
            responsible_name = clinic.get('responsible_name')
            if responsible_name:
                clinic_display_name = f"{clinic_name} (Sorumlu: {responsible_name})"
            else:
                clinic_display_name = clinic_name
            raw_required = clinic.get('required_assistants')
            try:
                required_assistants = int(raw_required)
            except (TypeError, ValueError):
                required_assistants = 1
            required_assistants = max(required_assistants, 1)

            for day in month_days:
                is_weekend = day.weekday() >= 5
                is_holiday = holiday_calendar is not None and day in holiday_calendar
                if is_weekend or is_holiday:
                    continue
                start_dt = dt.datetime.combine(day, dt.time(8, 0))
                for idx in range(required_assistants):
                    suffix = f"_{idx + 1}" if required_assistants > 1 else ""
                    identifier = f"clinic_{clinic_id}_{day.isoformat()}{suffix}"
                    label = f"{clinic_display_name} - {day.strftime('%Y-%m-%d')}"
                    if required_assistants > 1:
                        label = f"{label} #{idx + 1}"
                    slots.append(
                        DutySlot(
                            identifier=identifier,
                            duty_type='clinic',
                            start=start_dt,
                            duration_hours=8,
                            label=label,
                        )
                    )

    for duty_row in duty_types:
        duty = dict(duty_row)
        duty_id = duty.get('id')
        duty_name = duty.get('name') or 'Nobet'
        duty_category = (duty.get('duty_category') or 'nobet').strip().lower()
        if normalized_plan == "clinic" and duty_category != "mesa":
            continue
        if normalized_plan == "nobet" and duty_category != "nobet":
            continue
        raw_duration = duty.get('duration_hours') or 0
        try:
            duration = int(raw_duration)
        except (TypeError, ValueError):
            duration = 8
        duration = max(duration, 1)
        if duration >= 16:
            start_hour = (8 - duration) % 24  # aim to finish around 08:00 next day
        else:
            start_hour = 8
        raw_required_staff = duty.get('required_staff_count')
        try:
            required_staff = int(raw_required_staff)
        except (TypeError, ValueError):
            required_staff = 1
        required_staff = max(required_staff, 1)

        for day in month_days:
            is_weekend = day.weekday() >= 5
            is_holiday = holiday_calendar is not None and day in holiday_calendar
            if duty_category == "mesa" and (is_weekend or is_holiday):
                continue
            start_dt = dt.datetime.combine(day, dt.time(start_hour, 0))
            for idx in range(required_staff):
                suffix = f"_{idx + 1}" if required_staff > 1 else ""
                identifier = f"duty_{duty_id}_{day.isoformat()}{suffix}"
                label = f"{duty_name} - {day.strftime('%Y-%m-%d')}"
                if required_staff > 1:
                    label = f"{label} #{idx + 1}"
                slots.append(
                    DutySlot(
                        identifier=identifier,
                        duty_type='duty',
                        start=start_dt,
                        duration_hours=duration,
                        label=label,
                    )
                )

    return slots



def solve_schedule(
    people: Iterable[Person],
    slots: Iterable[DutySlot],
    rest_buffer_hours: int = 48,
    enforce_person_limits: bool = False,
    *,
    clinic_rotation_periods: Optional[Mapping[int, str]] = None,
    clinic_seniority_rules: Optional[Mapping[int, Mapping[str, int]]] = None,
    clinic_forbidden_people: Optional[Mapping[int, Sequence[str]]] = None,
    duty_seniority_rules: Optional[Mapping[int, Mapping[str, int]]] = None,
    duty_senorty_rules: Optional[Mapping[int, Mapping[str, int]]] = None,
    clinic_repeat_history: Optional[Mapping[int, Sequence[str]]] = None,
    weekend_history_counts: Optional[Mapping[str, int]] = None,
    staff_leave_requests: Optional[Mapping[int, Sequence[Tuple[dt.date, dt.date]]]] = None,
    objective_mode: str = "seniority",
) -> Dict[str, Any]:
    """Solve scheduling for arbitrary people and slots."""
    if duty_seniority_rules is None and duty_senorty_rules is not None:
        duty_seniority_rules = duty_senorty_rules

    rotation_days_map: Dict[int, int] = {}
    if clinic_rotation_periods:
        for clinic_key, rotation_value in clinic_rotation_periods.items():
            try:
                clinic_id = int(clinic_key)
            except (TypeError, ValueError):
                continue
            rotation_days_map[clinic_id] = rotation_period_to_block_size(rotation_value)
    repeat_history_map: Dict[int, List[str]] = {}
    if clinic_repeat_history:
        for clinic_key, identifiers in clinic_repeat_history.items():
            try:
                clinic_id = int(clinic_key)
            except (TypeError, ValueError):
                continue
            normalized_people = [
                str(identifier).strip()
                for identifier in identifiers
                if isinstance(identifier, str) and str(identifier).strip()
            ]
            if normalized_people:
                repeat_history_map[clinic_id] = normalized_people
    weekend_history_by_identifier: Dict[str, int] = {}
    if weekend_history_counts:
        for person in people:
            raw_value = weekend_history_counts.get(person.identifier)
            if raw_value is None:
                continue
            try:
                normalized_count = int(raw_value)
            except (TypeError, ValueError):
                continue
            if normalized_count > 0:
                weekend_history_by_identifier[person.identifier] = max(0, normalized_count)
    leave_calendar: Dict[str, List[Tuple[dt.date, dt.date]]] = {}
    if staff_leave_requests:
        for person in people:
            identifier = person.identifier
            staff_id: Optional[int] = None
            if identifier.startswith("staff_"):
                try:
                    staff_id = int(identifier.split("_", 1)[1])
                except ValueError:
                    staff_id = None
            if staff_id is None:
                continue
            windows = staff_leave_requests.get(staff_id)
            if not windows:
                continue
            normalized_windows: List[Tuple[dt.date, dt.date]] = []
            for window in windows:
                if not isinstance(window, tuple) or len(window) != 2:
                    continue
                start_date, end_date = window
                if not isinstance(start_date, dt.date) or not isinstance(end_date, dt.date):
                    continue
                if end_date < start_date:
                    start_date, end_date = end_date, start_date
                normalized_windows.append((start_date, end_date))
            if normalized_windows:
                leave_calendar[identifier] = normalized_windows
    prototype = SchedulingPrototype(
        people=people,
        slots=slots,
        rest_buffer_hours=rest_buffer_hours,
        enforce_person_limits=enforce_person_limits,
        clinic_rotation_days=rotation_days_map,
        clinic_seniority_rules=clinic_seniority_rules,
        clinic_forbidden_people=clinic_forbidden_people,
        duty_seniority_rules=duty_seniority_rules,
        duty_senorty_rules=duty_senorty_rules,
        repeat_history=repeat_history_map,
        leave_calendar=leave_calendar,
        weekend_history_counts=weekend_history_by_identifier,
        objective_mode=objective_mode,
    )
    solver = prototype.solve()

    status = getattr(prototype, "solve_status", None)
    status_strings = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
    }
    if status is None:
        status_label = "UNKNOWN"
    else:
        status_label = status_strings.get(status, solver.StatusName(status))

    result = {
        "status_code": status,
        "status_label": status_label,
        "objective_value": solver.ObjectiveValue(),
        "assignments": prototype.get_assignments(),
        "loads": prototype.get_person_loads(),
        "text": prototype.format_solution(),
    }
    return result


def solve_demo_schedule() -> Dict[str, Any]:
    """Convenience wrapper that solves the demo dataset and returns structured results."""
    people = [
        Person(identifier="demo_aksoy", display_name="Dr. Aksoy", seniority="kidemli", allowed_duty_types=("clinic", "night", "full")),
        Person(identifier="demo_bal", display_name="Dr. Bal", seniority="ara", allowed_duty_types=("clinic", "night")),
        Person(identifier="demo_can", display_name="Dr. Can", seniority="comez", allowed_duty_types=("clinic",)),
        Person(identifier="demo_dur", display_name="Dr. Dur", seniority="ara", allowed_duty_types=("clinic", "full")),
    ]
    slots = build_demo_slots()
    return solve_schedule(people=people, slots=slots)


def main() -> None:
    result = solve_demo_schedule()
    print(f"Solver status: {result['status_label']}")
    print(f"Objective value (weighted deviation): {result['objective_value']:.0f}")
    print(result["text"])


if __name__ == "__main__":
    main()
