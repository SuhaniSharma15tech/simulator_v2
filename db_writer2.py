"""
================================================================================
  EduMetrics Simulator — db_writer.py  v2

  Rewrites v1 to support the full 4-year BTech structure:

  ── Structure ────────────────────────────────────────────────────────────────
  4 classes running concurrently, one per year of study:
    • 1st Year  → Semester 1 (odd) + Semester 2 (even)
    • 2nd Year  → Semester 3 (odd) + Semester 4 (even)
    • 3rd Year  → Semester 5 (odd) + Semester 6 (even)
    • 4th Year  → Semester 7 (odd) + Semester 8 (even)

  Each class has 40 students.

  ── Timeline ─────────────────────────────────────────────────────────────────
  36 weeks total per academic cycle = 2 semesters × 18 weeks each.

  Odd semester  (1,3,5,7): starts first Monday of August,
                            ends   ~third week of December
  Even semester (2,4,6,8): starts first Monday of January,
                            ends   ~third week of May

  Within each 18-week semester:
    Week  8 = Midterm exam week   (no attendance / assignments / quizzes)
    Week 18 = Endterm exam week   (no attendance / assignments / quizzes)
  Weeks 1–7, 9–17 are teaching weeks (16 weeks total per semester).

  sim_state tracks:
    • current_week  : 1–36 (global week across both semesters)
                      weeks  1–18 = semester A (the odd sem for each class)
                      weeks 19–36 = semester B (the even sem for each class)
    • sim_year      : calendar year the odd semester started

  ── Archetype profiles ───────────────────────────────────────────────────────
  Aligned with dataset.py v3:
    • Attendance bases raised so most archetypes stay above 75% threshold
    • Noise sigma tightened (7→4) to prevent random threshold crossings
    • crisis_student: acute one-semester crash per student, not chronic
    • slow_fader:     longitudinal score + attendance decay per semester
    • late_bloomer:   longitudinal improvement across semesters
    • Grade boundary B = 50–59 (corrected from 55–59)
    • Exam weeks 8 and 18 (endterm moved from 16 to 18)
    • No score_pct jitter — exact marks/max*100

  ── Public API ───────────────────────────────────────────────────────────────
    advance_week(seed=None)        -> dict   advance DB by one week (1→36)
    rollback_to_week(target)       -> dict   delete all data beyond target
    get_db_status()                -> dict   current state snapshot

  ── DB schema expectations ───────────────────────────────────────────────────
  sim_state       : id, current_week, sim_year, last_updated
  classes         : class_id, year_of_study, odd_sem, even_sem
  students        : student_id, class_id, archetype, crisis_sem
                    (crisis_sem = semester number of their crisis, 0 if none)
  subjects        : subject_id, semester
  class_subjects  : class_id, subject_id
  assignment_definitions : assignment_id, class_id, subject_id, due_week,
                           semester, max_marks
  quiz_definitions       : quiz_id, class_id, subject_id, scheduled_week,
                           semester, max_marks
  exam_schedule          : schedule_id, class_id, subject_id, exam_type,
                           scheduled_week, semester, max_marks
  dropout_events  : student_id, class_id, dropout_semester, dropout_reason,
                    last_active_week
  attendance      : student_id, class_id, subject_id, semester, week,
                    week_date, lectures_held, present, absent, late,
                    attendance_pct
  assignment_submissions : assignment_id, student_id, class_id, status,
                           submission_date, latency_hours, marks_obtained,
                           quality_pct, plagiarism_pct
  quiz_submissions       : quiz_id, student_id, class_id, attempted,
                           attempt_date, marks_obtained, score_pct
  library_visits  : student_id, class_id, semester, week, week_date,
                    physical_visits
  book_borrows    : borrow_id, student_id, class_id, semester, book_title,
                    borrow_date, return_date, borrow_week, return_week
  exam_results    : schedule_id, student_id, class_id, marks_obtained,
                    max_marks, score_pct, pass_fail, grade, result_date

  MySQL user: simulator_app
    SELECT          on all tables
    INSERT, DELETE  on all transactional tables
    UPDATE          on sim_state only
================================================================================
"""

import sys
import os
import random
from datetime import date, timedelta, datetime

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(__file__))
from connection import query, get_conn, _release

# ── SIMULATION CONSTANTS ──────────────────────────────────────────────────────
WEEKS_PER_SEM      = 18
TOTAL_WEEKS        = WEEKS_PER_SEM * 2          # 36 weeks per academic year
MIDTERM_WEEK       = 8                           # within-semester week number
ENDTERM_WEEK       = 18                          # within-semester week number
EXAM_WEEKS         = {MIDTERM_WEEK, ENDTERM_WEEK}
MIDTERM_RESULT_WEEK = MIDTERM_WEEK + 2           # results 2 weeks after exam
ENDTERM_RESULT_WEEK = ENDTERM_WEEK + 2           # = week 20, i.e. week 2 of break
RESULT_DELAY_WEEKS  = 2

# ── YEAR-OF-STUDY → (odd_semester, even_semester) mapping ────────────────────
YEAR_SEMS = {
    1: (1, 2),
    2: (3, 4),
    3: (5, 6),
    4: (7, 8),
}

# ── ARCHETYPE PROFILES (aligned with dataset.py v3) ──────────────────────────
# attend/sub/lat/qual/plag/qa/qs/lib: base values
# sem_score_delta  : pts added to exam base per semester elapsed
# sem_attend_delta : ppts added to attendance base per semester elapsed
# crisis_attend_drop / crisis_score_drop / crisis_sub_drop: applied in crisis sem
ARCHETYPES = {
    "high_performer": {
        "attend": 94, "sub": 96, "lat": -52, "qual": 88,
        "plag":  2,   "qa": 95, "qs": 87,   "lib": 3.8,
        "sem_score_delta":  -0.5,
        "sem_attend_delta": -0.5,
    },
    "consistent_avg": {
        "attend": 86, "sub": 82, "lat": -20, "qual": 68,
        "plag":  8,   "qa": 74, "qs": 65,   "lib": 1.9,
        "sem_score_delta":  -0.4,
        "sem_attend_delta": -0.8,
    },
    "late_bloomer": {
        "attend": 77, "sub": 55, "lat":  -5, "qual": 52,
        "plag": 12,   "qa": 48, "qs": 45,   "lib": 0.8,
        "sem_score_delta":  +5.0,
        "sem_attend_delta": +2.0,
        "midterm_penalty":  -12,             # low midterm, strong endterm
    },
    "slow_fader": {
        "attend": 90, "sub": 80, "lat": -22, "qual": 70,
        "plag":  9,   "qa": 72, "qs": 65,   "lib": 1.6,
        "sem_score_delta":  -4.5,
        "sem_attend_delta": -2.5,
    },
    "crammer": {
        "attend": 79, "sub": 52, "lat":  -3, "qual": 58,
        "plag": 18,   "qa": 38, "qs": 51,   "lib": 0.4,
        "sem_score_delta":  -0.8,
        "sem_attend_delta": -0.6,
    },
    "crisis_student": {
        "attend": 86, "sub": 80, "lat": -22, "qual": 70,
        "plag":  7,   "qa": 74, "qs": 65,   "lib": 1.8,
        "sem_score_delta":   -0.4,
        "sem_attend_delta":  -0.8,
        "crisis_score_drop":   -38,
        "crisis_attend_drop":  -35,
        "crisis_sub_drop":     -40,
    },
    "silent_disengager": {
        "attend": 80, "sub": 75, "lat": -14, "qual": 62,
        "plag": 14,   "qa": 10, "qs": 44,   "lib": 0.1,
        "sem_score_delta":  -1.0,
        "sem_attend_delta": -0.8,
    },
}

# Per-semester cohort-level attendance modifier (class personality drift)
# Kept small so archetypes stay above the 75% threshold
CLS_MOD = {1: 2, 2: 3, 3: -1, 4: -2, 5: -4, 6: -3, 7: -6, 8: -8}


# ── HELPERS ───────────────────────────────────────────────────────────────────
def _clamp(v, lo, hi):
    return max(lo, min(hi, v))

def _noisy(v, sigma, lo=0.0, hi=100.0):
    return _clamp(v + random.gauss(0, sigma), lo, hi)

def _arc(archetype_str):
    return ARCHETYPES.get(archetype_str, ARCHETYPES["consistent_avg"])

def _score_to_grade(pct):
    """Corrected boundaries: B=50–59, C=40–49."""
    if pct >= 90: return "O"
    if pct >= 80: return "A+"
    if pct >= 70: return "A"
    if pct >= 60: return "B+"
    if pct >= 50: return "B"
    if pct >= 40: return "C"
    return "F"


def _sem_start(sim_year, semester):
    """
    Return the first Monday of the semester.
    Odd sems  (1,3,5,7): first Monday of August  in the relevant year
    Even sems (2,4,6,8): first Monday of January in the relevant year

    sim_year is the year the odd semester started for this academic cycle.
    E.g. sim_year=2024:
        Sem 1,3,5,7 start Aug 2024
        Sem 2,4,6,8 start Jan 2025
    """
    if semester % 2 == 1:          # odd — August
        yr, month = sim_year, 8
    else:                          # even — January next calendar year
        yr, month = sim_year + 1, 1

    # First Monday of the month
    d = date(yr, month, 1)
    # weekday(): Monday=0 … Sunday=6
    days_to_monday = (7 - d.weekday()) % 7
    return d + timedelta(days=days_to_monday)


def _week_monday(sem_start_date, week_num):
    """Monday of week_num (1-indexed) within a semester."""
    return sem_start_date + timedelta(weeks=week_num - 1)


def _global_to_sem_week(global_week):
    """
    Map global simulation week (1–36) to within-semester week (1–18)
    and which semester slot ('odd' or 'even').
    """
    if global_week <= WEEKS_PER_SEM:
        return global_week, "odd"
    else:
        return global_week - WEEKS_PER_SEM, "even"


def _week_modifiers(sem_week, semester):
    """
    Additive per-week behaviour modifiers based on within-semester week.
    Exam weeks (8 and 18) are never passed here — callers skip them.
    """
    m = {"attend": 0, "lat_add": 0.0, "lib_add": 0.0, "quiz_drop": 0}
    if sem_week == 7:                        # pre-midterm pressure
        m["lib_add"] += 2.5
        m["lat_add"] += 6
        m["attend"]  -= 2
    if sem_week == 9:                        # post-midterm dip
        m["attend"]  -= 4
    if sem_week == 17:                       # pre-endterm pressure
        m["lib_add"] += 3.0
        m["lat_add"] += 8
    if semester >= 5 and 8 <= sem_week <= 13:  # placement season
        m["attend"]  -= 5
    return m


# ── DB READ HELPERS (cursor-based, for use inside transactions) ───────────────
def _fetch_all(cur, sql, params=()):
    """Execute a SELECT and return all rows as a list of dicts."""
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def _get_sim_state(cur):
    rows = _fetch_all(cur, "SELECT current_week, sim_year FROM sim_state WHERE id = 1")
    if not rows:
        raise RuntimeError("sim_state is empty — run schema SQL and seed data first.")
    return rows[0]


def _get_classes(cur):
    """Return all 4 classes with their year_of_study, odd_sem, even_sem."""
    return _fetch_all(
        cur,
        "SELECT class_id, year_of_study, odd_sem, even_sem FROM classes"
    )


def _get_students(cur, class_id):
    """
    Return students for a class.
    crisis_sem = 0 means no crisis; > 0 = the semester number of their crisis.
    dropout_* pulled from dropout_events if present.
    """
    return _fetch_all(
        cur,
        """SELECT s.student_id, s.archetype,
                  COALESCE(s.crisis_sem, 0)         AS crisis_sem,
                  COALESCE(de.dropout_semester, 0)  AS dropout_semester,
                  COALESCE(de.last_active_week, 0)  AS dropout_last_week
           FROM   students s
           LEFT JOIN dropout_events de
                  ON s.student_id = de.student_id
           WHERE  s.class_id = %s""",
        (class_id,)
    )


def _get_subjects_for_sem(cur, class_id, semester):
    return _fetch_all(
        cur,
        """SELECT s.subject_id
           FROM   subjects s
           JOIN   class_subjects cs ON s.subject_id = cs.subject_id
           WHERE  cs.class_id = %s AND s.semester = %s""",
        (class_id, semester)
    )


def _get_assignments_due(cur, class_id, semester, sem_week):
    """Assignments due in a specific within-semester week."""
    return _fetch_all(
        cur,
        """SELECT assignment_id, subject_id, max_marks
           FROM   assignment_definitions
           WHERE  class_id = %s AND semester = %s AND due_week = %s""",
        (class_id, semester, sem_week)
    )


def _get_active_load(cur, class_id, semester, sem_week):
    rows = _fetch_all(
        cur,
        """SELECT COUNT(*) AS n FROM assignment_definitions
           WHERE class_id = %s AND semester = %s AND due_week = %s""",
        (class_id, semester, sem_week)
    )
    return rows[0]["n"]


def _get_quizzes(cur, class_id, semester, sem_week):
    return _fetch_all(
        cur,
        """SELECT quiz_id, subject_id, max_marks
           FROM   quiz_definitions
           WHERE  class_id = %s AND semester = %s AND scheduled_week = %s""",
        (class_id, semester, sem_week)
    )


def _get_exam_schedule(cur, class_id, semester, exam_sem_week):
    return _fetch_all(
        cur,
        """SELECT schedule_id, subject_id, exam_type, max_marks
           FROM   exam_schedule
           WHERE  class_id = %s AND semester = %s AND scheduled_week = %s""",
        (class_id, semester, exam_sem_week)
    )


def _get_assignment_due_date(cur, assignment_id):
    rows = _fetch_all(
        cur,
        "SELECT due_week FROM assignment_definitions WHERE assignment_id = %s",
        (assignment_id,)
    )
    return rows[0]["due_week"] if rows else None


def _week_exists(cur, class_id, semester, sem_week):
    rows = _fetch_all(
        cur,
        """SELECT COUNT(*) AS n FROM attendance
           WHERE class_id = %s AND semester = %s AND week = %s""",
        (class_id, semester, sem_week)
    )
    return rows[0]["n"] > 0


def _is_student_active(stu, semester, sem_week):
    """
    Returns True if the student should generate data this week.
    A dropout stops generating data after their last_active_week
    in their dropout_semester.
    """
    dsem = stu["dropout_semester"]
    if dsem == 0:
        return True    # not a dropout
    if semester < dsem:
        return True    # haven't reached dropout semester yet
    if semester == dsem and sem_week <= stu["dropout_last_week"]:
        return True    # still active within the dropout semester
    return False       # past their dropout point


# ── ROW GENERATORS ────────────────────────────────────────────────────────────

def _build_attendance(students, subjects, class_id, semester, sem_week, wdate):
    """
    Generate attendance rows for all active students × all subjects.
    Exam weeks must NOT be passed here (caller filters them out).
    Noise sigma = 4 (tightened from v1's 7).
    """
    ev      = _week_modifiers(sem_week, semester)
    cls_mod = CLS_MOD.get(semester, 0)
    rows    = []

    for stu in students:
        if not _is_student_active(stu, semester, sem_week):
            continue

        a          = _arc(stu["archetype"])
        # Longitudinal: sem_offset = 0 for sem 1, 1 for sem 2, etc.
        sem_offset = semester - 1
        attend_adj = a.get("sem_attend_delta", 0) * sem_offset
        in_crisis  = (stu["crisis_sem"] != 0 and stu["crisis_sem"] == semester)

        # Dropout semester: attendance visibly falls as student disengages
        is_dropout_sem = (stu["dropout_semester"] != 0
                          and stu["dropout_semester"] == semester)

        base = _clamp(
            a["attend"] + cls_mod + attend_adj + ev["attend"],
            10, 100
        )
        if in_crisis:
            base += a.get("crisis_attend_drop", 0)
        if is_dropout_sem:
            base -= 20
        if stu["archetype"] == "crammer" and sem_week in (7, 17):
            base += 8

        base = _noisy(base, 4, 0, 100)   # sigma=4, not 7

        for subj in subjects:
            lectures = 3
            s_att    = _clamp(base + random.gauss(0, 5), 0, 100)
            present  = round(lectures * s_att / 100)
            late     = 1 if present < lectures and random.random() < 0.3 else 0
            absent   = max(0, lectures - present - late)
            rows.append((
                stu["student_id"], class_id, subj["subject_id"],
                semester, sem_week, str(wdate),
                lectures, present, absent, late,
                round(present / lectures * 100, 1),
            ))
    return rows


def _build_assignment_submissions(cur, students, assignments, class_id,
                                   semester, sem_week):
    if not assignments:
        return []
    ev          = _week_modifiers(sem_week, semester)
    active_load = _get_active_load(cur, class_id, semester, sem_week)
    rows        = []

    for stu in students:
        if not _is_student_active(stu, semester, sem_week):
            continue

        a         = _arc(stu["archetype"])
        sem_offset = semester - 1
        in_crisis  = (stu["crisis_sem"] != 0 and stu["crisis_sem"] == semester)

        for asn in assignments:
            ws = a["sub"]
            wl = a["lat"] + ev["lat_add"]
            wq = a["qual"]
            wp = a["plag"]

            # Longitudinal modifiers
            if stu["archetype"] == "slow_fader":
                ws -= sem_offset * 3
                wl += sem_offset * 4
                wq -= sem_offset * 3
            if stu["archetype"] == "late_bloomer":
                ws += sem_offset * 4
                wl -= sem_offset * 4
                wq += sem_offset * 5
            if in_crisis:
                ws += a.get("crisis_sub_drop", 0)
            if stu["archetype"] == "crammer":
                wl += 15
            if active_load >= 4:
                wl += 8

            submitted = random.random() < _clamp(ws, 0, 100) / 100
            if not submitted:
                rows.append((
                    asn["assignment_id"], stu["student_id"], class_id,
                    "missing", None, None, None, None, 0.0,
                ))
                continue

            latency  = _noisy(wl, 12, -120, 48)
            quality  = _noisy(wq, 10, 15, 100)
            marks    = round(asn["max_marks"] * quality / 100)
            # Exact quality_pct from marks (no jitter)
            q_pct    = round(marks / asn["max_marks"] * 100, 1)
            plag     = round(_noisy(wp, 8, 0, 80), 1) if random.random() < 0.15 else 0.0
            is_late  = latency > 0

            sub_dt = None
            due_wk = _get_assignment_due_date(cur, asn["assignment_id"])
            if due_wk:
                sem_start = _sem_start_from_cache(class_id, semester)
                due_date  = _week_monday(sem_start, due_wk)
                sub_dt    = str(due_date + timedelta(hours=latency))

            rows.append((
                asn["assignment_id"], stu["student_id"], class_id,
                "late" if is_late else "on_time",
                sub_dt, round(latency, 1),
                marks, q_pct, plag,
            ))
    return rows


def _build_quiz_submissions(students, quizzes, class_id, semester, sem_week):
    if not quizzes:
        return []
    ev   = _week_modifiers(sem_week, semester)
    rows = []

    for stu in students:
        if not _is_student_active(stu, semester, sem_week):
            continue

        a          = _arc(stu["archetype"])
        sem_offset = semester - 1
        in_crisis  = (stu["crisis_sem"] != 0 and stu["crisis_sem"] == semester)

        for qz in quizzes:
            wqa = a["qa"] + ev["quiz_drop"]
            wqs = a["qs"]

            if stu["archetype"] == "silent_disengager":
                wqa -= 60
            if stu["archetype"] == "slow_fader":
                wqa -= sem_offset * 5
                wqs -= sem_offset * 3
            if in_crisis:
                wqa -= 50
                wqs -= 30
            if stu["archetype"] == "late_bloomer":
                wqa += sem_offset * 8
                wqs += sem_offset * 6
            if stu["archetype"] == "crammer" and sem_week in (7, 17):
                wqa += 35

            attempted = random.random() < _clamp(wqa, 0, 100) / 100
            qdate     = str(datetime.now().date())

            if not attempted:
                rows.append((
                    qz["quiz_id"], stu["student_id"], class_id,
                    0, None, None, None,
                ))
                continue

            spct  = _noisy(wqs, 12, 10, 100)
            marks = round(qz["max_marks"] * spct / 100)
            # Exact score_pct from marks (no jitter)
            s_pct = round(marks / qz["max_marks"] * 100, 1)
            rows.append((
                qz["quiz_id"], stu["student_id"], class_id,
                1, qdate, marks, s_pct,
            ))
    return rows


def _build_library_visits(students, class_id, semester, sem_week, wdate):
    ev   = _week_modifiers(sem_week, semester)
    rows = []

    for stu in students:
        if not _is_student_active(stu, semester, sem_week):
            continue

        a          = _arc(stu["archetype"])
        sem_offset = semester - 1
        in_crisis  = (stu["crisis_sem"] != 0 and stu["crisis_sem"] == semester)
        wlib       = a["lib"] + ev["lib_add"]

        if stu["archetype"] == "silent_disengager":
            wlib = max(0, wlib - 0.8)
        if in_crisis:
            wlib = 0.0
        if stu["archetype"] == "crammer" and sem_week in (7, 17):
            wlib += 3.0
        if stu["archetype"] == "late_bloomer":
            wlib += sem_offset * 0.3

        visits = max(0, round(random.gauss(wlib, 0.8)))
        rows.append((
            stu["student_id"], class_id,
            semester, sem_week, str(wdate), visits,
        ))
    return rows


def _build_exam_results(students, exams, class_id, semester, result_date):
    """
    Build exam result rows.
    Called at result_week = exam_week + 2.
    Exact score_pct = marks/max*100, no jitter.
    Corrected grade boundary: B=50–59.
    """
    if not exams:
        return []
    cls_mod    = CLS_MOD.get(semester, 0)
    sem_offset = semester - 1
    rows       = []

    for stu in students:
        # Students who dropped before the exam don't get results
        dsem = stu["dropout_semester"]
        if dsem != 0 and dsem <= semester:
            # Check if they were active during the actual exam week
            if dsem < semester:
                continue
            # Same dropout semester: only active if dropout_last_week >= exam_week
            # We can't know exam_week here directly, but exams stores exam_type
            # We'll filter: if last_active_week < MIDTERM_WEEK, skip all
            # For simplicity: skip if already dropped
            if stu["dropout_last_week"] < MIDTERM_WEEK:
                continue

        a         = _arc(stu["archetype"])
        in_crisis = (stu["crisis_sem"] != 0 and stu["crisis_sem"] == semester)

        for ex in exams:
            exam_type = ex["exam_type"]

            # Base scores per archetype (same as dataset.py v3)
            base_map = {
                "high_performer":    87,
                "consistent_avg":    67,
                "late_bloomer":      49 if exam_type == "midterm" else 64,
                "slow_fader":        75 if exam_type == "midterm" else 55,
                "crammer":           52 if exam_type == "midterm" else 66,
                "crisis_student":    67 if exam_type == "midterm" else 47,
                "silent_disengager": 57,
            }
            base = base_map.get(stu["archetype"], 65)
            base += cls_mod * 0.3

            # Longitudinal score shifts
            score_adj = a.get("sem_score_delta", 0) * sem_offset
            base += score_adj

            # Late bloomer: midterm penalty shrinks as semesters pass
            if stu["archetype"] == "late_bloomer" and exam_type == "midterm":
                base -= max(0, 12 - sem_offset * 2)

            # Slow fader: endterm decays faster
            if stu["archetype"] == "slow_fader" and exam_type == "endterm":
                base -= sem_offset * 3

            # Crisis: acute crash
            if in_crisis:
                base += a.get("crisis_score_drop", 0)
                if exam_type == "endterm":
                    base -= 10

            base     = _noisy(base, 9, 10, 100)
            marks    = round(ex["max_marks"] * base / 100)
            pct      = round(marks / ex["max_marks"] * 100, 1)

            rows.append((
                ex["schedule_id"], stu["student_id"], class_id,
                marks, ex["max_marks"], pct,
                "P" if pct >= 40 else "F",
                _score_to_grade(pct),
                str(result_date),
            ))
    return rows


# ── SEM-START CACHE (avoid repeated DB hits within a transaction) ─────────────
_sem_start_cache = {}

def _sem_start_from_cache(class_id, semester):
    """
    Return the first Monday of `semester` for the given class.
    Populated by _populate_sem_start_cache() at the start of each transaction.
    """
    return _sem_start_cache[(class_id, semester)]


def _populate_sem_start_cache(cur, classes, sim_year):
    """
    Pre-populate the sem-start cache for all classes and their semesters.
    Must be called inside the transaction after sim_state and classes are read.
    """
    _sem_start_cache.clear()
    for cls in classes:
        for sem in (cls["odd_sem"], cls["even_sem"]):
            key = (cls["class_id"], sem)
            _sem_start_cache[key] = _sem_start(sim_year, sem)


# ── PUBLIC: ADVANCE WEEK ──────────────────────────────────────────────────────
def advance_week(seed=None):
    """
    Advance the simulation by exactly one global week (1 → 36).

    Global week mapping:
      Weeks  1–18 → within-semester weeks 1–18 of the ODD semester
                    (no rows on weeks 8 and 18 — exam-only weeks)
      Weeks 19–36 → within-semester weeks 1–18 of the EVEN semester

    All 4 classes are advanced simultaneously, mirroring real college calendar.

    Returns:
      {
        "global_week": int,        # 1–36
        "sem_week":    int,        # 1–18 within-semester
        "semester_slot": str,      # "odd" or "even"
        "is_exam_week": bool,
        "classes": { class_id: { table: row_count } }
      }
    """
    if seed is not None:
        random.seed(seed)

    conn = get_conn()
    conn.autocommit = False
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ── All reads happen inside the transaction ───────────────────────
        state       = _get_sim_state(cur)
        cur_global  = state["current_week"]
        sim_year    = state["sim_year"]
        new_global  = cur_global + 1

        if new_global > TOTAL_WEEKS:
            raise ValueError(
                f"Academic year complete — already at global week {cur_global} "
                f"(max {TOTAL_WEEKS}). Use rollback_to_week(0) to reset."
            )

        sem_week, slot = _global_to_sem_week(new_global)
        is_exam_week   = sem_week in EXAM_WEEKS

        classes = _get_classes(cur)

        # Pre-populate sem-start cache inside the transaction
        _populate_sem_start_cache(cur, classes, sim_year)

        summary = {
            "global_week":   new_global,
            "sem_week":      sem_week,
            "semester_slot": slot,
            "is_exam_week":  is_exam_week,
            "classes":       {},
        }

        for cls in classes:
            class_id   = cls["class_id"]
            year       = cls["year_of_study"]
            odd_sem    = cls["odd_sem"]
            even_sem   = cls["even_sem"]
            semester   = odd_sem if slot == "odd" else even_sem

            # Each class has its own semester start (already in cache)
            sem_start  = _sem_start_from_cache(class_id, semester)
            wdate      = _week_monday(sem_start, sem_week)

            # Skip if this class-semester-week already has data (idempotent)
            if _week_exists(cur, class_id, semester, sem_week):
                print(f"  [{class_id}] sem {semester} week {sem_week} "
                      f"already exists — skipping.")
                continue

            students = _get_students(cur, class_id)
            counts   = {}

            # ── EXAM WEEK: no attendance, no assignments, no quizzes ───────
            if is_exam_week:
                exam_type     = "midterm" if sem_week == MIDTERM_WEEK else "endterm"
                result_sem_wk = sem_week + RESULT_DELAY_WEEKS

                print(f"  [{class_id}] Sem {semester} Week {sem_week} "
                      f"({exam_type.upper()} EXAM — no class data)")

                # Exam results are published RESULT_DELAY_WEEKS later.
                # We store results immediately when the exam happens (the
                # result_date field carries the future publication date).
                exams   = _get_exam_schedule(cur, class_id, semester, sem_week)
                results = _build_exam_results(students, exams, class_id,
                                              semester, wdate + timedelta(weeks=RESULT_DELAY_WEEKS))
                if results:
                    psycopg2.extras.execute_batch(
                        cur,
                        """INSERT INTO exam_results
                           (schedule_id, student_id, class_id,
                            marks_obtained, max_marks, score_pct,
                            pass_fail, grade, result_date)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (schedule_id, student_id) DO NOTHING""",
                        results
                    )
                    counts["exam_results"] = len(results)

                summary["classes"][class_id] = counts
                continue   # no further rows for exam week

            # ── TEACHING WEEK ─────────────────────────────────────────────
            subjects = _get_subjects_for_sem(cur, class_id, semester)

            # Attendance
            att = _build_attendance(
                students, subjects, class_id, semester, sem_week, wdate)
            if att:
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO attendance
                       (student_id, class_id, subject_id, semester, week,
                        week_date, lectures_held, present, absent, late,
                        attendance_pct)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT DO NOTHING""",
                    att
                )
                counts["attendance"] = len(att)

            # Assignment submissions (for assignments due this week)
            assignments = _get_assignments_due(cur, class_id, semester, sem_week)
            subs = _build_assignment_submissions(
                cur, students, assignments, class_id, semester, sem_week)
            if subs:
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO assignment_submissions
                       (assignment_id, student_id, class_id, status,
                        submission_date, latency_hours, marks_obtained,
                        quality_pct, plagiarism_pct)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (assignment_id, student_id) DO NOTHING""",
                    subs
                )
                counts["assignment_submissions"] = len(subs)

            # Quiz submissions
            quizzes = _get_quizzes(cur, class_id, semester, sem_week)
            qzs = _build_quiz_submissions(
                students, quizzes, class_id, semester, sem_week)
            if qzs:
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO quiz_submissions
                       (quiz_id, student_id, class_id, attempted,
                        attempt_date, marks_obtained, score_pct)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (quiz_id, student_id) DO NOTHING""",
                    qzs
                )
                counts["quiz_submissions"] = len(qzs)

            # Library visits
            lib = _build_library_visits(
                students, class_id, semester, sem_week, wdate)
            if lib:
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO library_visits
                       (student_id, class_id, semester, week,
                        week_date, physical_visits)
                       VALUES (%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (student_id, semester, week) DO NOTHING""",
                    lib
                )
                counts["library_visits"] = len(lib)

            summary["classes"][class_id] = counts

        # ── Advance sim_state ─────────────────────────────────────────────
        cur.execute(
            """UPDATE sim_state
               SET current_week = %s, last_updated = NOW()
               WHERE id = 1""",
            (new_global,)
        )

        conn.commit()
        print(f"  Global week {new_global} "
              f"(sem week {sem_week}, {slot} semester) committed.")

    except Exception as e:
        conn.rollback()
        print(f"  ERROR — rolled back. {e}")
        raise
    finally:
        _sem_start_cache.clear()   # clear cache after each transaction
        cur.close()
        _release(conn)

    return summary


# ── PUBLIC: ROLLBACK ──────────────────────────────────────────────────────────
def rollback_to_week(target_week):
    """
    Delete all transactional data beyond target_week (global week) and reset
    sim_state.current_week to target_week.

    target_week = 0 wipes all transactional data (full reset).
    Wrapped in a single transaction — atomically succeeds or changes nothing.

    Note: exam_results are stored at the exam week (sem weeks 8 and 18 →
    global weeks 8, 18, 26, 36).  When rolling back we delete results whose
    exam global week > target_week.
    """
    if target_week < 0:
        raise ValueError("target_week cannot be negative.")

    conn = get_conn()
    conn.autocommit = False
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ── All reads happen inside the transaction ───────────────────────
        state    = _get_sim_state(cur)
        cur_week = state["current_week"]

        if target_week >= cur_week:
            raise ValueError(
                f"target_week ({target_week}) must be less than "
                f"current_week ({cur_week})."
            )

        print(f"  Rolling back global week {cur_week} → {target_week} ...")

        # Translate global target_week to within-semester context for each table.
        # Attendance / library / quizzes / assignments reference (semester, week)
        # so we need to know which semester weeks become "beyond" the target.

        # Weeks 1–18 → odd sem weeks 1–18; weeks 19–36 → even sem weeks 1–18
        target_sem_week, target_slot = _global_to_sem_week(max(target_week, 1)) \
            if target_week > 0 else (0, "odd")

        # ── Attendance ────────────────────────────────────────────────────
        # Delete rows whose (semester slot, sem_week) are beyond target.
        # Easier: use the global week translated to (semester, week) pairs.
        # Since all 4 classes advance together, we can just check week numbers.
        if target_week == 0:
            cur.execute("DELETE FROM attendance")
        elif target_week < WEEKS_PER_SEM:
            # Only in odd sems, weeks > target_sem_week
            cur.execute(
                """DELETE FROM attendance
                   WHERE week > %s
                      OR (week <= %s AND semester IN
                          (SELECT even_sem FROM classes))""",
                (target_sem_week, target_sem_week)
            )
        else:
            # Odd sem is fully committed; delete even-sem weeks > target_sem_week
            even_sem_week = target_week - WEEKS_PER_SEM
            cur.execute(
                """DELETE FROM attendance
                   WHERE semester IN (SELECT even_sem FROM classes)
                     AND week > %s""",
                (even_sem_week,)
            )
        deleted_att = cur.rowcount

        # ── Assignment submissions ─────────────────────────────────────────
        if target_week == 0:
            cur.execute("DELETE FROM assignment_submissions")
        elif target_week < WEEKS_PER_SEM:
            cur.execute(
                """DELETE FROM assignment_submissions sub
                   USING  assignment_definitions def
                   WHERE  sub.assignment_id = def.assignment_id
                     AND  (def.due_week > %s
                      OR   def.semester IN (SELECT even_sem FROM classes))""",
                (target_sem_week,)
            )
        else:
            even_sem_week = target_week - WEEKS_PER_SEM
            cur.execute(
                """DELETE FROM assignment_submissions sub
                   USING  assignment_definitions def
                   WHERE  sub.assignment_id = def.assignment_id
                     AND  def.semester IN (SELECT even_sem FROM classes)
                     AND  def.due_week > %s""",
                (even_sem_week,)
            )
        deleted_sub = cur.rowcount

        # ── Quiz submissions ───────────────────────────────────────────────
        if target_week == 0:
            cur.execute("DELETE FROM quiz_submissions")
        elif target_week < WEEKS_PER_SEM:
            cur.execute(
                """DELETE FROM quiz_submissions qs
                   USING  quiz_definitions qd
                   WHERE  qs.quiz_id = qd.quiz_id
                     AND  (qd.scheduled_week > %s
                      OR   qd.semester IN (SELECT even_sem FROM classes))""",
                (target_sem_week,)
            )
        else:
            even_sem_week = target_week - WEEKS_PER_SEM
            cur.execute(
                """DELETE FROM quiz_submissions qs
                   USING  quiz_definitions qd
                   WHERE  qs.quiz_id = qd.quiz_id
                     AND  qd.semester IN (SELECT even_sem FROM classes)
                     AND  qd.scheduled_week > %s""",
                (even_sem_week,)
            )
        deleted_qz = cur.rowcount

        # ── Library visits ─────────────────────────────────────────────────
        if target_week == 0:
            cur.execute("DELETE FROM library_visits")
        elif target_week < WEEKS_PER_SEM:
            cur.execute(
                """DELETE FROM library_visits
                   WHERE week > %s
                      OR semester IN (SELECT even_sem FROM classes)""",
                (target_sem_week,)
            )
        else:
            even_sem_week = target_week - WEEKS_PER_SEM
            cur.execute(
                """DELETE FROM library_visits
                   WHERE semester IN (SELECT even_sem FROM classes)
                     AND week > %s""",
                (even_sem_week,)
            )
        deleted_lib = cur.rowcount

        # ── Book borrows ───────────────────────────────────────────────────
        if target_week == 0:
            cur.execute("DELETE FROM book_borrows")
        elif target_week < WEEKS_PER_SEM:
            cur.execute(
                """DELETE FROM book_borrows
                   WHERE borrow_week > %s
                      OR semester IN (SELECT even_sem FROM classes)""",
                (target_sem_week,)
            )
        else:
            even_sem_week = target_week - WEEKS_PER_SEM
            cur.execute(
                """DELETE FROM book_borrows
                   WHERE semester IN (SELECT even_sem FROM classes)
                     AND borrow_week > %s""",
                (even_sem_week,)
            )
        deleted_brw = cur.rowcount

        # ── Exam results ───────────────────────────────────────────────────
        # Exam results stored at global weeks 8, 18, 26, 36 (exam weeks).
        # Delete results for any exam whose global week > target_week.
        if target_week == 0:
            cur.execute("DELETE FROM exam_results")
        elif target_week < MIDTERM_WEEK:
            cur.execute("DELETE FROM exam_results")
        elif target_week < ENDTERM_WEEK:
            # Keep only midterm (odd sem week 8); delete endterm and even sem
            cur.execute(
                """DELETE FROM exam_results er
                   USING  exam_schedule es
                   WHERE  er.schedule_id = es.schedule_id
                     AND  (es.exam_type = 'endterm'
                      OR   es.semester IN (SELECT even_sem FROM classes))"""
            )
        elif target_week < WEEKS_PER_SEM + MIDTERM_WEEK:
            # Odd sem complete; delete even sem results
            cur.execute(
                """DELETE FROM exam_results er
                   USING  exam_schedule es
                   WHERE  er.schedule_id = es.schedule_id
                     AND  es.semester IN (SELECT even_sem FROM classes)"""
            )
        elif target_week < WEEKS_PER_SEM + ENDTERM_WEEK:
            # Even sem midterm done; delete even sem endterm only
            cur.execute(
                """DELETE FROM exam_results er
                   USING  exam_schedule es
                   WHERE  er.schedule_id = es.schedule_id
                     AND  es.semester IN (SELECT even_sem FROM classes)
                     AND  es.exam_type = 'endterm'"""
            )
        # else: target_week >= 36 — nothing to delete
        deleted_ex = cur.rowcount

        # ── Update sim_state ──────────────────────────────────────────────
        cur.execute(
            "UPDATE sim_state SET current_week = %s, last_updated = NOW() WHERE id = 1",
            (target_week,)
        )

        conn.commit()

        result = {
            "from_week": cur_week,
            "to_week":   target_week,
            "deleted": {
                "attendance":             deleted_att,
                "assignment_submissions": deleted_sub,
                "quiz_submissions":       deleted_qz,
                "library_visits":         deleted_lib,
                "book_borrows":           deleted_brw,
                "exam_results":           deleted_ex,
            }
        }
        print(f"  Rollback complete.")
        for tbl, n in result["deleted"].items():
            if n:
                print(f"    {tbl:<28} {n} rows deleted")
        print(f"    sim_state reset to global week {target_week}")
        return result

    except Exception as e:
        conn.rollback()
        print(f"  ERROR — nothing was changed. {e}")
        raise
    finally:
        _sem_start_cache.clear()
        cur.close()
        _release(conn)


# ── PUBLIC: STATUS ────────────────────────────────────────────────────────────
def get_db_status():
    """
    Return a snapshot of the current simulation state.
    Used by the Streamlit UI status panel.
    Reads are non-transactional (read-only snapshot, no writes needed).
    """
    state      = query("SELECT current_week, sim_year FROM sim_state WHERE id = 1")[0]
    global_wk  = state["current_week"]
    sim_year   = state["sim_year"]

    sem_week, slot = _global_to_sem_week(max(global_wk, 1)) \
        if global_wk > 0 else (0, "odd")
    is_exam    = sem_week in EXAM_WEEKS

    classes    = query("SELECT class_id, year_of_study, odd_sem, even_sem FROM classes")
    events     = []
    row_counts = {}

    for cls in classes:
        cid      = cls["class_id"]
        odd_sem  = cls["odd_sem"]
        even_sem = cls["even_sem"]
        semester = odd_sem if slot == "odd" else even_sem

        sem_start = _sem_start(sim_year, semester)
        wdate     = _week_monday(sem_start, sem_week) if sem_week > 0 else None

        # What's scheduled at this week
        asn_n = query(
            """SELECT COUNT(*) AS n FROM assignment_definitions
               WHERE class_id=%s AND semester=%s AND due_week=%s""",
            (cid, semester, sem_week)
        )[0]["n"] if sem_week > 0 else 0

        qz_n = query(
            """SELECT COUNT(*) AS n FROM quiz_definitions
               WHERE class_id=%s AND semester=%s AND scheduled_week=%s""",
            (cid, semester, sem_week)
        )[0]["n"] if sem_week > 0 else 0

        ex_n = query(
            """SELECT COUNT(*) AS n FROM exam_schedule
               WHERE class_id=%s AND semester=%s AND scheduled_week=%s""",
            (cid, semester, sem_week)
        )[0]["n"] if is_exam else 0

        if asn_n: events.append(f"{cid} (sem {semester}): {asn_n} assignment(s) due")
        if qz_n:  events.append(f"{cid} (sem {semester}): {qz_n} quiz(zes)")
        if ex_n:  events.append(f"{cid} (sem {semester}): {'MIDTERM' if sem_week==MIDTERM_WEEK else 'ENDTERM'} EXAM")

        # Row counts for this class
        row_counts[cid] = {
            "attendance": query(
                "SELECT COUNT(*) AS n FROM attendance WHERE class_id=%s", (cid,)
            )[0]["n"],
            "assignment_submissions": query(
                """SELECT COUNT(*) AS n FROM assignment_submissions
                   WHERE class_id=%s""", (cid,)
            )[0]["n"],
            "quiz_submissions": query(
                "SELECT COUNT(*) AS n FROM quiz_submissions WHERE class_id=%s", (cid,)
            )[0]["n"],
            "exam_results": query(
                "SELECT COUNT(*) AS n FROM exam_results WHERE class_id=%s", (cid,)
            )[0]["n"],
        }

    _sem_start_cache.clear()

    return {
        "global_week":            global_wk,
        "semester_week":          sem_week,
        "semester_slot":          slot,
        "is_exam_week":           is_exam,
        "weeks_remaining":        TOTAL_WEEKS - global_wk,
        "sim_year":               sim_year,
        "events_at_current_week": events,
        "row_counts_by_class":    row_counts,
    }
