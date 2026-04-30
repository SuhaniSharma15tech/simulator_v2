import os
import requests
import streamlit as st
from datetime import datetime
from db_writer2 import advance_week, rollback_to_week, get_db_status
from connection import query as db_query

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EduMetrics Simulator",
    page_icon="🗓️",
    layout="wide",
)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
TOTAL_WEEKS   = 36
WEEKS_PER_SEM = 18

# Milestones keyed by GLOBAL week number
MILESTONES = {
    8:  "Midterm (Odd)",
    18: "Endterm (Odd)",
    26: "Midterm (Even)",
    36: "Endterm (Even)",
}

# ── SESSION STATE ─────────────────────────────────────────────────────────────
if "log" not in st.session_state:
    st.session_state.log = []

if "confirm_reset" not in st.session_state:
    st.session_state.confirm_reset = False

if "confirm_rollback" not in st.session_state:
    st.session_state.confirm_rollback = None   # stores target global week when pending


# ── CALIBRATION TRIGGER ───────────────────────────────────────────────────────
def _trigger_calibration():
    """
    POST to the calibrate Flask service on the analysis Railway service.
    Blocks until calibrate() has fully committed — st.rerun() is only called
    after this returns, so the UI always reflects up-to-date analysis data.

    On failure (service down, timeout, etc.) a warning is shown but the
    simulator keeps working — the analysis DB will just be behind until
    the next successful trigger.
    """
    url    = os.getenv("CALIBRATE_SERVICE_URL")
    secret = os.getenv("INTERNAL_SECRET")

    if not url:
        # Local dev without the analysis service running — skip silently.
        add_log("Calibration skipped (CALIBRATE_SERVICE_URL not set).", log_type="system")
        return None

    try:
        resp = requests.post(
            f"{url}/calibrate",
            headers={"X-Internal-Secret": secret or ""},
            timeout=120,   # multi-week jumps can take a while
        )
        resp.raise_for_status()
        result = resp.json()
        action = result.get("action", "unknown")
        weeks  = result.get("weeks_processed", 0)
        ms     = result.get("elapsed_ms", 0)
        add_log(
            f"Analysis DB calibrated — {action}"
            + (f", {weeks} week(s) processed" if weeks else "")
            + f"  ({ms} ms)",
            log_type="system",
        )
        return result
    except requests.exceptions.Timeout:
        st.warning("Calibration service timed out — analysis DB may be behind.")
        add_log("Calibration timed out.", log_type="system")
        return None
    except Exception as e:
        st.warning(f"Calibration service unreachable: {e}")
        add_log(f"Calibration failed: {e}", log_type="system")
        return None


# ── HELPERS ───────────────────────────────────────────────────────────────────
def global_to_sem_week(global_week):
    """Map global week (1–36) → (sem_week 1–18, slot 'odd'/'even')."""
    if global_week <= WEEKS_PER_SEM:
        return global_week, "odd"
    return global_week - WEEKS_PER_SEM, "even"


def clean_event(event_str):
    """
    'YEAR1 (sem 1): 2 assignment(s) due'  →  '2 assignment(s) due (Sem 1, Y1)'
    Falls back to the raw string if the format doesn't match.
    """
    try:
        head, detail = event_str.split(": ", 1)
        cls_part = head.split(" (sem ")[0].strip()
        sem_part  = head.split(" (sem ")[1].rstrip(")")
        return f"{detail} (Sem {sem_part}, {cls_part})"
    except Exception:
        return event_str


def add_log(text, week=None, log_type="analysis"):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.log.insert(0, {
        "ts":   ts,
        "text": text,
        "week": week,
        "type": log_type,
    })
    if len(st.session_state.log) > 80:
        st.session_state.log.pop()


def get_events_for_global_week(global_week):
    """
    Return a list of short event strings for a given global week.
    Used for timeline button tooltips and log messages.
    """
    sem_week, slot = global_to_sem_week(max(global_week, 1))
    events  = []
    classes = db_query("SELECT class_id, odd_sem, even_sem FROM classes")

    for cls in classes:
        cid      = cls["class_id"]
        semester = cls["odd_sem"] if slot == "odd" else cls["even_sem"]
        short    = f"S{semester}"

        asn = db_query(
            """SELECT COUNT(*) AS n FROM assignment_definitions
               WHERE class_id=%s AND semester=%s AND due_week=%s""",
            (cid, semester, sem_week)
        )[0]["n"]
        qz = db_query(
            """SELECT COUNT(*) AS n FROM quiz_definitions
               WHERE class_id=%s AND semester=%s AND scheduled_week=%s""",
            (cid, semester, sem_week)
        )[0]["n"]
        ex = db_query(
            """SELECT COUNT(*) AS n FROM exam_schedule
               WHERE class_id=%s AND semester=%s AND scheduled_week=%s""",
            (cid, semester, sem_week)
        )[0]["n"]

        if asn: events.append(f"{asn} asn due ({short})")
        if qz:  events.append(f"{qz} quiz ({short})")
        if ex:  events.append(f"EXAM ({short})")

    return events


def has_events_at(global_week):
    return bool(get_events_for_global_week(global_week))


# ── FETCH CURRENT STATUS ──────────────────────────────────────────────────────
status       = get_db_status()
current_week = status["global_week"]
sem_week     = status["semester_week"]
sem_slot     = status["semester_slot"]
is_exam_week = status["is_exam_week"]
sim_year     = status["sim_year"]
raw_events   = status["events_at_current_week"]
events       = [clean_event(e) for e in raw_events]

# Weeks that have attendance data (used to colour the timeline green)
analysed_weeks = []
for w in range(1, TOTAL_WEEKS + 1):
    sw, slot = global_to_sem_week(w)
    rows = db_query(
        """SELECT COUNT(*) AS n FROM attendance att
           JOIN classes cls ON att.class_id = cls.class_id
           WHERE att.week = %s
             AND att.semester = CASE WHEN %s = 'odd' THEN cls.odd_sem
                                     ELSE cls.even_sem END
           LIMIT 1""",
        (sw, slot)
    )
    if rows[0]["n"] > 0:
        analysed_weeks.append(w)

last_analysed = max(analysed_weeks) if analysed_weeks else 0
pending_count = current_week - last_analysed


# ── HEADER ────────────────────────────────────────────────────────────────────
header_col, reset_col = st.columns([4, 1])
with header_col:
    st.subheader("EduMetrics — DB Simulator")
    st.caption(
        f"Dev mode only — simulates the college database state week by week  "
        f"| Sim year: **{sim_year}**"
    )
with reset_col:
    if st.button("Reset to Week 0", type="secondary", use_container_width=True):
        st.session_state.confirm_reset = True


# ── RESET CONFIRMATION ────────────────────────────────────────────────────────
if st.session_state.confirm_reset:
    with st.container(border=True):
        st.warning(
            f"This will delete ALL transactional data (weeks 1–{current_week}) "
            "and reset sim_state to global week 0. This cannot be undone."
        )
        c1, c2, _ = st.columns([1, 1, 4])
        if c1.button("Yes, reset", type="primary"):
            try:
                rollback_to_week(0)
                _trigger_calibration()
                add_log(
                    "DB reset to week 0 — all transactional data cleared.",
                    log_type="rollback",
                )
                st.session_state.confirm_reset = False
                st.rerun()
            except Exception as e:
                st.error(f"Reset failed: {e}")
        if c2.button("Cancel"):
            st.session_state.confirm_reset = False
            st.rerun()


# ── ROLLBACK CONFIRMATION ─────────────────────────────────────────────────────
if st.session_state.confirm_rollback is not None:
    target = st.session_state.confirm_rollback
    t_sw, t_slot = global_to_sem_week(max(target, 1))
    with st.container(border=True):
        st.warning(
            f"Roll back to **Global Week {target}** "
            f"(Sem-week {t_sw}, {t_slot} semester)? "
            f"All data from week {target + 1} to week {current_week} "
            "will be permanently deleted from the database."
        )
        c1, c2, _ = st.columns([1, 1, 4])
        if c1.button("Yes, roll back", type="primary"):
            try:
                result  = rollback_to_week(target)
                deleted = sum(result["deleted"].values())
                _trigger_calibration()
                add_log(
                    f"Rolled back week {current_week} → week {target}. "
                    f"{deleted} rows deleted.",
                    week=target,
                    log_type="rollback",
                )
                st.session_state.confirm_rollback = None
                st.rerun()
            except Exception as e:
                st.error(f"Rollback failed: {e}")
        if c2.button("Cancel", key="cancel_rb"):
            st.session_state.confirm_rollback = None
            st.rerun()


# ── STAT CARDS ────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Global week",      current_week)
c2.metric("Semester week",    sem_week if current_week > 0 else "—")
c3.metric("Semester",         f"{sem_slot.title()}" if current_week > 0 else "—")
c4.metric("Last analysed",    last_analysed if last_analysed else "—")
c5.metric("Pending analysis", pending_count)


# ── CURRENT STATE ─────────────────────────────────────────────────────────────
with st.container(border=True):
    st.write("**CURRENT STATE**")

    milestone   = MILESTONES.get(current_week, "")
    state_label = f"G-W{current_week} / S-W{sem_week} ({sem_slot})" \
                  if current_week > 0 else "Pre-semester"
    if milestone:
        state_label += f"  ·  {milestone}"
    if is_exam_week:
        state_label += "  ·  📝 Exam week"

    st.markdown(f"### {state_label}")

    if events:
        cols = st.columns(min(len(events), 4))
        for i, ev in enumerate(events):
            cols[i % len(cols)].info(ev)
    elif current_week == 0:
        st.caption("No data yet — click a week below to begin.")
    elif is_exam_week:
        st.caption("Exam week — no attendance / assignments / quizzes generated.")
    else:
        st.caption("Regular week — attendance only.")


# ── SEMESTER TIMELINE ─────────────────────────────────────────────────────────
with st.container(border=True):
    st.write("**SEMESTER TIMELINE — CLICK ANY WEEK**")

    lc1, lc2, lc3, lc4 = st.columns([1, 1, 1, 4])
    lc1.markdown("🟦 Current")
    lc2.markdown("🟩 Analysed")
    lc3.markdown("• Has events")

    def _render_week_buttons(week_range):
        cols = st.columns(WEEKS_PER_SEM)
        for i, w in enumerate(week_range):
            is_current   = (w == current_week)
            is_milestone = (w in MILESTONES)
            has_ev       = has_events_at(w)

            dot      = " •" if has_ev else ""
            label    = f"**{w}**{dot}" if is_milestone else f"{w}{dot}"
            btn_type = "primary" if is_current else "secondary"

            tip_parts = []
            if is_milestone:
                tip_parts.append(MILESTONES[w])
            tip_parts.extend(get_events_for_global_week(w))
            tip = " · ".join(tip_parts) if tip_parts else f"Global week {w}"

            if cols[i].button(
                label,
                key=f"week_btn_{w}",
                type=btn_type,
                use_container_width=True,
                help=tip,
            ):
                if w < current_week:
                    st.session_state.confirm_rollback = w
                    st.rerun()
                elif w == current_week:
                    pass
                else:
                    try:
                        for step in range(w - current_week):
                            target_w = current_week + step + 1
                            advance_week()
                            ev_list = get_events_for_global_week(target_w)
                            ev_str  = ", ".join(ev_list) if ev_list else "Attendance logged"
                            add_log(
                                f"Advanced G-W{target_w} [{ev_str}]",
                                week=target_w,
                                log_type="analysis",
                            )
                        # All weeks committed — now trigger calibration once
                        _trigger_calibration()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not advance: {e}")

    # ── Odd semester (global weeks 1–18) ─────────────────────────────────────
    st.caption("**Odd semester** (weeks 1–18)")
    _render_week_buttons(range(1, WEEKS_PER_SEM + 1))

    # ── Even semester (global weeks 19–36) ───────────────────────────────────
    st.caption("**Even semester** (weeks 19–36)")
    _render_week_buttons(range(WEEKS_PER_SEM + 1, TOTAL_WEEKS + 1))

    # Milestone labels below the rows
    ml_cols_odd  = st.columns(WEEKS_PER_SEM)
    ml_cols_even = st.columns(WEEKS_PER_SEM)
    for gw, label in MILESTONES.items():
        if gw <= WEEKS_PER_SEM:
            ml_cols_odd[gw - 1].caption(f"W{gw}: {label}")
        else:
            ml_cols_even[gw - WEEKS_PER_SEM - 1].caption(f"W{gw}: {label}")


# ── QUICK JUMP ────────────────────────────────────────────────────────────────
with st.container(border=True):
    st.write("**QUICK JUMP**")
    jc1, jc2, jc3, jc4 = st.columns([2, 1, 1, 1])

    jump_target = jc1.number_input(
        "Go to global week",
        min_value=0, max_value=TOTAL_WEEKS,
        value=min(current_week + 1, TOTAL_WEEKS),
        label_visibility="collapsed",
    )

    if jc2.button("Jump", use_container_width=True):
        if jump_target == current_week:
            st.info("Already at this week.")
        elif jump_target < current_week:
            st.session_state.confirm_rollback = jump_target
            st.rerun()
        else:
            try:
                for step in range(jump_target - current_week):
                    target_w = current_week + step + 1
                    advance_week()
                    ev_list = get_events_for_global_week(target_w)
                    ev_str  = ", ".join(ev_list) if ev_list else "Attendance logged"
                    add_log(
                        f"Advanced G-W{target_w} [{ev_str}]",
                        week=target_w,
                        log_type="analysis",
                    )
                # All weeks committed — trigger calibration once
                _trigger_calibration()
                st.rerun()
            except Exception as e:
                st.error(f"Jump failed: {e}")

    if current_week < TOTAL_WEEKS:
        if jc3.button(f"Next (G-W{current_week + 1})", use_container_width=True):
            try:
                advance_week()
                new_w   = current_week + 1
                ev_list = get_events_for_global_week(new_w)
                ev_str  = ", ".join(ev_list) if ev_list else "Attendance logged"
                add_log(
                    f"Advanced G-W{new_w} [{ev_str}]",
                    week=new_w,
                    log_type="analysis",
                )
                _trigger_calibration()
                st.rerun()
            except Exception as e:
                st.error(f"Could not advance: {e}")

    if current_week > 0:
        if jc4.button(f"Back to G-W{current_week - 1}", use_container_width=True):
            st.session_state.confirm_rollback = current_week - 1
            st.rerun()


# ── ANALYSIS & EVENT LOG ──────────────────────────────────────────────────────
with st.container(border=True):
    log_col, clear_col = st.columns([5, 1])
    log_col.write("**ANALYSIS & EVENT LOG**")
    if clear_col.button("Clear", use_container_width=True):
        st.session_state.log = []
        st.rerun()

    if not st.session_state.log:
        st.caption("No activity yet — click a week to begin.")
    else:
        dot_color = {"analysis": "🟢", "rollback": "🔴", "system": "⚪"}
        for entry in st.session_state.log:
            dot        = dot_color.get(entry["type"], "⚪")
            week_label = f"**G-W{entry['week']}**" if entry.get("week") is not None else ""
            ts         = entry["ts"]
            text       = entry["text"]

            col_ts, col_dot, col_text = st.columns([1, 0.2, 6])
            col_ts.caption(ts)
            col_dot.write(dot)
            if week_label:
                col_text.markdown(f"{week_label} — {text}")
            else:
                col_text.markdown(text)
