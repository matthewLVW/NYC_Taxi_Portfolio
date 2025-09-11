import streamlit as st
import duckdb
import pandas as pd
import altair as alt
from datetime import date, timedelta

# -------------------- CONFIG --------------------
DB_PATH = "db/warehouse.duckdb"
MAX_LOOKBACK = 90  # enforce no more than 90 days in memory

con = duckdb.connect(DB_PATH, read_only=True)
st.set_page_config(page_title="🚖 Executive Dashboard", layout="wide")

# -------------------- HELPERS --------------------
def load_df(query: str) -> pd.DataFrame:
    return con.execute(query).df()

def sparkline_chart(df, x, y, title=""):
    return alt.Chart(df).mark_line().encode(x=x, y=y).properties(height=150, title=title)

def bar_chart(df, x, y, title=""):
    return alt.Chart(df).mark_bar(size=24).encode(x=x, y=y).properties(height=300, title=title)

def pie_chart(df, theta, color, title=""):
    return alt.Chart(df).mark_arc().encode(theta=theta, color=color).properties(title=title)

def clamp(d, min_data, max_date):
    return max(min(d, max_date), min_data)

# -------------------- NAV --------------------
page = st.sidebar.radio("📑 Pages", ["Company Pulse", "Strategic Levers", "Zone Heat"])

# dataset bounds
min_date_bound = pd.to_datetime("2023-12-31").date()
max_date = con.execute("SELECT max(date_day) FROM main_marts.mart_kpi_daily").fetchone()[0]

def clamp_date(d):  # single clamp
    return max(min(d, max_date), min_date_bound)

# --- canonical state ---
if "today" not in st.session_state:
    st.session_state.today = clamp_date(max_date)
# initialize the widget's state *before* the widget is created
if "today_picker" not in st.session_state:
    st.session_state.today_picker = st.session_state.today

# ---- Month helpers (snap to month starts + clamp to month starts) ----
def month_start(d):
    return d.replace(day=1)

def prev_month_start(d):
    return date(d.year - 1, 12, 1) if d.month == 1 else date(d.year, d.month - 1, 1)

def next_month_start(d):
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)

def clamp_month_start(d):
    max_start = date(max_date.year, max_date.month, 1)
    min_start = date(min_date_bound.year, min_date_bound.month, 1)
    if d > max_date:
        return max_start
    if d < min_date_bound:
        return min_start
    return d

# arrows FIRST so we can safely set the widget's key before it's instantiated
col_a, col_b = st.sidebar.columns(2)
with col_a:
    if st.button("← Day"):
        new = clamp_date(st.session_state.today - timedelta(days=1))
        st.session_state.today = new
        st.session_state.today_picker = new
    if st.button("← Week"):
        new = clamp_date(st.session_state.today - timedelta(weeks=1))
        st.session_state.today = new
        st.session_state.today_picker = new
    if st.button("← Month"):
        t = st.session_state.today
        new = month_start(t) if t.day != 1 else prev_month_start(t)
        new = clamp_month_start(new)
        st.session_state.today = new
        st.session_state.today_picker = new
with col_b:
    if st.button("→ Day"):
        new = clamp_date(st.session_state.today + timedelta(days=1))
        st.session_state.today = new
        st.session_state.today_picker = new
    if st.button("→ Week"):
        new = clamp_date(st.session_state.today + timedelta(weeks=1))
        st.session_state.today = new
        st.session_state.today_picker = new
    if st.button("→ Month"):
        t = st.session_state.today
        new = next_month_start(t)  # next month start regardless of whether t is the 1st
        new = clamp_month_start(new)
        st.session_state.today = new
        st.session_state.today_picker = new

if st.button("Reset to Max Date"):
    st.session_state.today = max_date
    st.session_state.today_picker = max_date

# callback to keep canonical 'today' in sync when user changes the picker
def _sync_today_from_picker():
    st.session_state.today = clamp_date(st.session_state.today_picker)

# now create the picker (AFTER any state changes above)
st.sidebar.date_input(
    "📅 Select 'Today'",
    value=st.session_state.today,
    min_value=min_date_bound,
    max_value=max_date,
    key="today_picker",
    on_change=_sync_today_from_picker,
)

# single reference used everywhere below
today = st.session_state.today

# window selector
window_choice = st.sidebar.selectbox("📊 Reporting Window", ["Last 7 Days", "Last 30 Days", "Last 90 Days"])
window_map = {"Last 7 Days": 7, "Last 30 Days": 30, "Last 90 Days": 90}
window_days = window_map[window_choice]

# build ranges
current_start = today - timedelta(days=window_days - 1)
prev_start = current_start - timedelta(days=window_days)
prev_end = current_start - timedelta(days=1)

# =================================================
# PAGE 1: COMPANY PULSE
# =================================================
if page == "Company Pulse":
    st.header("📊 Company Pulse")

    kpi = load_df(f"""
        SELECT date_day, trips, gross_revenue, net_revenue, tips, tip_rate
        FROM main_marts.mart_kpi_daily
        WHERE date_day BETWEEN '{prev_start}' AND '{today}'
        ORDER BY date_day
    """)

    current  = kpi[kpi["date_day"].between(pd.to_datetime(current_start), pd.to_datetime(today))].copy()
    previous = kpi[kpi["date_day"].between(pd.to_datetime(prev_start),   pd.to_datetime(prev_end))].copy()

    for df in (current, previous):
        if df.empty:
            continue
        df["date_day"] = pd.to_datetime(df["date_day"]).dt.date
        for col in ("trips", "gross_revenue", "tip_rate"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["trips"] = df["trips"].fillna(0)
        df["gross_revenue"] = df["gross_revenue"].fillna(0.0)
        df["tip_rate"] = df["tip_rate"].fillna(df["tip_rate"].mean() if not df["tip_rate"].dropna().empty else 0)

    curr_trips_sum = int(current["trips"].sum()) if not current.empty else 0
    curr_rev_sum   = float(current["gross_revenue"].sum()) if not current.empty else 0.0
    curr_tip_rate  = float(current["tip_rate"].mean()) if not current.empty else 0.0
    curr_avg_ticket = (curr_rev_sum / curr_trips_sum) if curr_trips_sum else 0.0

    prev_trips_sum = int(previous["trips"].sum()) if not previous.empty else 0
    prev_rev_sum   = float(previous["gross_revenue"].sum()) if not previous.empty else 0.0
    prev_tip_rate  = float(previous["tip_rate"].mean()) if not previous.empty else 0.0
    prev_avg_ticket = (prev_rev_sum / prev_trips_sum) if prev_trips_sum else 0.0

    trips_delta   = ((curr_trips_sum - prev_trips_sum) / prev_trips_sum) if prev_trips_sum else 0.0
    rev_delta     = ((curr_rev_sum   - prev_rev_sum)   / prev_rev_sum)   if prev_rev_sum   else 0.0
    tip_delta     = (curr_tip_rate - prev_tip_rate) if previous.shape[0] else 0.0
    ticket_delta  = ((curr_avg_ticket - prev_avg_ticket) / prev_avg_ticket) if prev_avg_ticket else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🚕 Trips (window total)", f"{curr_trips_sum:,.0f}", f"{trips_delta:+.1%}" if window_days != 90 else None)
    c2.metric("💵 Gross Revenue (window)", f"${curr_rev_sum:,.0f}", f"{rev_delta:+.1%}" if window_days != 90 else None)
    c3.metric("🎟 Avg Ticket", f"${curr_avg_ticket:,.2f}", f"{ticket_delta:+.1%}" if window_days != 90 else None)
    c4.metric("💰 Tip Rate", f"{curr_tip_rate:.1%}", f"{tip_delta:+.1%}" if window_days != 90 else None)

    def day_bar(df, x_col, y_col, title, y_domain=None):
        enc_x = alt.X(f"{x_col}:T", title="Day", axis=alt.Axis(format="%b %d"))
        enc_y = alt.Y(f"{y_col}:Q",
                      title=None,
                      scale=alt.Scale(domain=y_domain) if y_domain else alt.Undefined)
        return (
            alt.Chart(df)
              .mark_bar(size=24)
              .encode(x=enc_x, y=enc_y,
                      tooltip=[alt.Tooltip(f"{x_col}:T", title="Date"),
                               alt.Tooltip(f"{y_col}:Q")])
              .properties(height=240, title=title)
        )

    trips_max = max(current["trips"].max() if not current.empty else 0,
                    previous["trips"].max() if not previous.empty else 0)
    rev_max   = max(current["gross_revenue"].max() if not current.empty else 0.0,
                    previous["gross_revenue"].max() if not previous.empty else 0.0)
    trips_domain = [0, trips_max] if trips_max else None
    rev_domain   = [0, rev_max]   if rev_max   else None

    cur_left, cur_right = st.columns(2)
    cur_left.altair_chart(
        day_bar(current, "date_day", "trips", "Trips — Current Window (daily)", y_domain=trips_domain),
        use_container_width=True
    )
    cur_right.altair_chart(
        day_bar(current, "date_day", "gross_revenue", "Gross Revenue — Current Window (daily)", y_domain=rev_domain),
        use_container_width=True
    )

    if window_days != 90:
        st.markdown(f"**Previous {window_choice}** — Baseline (same length)")
        prev_left, prev_right = st.columns(2)
        prev_left.altair_chart(
            day_bar(previous, "date_day", "trips", "Trips — Previous Window (daily)", y_domain=trips_domain),
            use_container_width=True
        )
        prev_right.altair_chart(
            day_bar(previous, "date_day", "gross_revenue", "Gross Revenue — Previous Window (daily)", y_domain=rev_domain),
            use_container_width=True
        )

    st.markdown("**⚠️ Alert Flags**")
    if window_days != 90:
        if trips_delta < -0.05:
            st.error(f"Trips down >5% vs prior {window_days}-day period ({trips_delta:+.1%}).")
        if rev_delta < -0.05:
            st.error(f"Revenue down >5% vs prior {window_days}-day period ({rev_delta:+.1%}).")
        if tip_delta < 0:
            st.error(f"Tip rate falling vs prior period ({tip_delta:+.1%}).")
    else:
        st.info("Monitoring 90-day trend only; alerts compare to prior period when a shorter window is selected.")

# =================================================
# PAGE 2: STRATEGIC LEVERS
# =================================================
if page == "Strategic Levers":
    st.header("🧭 What’s Driving Revenue?")

    pay_cur = load_df(f"""
        SELECT pm.date_day, pm.payment_type, pm.trips, pm.avg_ticket, pm.share, dp.payment_desc
        FROM main_marts.mart_payment_mix_daily pm
        LEFT JOIN main_gold.dim_payment dp USING (payment_type)
        WHERE pm.date_day BETWEEN '{current_start}' AND '{today}'
        ORDER BY pm.date_day, pm.payment_type
    """)
    pay_prev = load_df(f"""
        SELECT pm.date_day, pm.payment_type, pm.trips, pm.avg_ticket, pm.share, dp.payment_desc
        FROM main_marts.mart_payment_mix_daily pm
        LEFT JOIN main_gold.dim_payment dp USING (payment_type)
        WHERE pm.date_day BETWEEN '{prev_start}' AND '{prev_end}'
        ORDER BY pm.date_day, pm.payment_type
    """)
    kpi_cur = load_df(f"""
        SELECT date_day, trips, gross_revenue, net_revenue, tips, tip_rate
        FROM main_marts.mart_kpi_daily
        WHERE date_day BETWEEN '{current_start}' AND '{today}'
        ORDER BY date_day
    """)
    ap_cur = load_df(f"""
        SELECT date_day, SUM(total_revenue) AS airport_revenue, SUM(trips) AS airport_trips
        FROM main_marts.mart_airport_daily
        WHERE date_day BETWEEN '{current_start}' AND '{today}'
        GROUP BY 1
        ORDER BY 1
    """)
    vend_cur = load_df(f"""
        SELECT v.vendor_id, dv.vendor_name,
               COUNT(*)::BIGINT AS trips,
               SUM(total_amount) AS revenue,
               AVG(total_amount) AS avg_ticket,
               SUM(tip_amount)/NULLIF(SUM(total_amount),0) AS tip_rate
        FROM main_gold.fact_trips v
        LEFT JOIN main_gold.dim_vendor dv USING (vendor_id)
        WHERE v.date_day BETWEEN '{current_start}' AND '{today}'
        GROUP BY 1,2
        ORDER BY revenue DESC
    """)

    for d in (pay_cur, pay_prev, kpi_cur, ap_cur):
        if not d.empty and 'date_day' in d:
            d['date_day'] = pd.to_datetime(d['date_day']).dt.date

    def pmix_agg(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=['payment_desc','share','avg_ticket','trips'])
        g = df.groupby('payment_desc', as_index=False).agg(
            trips=('trips','sum'),
            avg_ticket=('avg_ticket', lambda x: pd.to_numeric(x, errors='coerce').mean(skipna=True)),
        )
        T = g['trips'].sum()
        g['share'] = g['trips'] / T if T else 0.0
        return g

    agg_cur  = pmix_agg(pay_cur)
    agg_prev = pmix_agg(pay_prev)

    all_pmt = sorted(set(agg_cur['payment_desc']).union(set(agg_prev['payment_desc'])))
    df1 = agg_cur.set_index('payment_desc').reindex(all_pmt).fillna(0)
    s1 = (df1['share']).fillna(0.0)
    p1 = (df1['avg_ticket']).fillna(0.0)
    T1 = int(kpi_cur['trips'].sum()) if not kpi_cur.empty else 0

    r1c1, r1c2 = st.columns([3, 2])

    if not pay_cur.empty:
        pay_mix = (pay_cur.groupby(['date_day','payment_desc'], as_index=False)
                        .agg(share=('share','mean')))
        mix_chart = (
            alt.Chart(pay_mix)
              .mark_bar()
              .encode(
                  x=alt.X('date_day:T', axis=alt.Axis(format='%b %d'), title='Day'),
                  y=alt.Y('share:Q', stack='normalize', title='Payment Mix (100%)'),
                  color=alt.Color('payment_desc:N', legend=alt.Legend(title='Payment Type'))
              )
              .properties(height=260, title='Payment Mix (current window)')
        )
        r1c1.altair_chart(mix_chart, use_container_width=True)
    else:
        r1c1.info("No payment-mix data in the selected window.")

    if not agg_cur.empty:
        guess_card = next((x for x in agg_cur['payment_desc'] if x and 'card' in x.lower()),
                          agg_cur.sort_values('share', ascending=False)['payment_desc'].iloc[0])
        guess_cash = next((x for x in agg_cur['payment_desc'] if x and 'cash' in x.lower()), None)

        r1c2.subheader("Sensitivity: Shift payment mix")
        shift_pp = r1c2.slider("Shift to card (pp)", min_value=-2.0, max_value=2.0, value=0.5, step=0.1)

        s_adj = s1.copy()
        if guess_card in s_adj.index:
            s_adj[guess_card] = max(0.0, min(1.0, s_adj[guess_card] + shift_pp/100.0))
            take_from = guess_cash if (guess_cash in s_adj.index) else s_adj.drop(index=[guess_card]).idxmax()
            s_adj[take_from] = max(0.0, s_adj[take_from] - shift_pp/100.0)
            s_adj = s_adj / s_adj.sum() if s_adj.sum() else s_adj

        R_base = T1 * float((s1 * p1).sum())
        R_sens = T1 * float((s_adj * p1).sum())
        r1c2.metric("Estimated Δ Net Revenue (mix only)", f"${(R_sens - R_base):,.0f}",
                    f"Shift {shift_pp:+.1f} pp → {guess_card}")
    else:
        r1c2.info("Not enough payment mix data for sensitivity.")

    r2c1, r2c2 = st.columns([3, 2])

    if not vend_cur.empty:
        vend_cur['share'] = vend_cur['trips'] / vend_cur['trips'].sum()
        vend_cur['revenue_per_trip'] = vend_cur['revenue'] / vend_cur['trips'].replace(0, pd.NA)

        vc = vend_cur.copy()
        vc = vc[vc['vendor_name'].notna()]
        vc = vc[~vc['vendor_name'].str.contains('unknown', case=False, na=False)]
        vc = vc[vc['trips'] > 0]
        vc = vc.sort_values('trips', ascending=False).head(2)

        r2c1.subheader("Vendor Scorecards")
        t1, t2 = r2c1.columns(2)

        def vendor_tile(col, name, share, rpt, tip):
            col.markdown(
                f"""
                <div style="padding:16px;border:1px solid #eaeaea;border-radius:14px;">
                <div style="font-weight:600;font-size:1.05rem;margin-bottom:8px;">{name}</div>
                <div style="display:flex;gap:20px;flex-wrap:wrap;">
                    <div>
                    <div style="font-size:.8rem;color:#666;">Trip Share</div>
                    <div style="font-size:1.35rem;font-weight:600;">{share:.1%}</div>
                    </div>
                    <div>
                    <div style="font-size:.8rem;color:#666;">Revenue / Trip</div>
                    <div style="font-size:1.35rem;font-weight:600;">${rpt:,.2f}</div>
                    </div>
                    <div>
                    <div style="font-size:.8rem;color:#666;">Tip Rate</div>
                    <div style="font-size:1.35rem;font-weight:600;">{tip:.1%}</div>
                    </div>
                </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        if len(vc) >= 1:
            r = vc.iloc[0]
            vendor_tile(t1, r['vendor_name'], float(r['share']), float(r['revenue_per_trip']), float(r['tip_rate'] or 0))
        if len(vc) >= 2:
            r = vc.iloc[1]
            vendor_tile(t2, r['vendor_name'], float(r['share']), float(r['revenue_per_trip']), float(r['tip_rate'] or 0))
        if len(vc) == 1:
            t2.info("Only one vendor available in this window after filtering unknown trips.")
        if len(vc) == 0:
            r2c1.info("No vendor data after filtering unknown trips.")
    else:
        r2c1.info("No vendor data in window.")

    total_rev = kpi_cur[['date_day','gross_revenue','trips']].rename(columns={'gross_revenue':'total_revenue'})
    ap_cur = ap_cur if 'ap_cur' in locals() else pd.DataFrame(columns=['date_day','airport_revenue','airport_trips'])
    ap_join = pd.merge(total_rev, ap_cur, on='date_day', how='left').fillna({'airport_revenue':0.0, 'airport_trips':0})
    ap_join['city_revenue'] = ap_join['total_revenue'] - ap_join['airport_revenue']
    apm = ap_join.melt(id_vars=['date_day'], value_vars=['airport_revenue','city_revenue'],
                    var_name='segment', value_name='revenue')
    bars = (
        alt.Chart(apm)
        .mark_bar()
        .encode(
            x=alt.X('date_day:T', axis=alt.Axis(format='%b %d'), title='Day'),
            y=alt.Y('revenue:Q', title='Revenue ($)'),
            color=alt.Color('segment:N', title=None,
                            scale=alt.Scale(domain=['airport_revenue','city_revenue'],
                                            range=['#6baed6','#9ecae1']))
        )
        .properties(height=220, title='Airport vs City Revenue (current window, daily)')
    )
    r2c2.altair_chart(bars, use_container_width=True)

    st.markdown("---")
    card_pp = None
    if not agg_cur.empty and not agg_prev.empty:
        card_lbl = next((x for x in agg_cur['payment_desc'] if x and 'card' in x.lower()),
                        agg_cur.sort_values('share', ascending=False)['payment_desc'].iloc[0])
        df0 = agg_prev.set_index('payment_desc').reindex(all_pmt).fillna(0)
        if card_lbl in df1.index and card_lbl in df0.index:
            card_pp = (df1.loc[card_lbl,'share'] - df0.loc[card_lbl,'share']) * 100.0

    c1, c2 = st.columns(2)
    if card_pp is not None:
        mix_impact = T1 * float(((df1['share'] - df0['share']).fillna(0) * p1).sum())
        c1.success(f"**Card adoption** {card_pp:+.1f} pp → "
                f"{'+' if mix_impact>=0 else ''}${mix_impact:,.0f} (mix impact).")
    else:
        c1.info("Card adoption change not available.")

    if not vend_cur.empty:
        med_rpt = vend_cur['revenue_per_trip'].median()
        vend_cur['gap'] = (vend_cur['revenue_per_trip'] - med_rpt) * vend_cur['trips']
        worst = vend_cur.sort_values('gap').iloc[0]
        c2.warning(f"**{worst['vendor_name']} underperformed** by ${abs(worst['gap']):,.0f} vs median unit economics.")
    else:
        c2.info("Vendor contribution not available.")

    st.caption("**Company Pulse** = Are we healthy?  •  **Levers** = Why, and what to pull?")

# =================================================
# PAGE 3: ZONE HEAT — Where to point the fleet
# =================================================
if page == "Zone Heat":
    st.header("🗺️ Zone Heat — Where to Point the Fleet")

    # ----------------------------- DATA (≤90 days) -----------------------------
    # Zone performance (daily) + labels
    zone_daily = load_df(f"""
        SELECT rz.date_day, rz.location_id, rz.trips, rz.total_revenue,
               dz.borough, dz.zone
        FROM main_marts.mart_revenue_daily_zone rz
        LEFT JOIN main_gold.dim_zone dz
          ON dz.location_id = rz.location_id
        WHERE rz.date_day BETWEEN '{current_start}' AND '{today}'
    """)

    # Day-of-week for heatmap skews
    dow = load_df(f"""
        SELECT date_day, day_name, iso_dow
        FROM main_main_gold.dim_date
        WHERE date_day BETWEEN '{current_start}' AND '{today}'
    """)

    # Anomaly counts (daily)
    anom_daily = load_df(f"""
        SELECT date_day, location_id, anomalies
        FROM main_marts.mart_anomaly_summary_daily
        WHERE date_day BETWEEN '{current_start}' AND '{today}'
    """)
    def coerce_date_col(df: pd.DataFrame, col="date_day"):
        if df is None or df.empty or col not in df:
            return df
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date  # -> Python date (object)
        return df

    zone_daily = coerce_date_col(zone_daily)
    dow        = coerce_date_col(dow)
    anom_daily = coerce_date_col(anom_daily)
    # Corridors (monthly OD) limited to months overlapping the window
    months = pd.period_range(pd.to_datetime(current_start), pd.to_datetime(today), freq='M')
    ym_list = [m.strftime("%Y-%m") for m in months] or []
    od_df = pd.DataFrame()
    if ym_list:
        ym_sql = ", ".join([f"'{m}'" for m in ym_list])
        od_df = load_df(f"""
            SELECT
                od.year_month,
                od.pu_location_id,
                od.do_location_id,
                od.trips,
                od.revenue,
                dpu.zone AS pu_zone,
                dpu.borough AS pu_borough,
                ddo.zone AS do_zone,
                ddo.borough AS do_borough
            FROM main_marts.mart_od_matrix_monthly AS od
            LEFT JOIN main_gold.dim_zone AS dpu ON dpu.location_id = od.pu_location_id
            LEFT JOIN main_gold.dim_zone AS ddo ON ddo.location_id = od.do_location_id
            WHERE od.year_month IN ({ym_sql})
        """)

    # ----------------------------- GUARD RAILS -----------------------------
    if zone_daily.empty:
        st.info("No zone data in the selected window.")
    else:
        zone_daily["date_day"] = pd.to_datetime(zone_daily["date_day"]).dt.date
        zone_daily["trips"] = pd.to_numeric(zone_daily["trips"], errors="coerce").fillna(0)
        zone_daily["total_revenue"] = pd.to_numeric(zone_daily["total_revenue"], errors="coerce").fillna(0.0)

        # ============================= ROW 1: Top Zones (impact) =============================
        c1, c2 = st.columns([3, 2])

        z_agg = (zone_daily.groupby(["location_id","borough","zone"], as_index=False)
                           .agg(trips=("trips","sum"),
                                revenue=("total_revenue","sum")))
        z_agg["avg_ticket"] = z_agg["revenue"] / z_agg["trips"].replace(0, pd.NA)
        total_rev_window = z_agg["revenue"].sum()
        z_agg["rev_share"] = z_agg["revenue"] / total_rev_window if total_rev_window else 0.0

        topN = z_agg.sort_values("revenue", ascending=False).head(12).copy()
        topN["label"] = topN.apply(lambda r: f"{r['borough']} — {r['zone']}", axis=1)

        top_chart = (
            alt.Chart(topN)
              .mark_bar(size=22)
              .encode(
                  y=alt.Y("label:N", sort='-x', title=None),
                  x=alt.X("revenue:Q", title="Revenue ($)"),
                  tooltip=[
                      alt.Tooltip("label:N", title="Zone"),
                      alt.Tooltip("revenue:Q", title="Revenue", format=",.0f"),
                      alt.Tooltip("trips:Q", title="Trips", format=",.0f"),
                      alt.Tooltip("avg_ticket:Q", title="Avg Ticket", format=",.2f"),
                      alt.Tooltip("rev_share:Q", title="Share", format=".1%")
                  ]
              )
              .properties(height=22 * len(topN) + 30, title="Top Zones by Revenue (window)")
        )
        c1.altair_chart(top_chart, use_container_width=True)

        # Day-of-week skew by borough (heatmap)
        if not dow.empty:
            zd = zone_daily.merge(dow, on="date_day", how="left")
            skew = (zd.groupby(["borough","day_name"], as_index=False)
                      .agg(revenue=("total_revenue","sum")))
            # Normalize within borough so exec sees skew vs that borough's average
            skew["pct_of_borough"] = skew.groupby("borough")["revenue"].transform(lambda s: s / s.sum())
            heat = (
                alt.Chart(skew)
                  .mark_rect()
                  .encode(
                      x=alt.X("day_name:N", sort=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], title=None),
                      y=alt.Y("borough:N", title=None),
                      color=alt.Color("pct_of_borough:Q", title="Revenue Mix", legend=alt.Legend(format=".0%")),
                      tooltip=[
                          alt.Tooltip("borough:N"),
                          alt.Tooltip("day_name:N", title="Day"),
                          alt.Tooltip("revenue:Q", title="Revenue", format=",.0f"),
                          alt.Tooltip("pct_of_borough:Q", title="Mix", format=".0%")
                      ]
                  )
                  .properties(height=220, title="Day-of-Week Revenue Skew by Borough (normalized)")
            )
            c2.altair_chart(heat, use_container_width=True)
        else:
            c2.info("No calendar metadata available for day-of-week skew.")

        # ============================= ROW 2: Corridors (OD) =============================
        st.markdown("---")
        st.subheader("Priority Corridors (Origin → Destination)")
        if od_df.empty:
            st.info("No OD matrix data for months overlapping the selected window.")
        else:
            # focus on corridors connected to the top pickup zones
            top_pu_ids = topN["location_id"].tolist()
            odf = od_df[od_df["pu_location_id"].isin(top_pu_ids)].copy()

            # keep the strongest 20 corridors by revenue across months
            odf_grp = (odf.groupby(["pu_location_id","do_location_id","pu_zone","do_zone","pu_borough","do_borough"], as_index=False)
                          .agg(trips=("trips","sum"), revenue=("revenue","sum")))
            odf_grp["corridor"] = odf_grp.apply(lambda r: f"{r['pu_borough']} — {r['pu_zone']} → {r['do_borough']} — {r['do_zone']}", axis=1)
            top_corr = odf_grp.sort_values("revenue", ascending=False).head(20)

            corr_chart = (
                alt.Chart(top_corr)
                  .mark_bar(size=22)
                  .encode(
                      y=alt.Y("corridor:N", sort='-x', title=None),
                      x=alt.X("revenue:Q", title="Revenue ($)"),
                      tooltip=[
                          alt.Tooltip("corridor:N", title="Corridor"),
                          alt.Tooltip("revenue:Q", title="Revenue", format=",.0f"),
                          alt.Tooltip("trips:Q", title="Trips", format=",.0f")
                      ]
                  )
                  .properties(height=22 * len(top_corr) + 30, title="Top OD Corridors by Revenue (months in window)")
            )
            st.altair_chart(corr_chart, use_container_width=True)

        # ============================= ROW 3: Hotspots (risk) =============================
        st.markdown("---")
        st.subheader("Hotspots — Highest Anomaly Rates")

        if not anom_daily.empty:
            anom_daily["date_day"] = pd.to_datetime(anom_daily["date_day"]).dt.date
            a = (anom_daily.groupby("location_id", as_index=False)
                           .agg(anomalies=("anomalies","sum")))
            zr = z_agg[["location_id","borough","zone","trips"]].copy()
            hot = a.merge(zr, on="location_id", how="left")
            hot["anomaly_rate"] = hot["anomalies"] / hot["trips"].replace(0, pd.NA)
            hot = hot[(hot["trips"] >= 100)].copy()  # ignore tiny denominators
            hot = hot.sort_values("anomaly_rate", ascending=False).head(8)
            hot["label"] = hot.apply(lambda r: f"{r['borough']} — {r['zone']}", axis=1)

            hot_bar = (
                alt.Chart(hot)
                .mark_bar(size=22)
                .encode(
                    y=alt.Y("label:N", sort='-x', title=None),
                    x=alt.X("anomaly_rate:Q",
                            title="Anomaly Rate (%)",
                            axis=alt.Axis(format=".2%")),   # ✅ show as percentage
                    tooltip=[
                        alt.Tooltip("label:N", title="Zone"),
                        alt.Tooltip("anomalies:Q", title="Anomalies", format=",.0f"),
                        alt.Tooltip("trips:Q", title="Trips", format=",.0f"),
                        alt.Tooltip("anomaly_rate:Q", title="Rate", format=".2%")
                    ]
                )
                .properties(height=22 * len(hot) + 30,
                            title="Zones with Highest Anomaly Rates (min 100 trips)")
            )
            st.altair_chart(hot_bar, use_container_width=True)
        else:
            st.info("No anomaly data in the selected window.")

    # Executive crib note
    st.caption("**Zone Heat** = Where the money and risks cluster. Use with vendor levers and airport split to direct supply and quality actions.")
