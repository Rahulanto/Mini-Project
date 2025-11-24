import pandas as pd
import streamlit as st
import mysql.connector
from sqlalchemy import create_engine, text
# Corrected DB connection string
DATABASE_URL = "mysql+pymysql://root:Rahul%4098@localhost:3306/Police1"

# DB connection
engine = create_engine(DATABASE_URL)

def run_query(sql: str) -> pd.DataFrame:
    """Run a SELECT query and return a DataFrame."""
    try:
        with engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn)
    except Exception as e:
        st.error(f"❌ Error running query:\n{e}")
        return pd.DataFrame()

def beautify_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Convert headers to Title Case without underscores."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(col).replace("_", " ").title() for col in df.columns]
    return df
# ----------------- Tabs in streamlit dashboard -----------------
tabs = st.tabs([
    "🚗 Vehicle-Based",
    "🧍 Demographic-Based",
    "🕒 Time & Duration Based",
    "⚖️ Violation-Based",
    "🌍 Location-Based",
    "🧩 Miscellaneous Reports"
])
# ======================================================
# 🚗 VEHICLE-BASED TAB
# ======================================================
with tabs[0]:
    st.header("🚗 Vehicle-Based Reports")
 
    st.subheader("1️⃣ Top 10 Vehicle Numbers Involved In Drug-Related Stops")
    q1 = """

SELECT 
    vehicle_number,
    COUNT(*) AS drug_related_stop_count
FROM traffic_data
WHERE LOWER(drugs_related_stop) IN ('true', 'yes')
GROUP BY vehicle_number
ORDER BY drug_related_stop_count DESC
LIMIT 10;

"""
    st.dataframe(beautify_headers(run_query(q1)), use_container_width=True)
 
    st.subheader("2️⃣ Vehicles Most Frequently Searched")
    q2 = """

SELECT 
    vehicle_number,
    COUNT(*) AS search_count
FROM traffic_data
WHERE LOWER(search_conducted) IN ('yes', 'true')
GROUP BY vehicle_number
ORDER BY search_count DESC
LIMIT 10;

"""
    st.dataframe(beautify_headers(run_query(q2)), use_container_width=True)
 
# ======================================================
# 🧍 DEMOGRAPHIC-BASED TAB
# ======================================================
with tabs[1]:
    st.header("🧍 Demographic-Based Reports")
 
    st.subheader("1️⃣ Which Driver Age Group Had The Highest Arrest Rate?")
    q3 = """
SELECT
    CASE 
        WHEN driver_age IS NULL THEN 'unknown'
        WHEN driver_age < 18 THEN '<18'
        WHEN driver_age BETWEEN 18 AND 24 THEN '18-24'
        WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
        WHEN driver_age BETWEEN 35 AND 44 THEN '35-44'
        WHEN driver_age BETWEEN 45 AND 54 THEN '45-54'
        WHEN driver_age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
FROM traffic_data
GROUP BY age_group
ORDER BY arrest_rate_pct DESC;
"""
    st.dataframe(beautify_headers(run_query(q3)), use_container_width=True)
 
    st.subheader("2️⃣ Gender Distribution Of Drivers Stopped In Each Country")
    q4 = """
SELECT
  country_name,
  driver_gender,
  COUNT(*) AS stops,
  ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY country_name),0), 2) AS pct_of_country
FROM traffic_data
GROUP BY country_name, driver_gender
ORDER BY country_name, stops DESC;
"""
    st.dataframe(beautify_headers(run_query(q4)), use_container_width=True)
 
    st.subheader("3️⃣ Race & Gender Combination With The Highest Search Rate (Min 5 Stops)")
    q5 = """
SELECT
    driver_race, 
    driver_gender, 
    COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) AS searches,
    ROUND(100.0 * SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS search_rate_pct
FROM traffic_data
GROUP BY driver_race, driver_gender
HAVING COUNT(*) >= 5
ORDER BY search_rate_pct DESC;
"""
    st.dataframe(beautify_headers(run_query(q5)), use_container_width=True)
 
# ======================================================
# 🕒 TIME & DURATION-BASED TAB
# ======================================================
with tabs[2]:
    st.header("🕒 Time & Duration-Based Reports")
 
    st.subheader("1️⃣ What Time Of Day Sees The Most Traffic Stops?")
    q6 = """
SELECT
    CASE
        WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 0 AND 5 THEN 'Late Night (00:00–05:59)'
        WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 6 AND 11 THEN 'Morning (06:00–11:59)'
        WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 12 AND 17 THEN 'Afternoon (12:00–17:59)'
        ELSE 'Evening (18:00–23:59)'
    END AS time_of_day,
    COUNT(*) AS total_stops
FROM traffic_data 
WHERE stop_time IS NOT NULL
GROUP BY time_of_day
ORDER BY time_of_day;
"""
    st.dataframe(beautify_headers(run_query(q6)), use_container_width=True)
 
    st.subheader("2️⃣ Average Stop Duration For Different Violations (By Country)")
    q7 = """
SELECT
    country_name, 
    violation, 
    ROUND(AVG(
        CASE 
            WHEN stop_duration LIKE '%0-15%' THEN 15
            WHEN stop_duration LIKE '%16-30%' THEN 30
            WHEN stop_duration LIKE '%30%' THEN 60
            ELSE NULL
        END
    ), 2) AS avg_duration_minutes,
    COUNT(*) AS total_stops
FROM traffic_data
WHERE stop_duration IS NOT NULL
GROUP BY country_name, violation
HAVING COUNT(*) >= 3
ORDER BY country_name, avg_duration_minutes DESC;
"""
    st.dataframe(beautify_headers(run_query(q7)), use_container_width=True)
 
    st.subheader("3️⃣ Are Stops During The Night More Likely To Lead To Arrests?")
    q8 = """
SELECT
    country_name,
    CASE 
        WHEN EXTRACT(HOUR FROM stop_time) BETWEEN 6 AND 19 THEN 'Day'
        ELSE 'Night'
    END AS period,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
FROM traffic_data 
WHERE stop_time IS NOT NULL
GROUP BY country_name, period
ORDER BY country_name, period;
"""
    st.dataframe(beautify_headers(run_query(q8)), use_container_width=True)
 
# ======================================================
# ⚖️ VIOLATION-BASED TAB
# ======================================================
with tabs[3]:
    st.header("⚖️ Violation-Based Reports")
 
    st.subheader("1️⃣ Which Violations Are Most Associated With Searches Or Arrests?")
    q9 = """
SELECT
  violation,
  COUNT(*) AS total_occurrences,
  SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) AS searches,
  SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS arrests,
  ROUND(100.0 * SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS search_rate_pct,
  ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS arrest_rate_pct
FROM traffic_data
GROUP BY violation
ORDER BY arrests DESC, searches DESC;
"""
    st.dataframe(beautify_headers(run_query(q9)), use_container_width=True)
 
    st.subheader("2️⃣ Which Violations Are Most Common Among Younger Drivers (<25)?")
    q10 = """
SELECT
  violation,
  COUNT(*) AS young_driver_count
FROM traffic_data
WHERE driver_age IS NOT NULL
  AND driver_age < 25
GROUP BY violation
ORDER BY young_driver_count DESC
LIMIT 10;
"""
    st.dataframe(beautify_headers(run_query(q10)), use_container_width=True)
 
    st.subheader("3️⃣ Violations That Rarely Result In Search Or Arrest")
    q11 = """
SELECT * FROM (
    SELECT 
        violation, 
        COUNT(*) AS total_occurrences,
        SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) AS searches,
        SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS arrests,
        ROUND(100.0 * SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS search_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
    FROM traffic_data
    GROUP BY violation
) AS sub
ORDER BY (sub.search_rate_pct + sub.arrest_rate_pct) IS NULL, 
         (sub.search_rate_pct + sub.arrest_rate_pct) ASC
LIMIT 10;
"""
    st.dataframe(beautify_headers(run_query(q11)), use_container_width=True)
 
# ======================================================
# 🌍 LOCATION-BASED TAB
# ======================================================
with tabs[4]:
    st.header("🌍 Location-Based Reports")
 
    st.subheader("1️⃣ Which Countries Report The Highest Rate Of Drug-Related Stops?")
    q_loc1 = """
WITH country_summary AS (
    SELECT
        country_name,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN drugs_related_stop THEN 1 ELSE 0 END) AS drug_stops
    FROM traffic_data
    GROUP BY country_name
)
SELECT
    country_name,
    total_stops,
    drug_stops,
    ROUND(100.0 * drug_stops / NULLIF(total_stops, 0), 2) AS drug_rate_pct
FROM country_summary
ORDER BY drug_rate_pct DESC;
"""
    st.dataframe(beautify_headers(run_query(q_loc1)), use_container_width=True)
 
    st.subheader("2️⃣ Arrest Rate By Country And Violation")
    q_loc2 = """
SELECT
  country_name,
  violation,
  COUNT(*) AS total_stops,
  SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS total_arrests,
  ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
FROM traffic_data
WHERE country_name IS NOT NULL
GROUP BY country_name, violation
ORDER BY country_name, arrest_rate_pct DESC;
"""
    st.dataframe(beautify_headers(run_query(q_loc2)), use_container_width=True)
 
    st.subheader("3️⃣ Which Country Has The Most Stops With Search Conducted?")
    q_loc3 = """
SELECT
    country_name, 
    COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) AS total_searches,
    ROUND(100.0 * SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS search_rate_pct
FROM traffic_data
WHERE country_name IS NOT NULL
GROUP BY country_name
HAVING COUNT(*) >= 10
ORDER BY total_searches DESC;
"""
    st.dataframe(beautify_headers(run_query(q_loc3)), use_container_width=True)
 
# ======================================================
# 🧩 MISCELLANEOUS TAB (POPULATED)
# ======================================================
with tabs[5]:
    st.header("🧩 Miscellaneous Reports")
 
    st.subheader("1️⃣ Yearly Breakdown Of Stops And Arrests By Country")
    q_m1 = """
WITH yearly_agg AS (
    SELECT 
        YEAR(stop_date) AS year,
        country_name,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS total_arrests,
        ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
    FROM traffic_data
    WHERE stop_date IS NOT NULL
    GROUP BY year, country_name
)
SELECT 
    year,
    country_name,
    total_stops,
    total_arrests,
    arrest_rate_pct,
    ROUND(100.0 * total_stops / NULLIF(SUM(total_stops) OVER (PARTITION BY year), 0), 2) AS pct_of_year_stops,
    RANK() OVER (PARTITION BY year ORDER BY arrest_rate_pct DESC) AS arrest_rate_rank_within_year
FROM yearly_agg
ORDER BY year, arrest_rate_pct DESC, country_name;"""
    st.dataframe(beautify_headers(run_query(q_m1)), use_container_width=True)
 
    st.subheader("2️⃣ Driver Violation Trends Based On Age And Race")
    q_m2 = """
WITH grp_violation AS (
  SELECT
    CASE
      WHEN driver_age IS NULL THEN 'unknown'
      WHEN driver_age < 18 THEN '<18'
      WHEN driver_age BETWEEN 18 AND 24 THEN '18-24'
      WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
      WHEN driver_age BETWEEN 35 AND 44 THEN '35-44'
      WHEN driver_age BETWEEN 45 AND 54 THEN '45-54'
      WHEN driver_age BETWEEN 55 AND 64 THEN '55-64'
      ELSE '65+'
    END AS age_group,
    COALESCE(driver_race, 'unknown') AS driver_race,
    COALESCE(violation, 'unknown') AS violation,
    COUNT(*) AS violation_count
  FROM traffic_data
  GROUP BY age_group, driver_race, violation
),
grp_total AS (
  SELECT
    age_group,
    driver_race,
    SUM(violation_count) AS total_in_group
  FROM grp_violation
  GROUP BY age_group, driver_race
),
joined AS (
  SELECT
    v.age_group,
    v.driver_race,
    v.violation,
    v.violation_count,
    t.total_in_group,
    ROUND(100.0 * v.violation_count / NULLIF(t.total_in_group,0), 2) AS pct_of_group
  FROM grp_violation v
  JOIN grp_total t
    ON v.age_group = t.age_group
   AND v.driver_race = t.driver_race
),
ranked AS (
  SELECT
    age_group,
    driver_race,
    violation,
    violation_count,
    total_in_group,
    pct_of_group,
    ROW_NUMBER() OVER (PARTITION BY age_group, driver_race ORDER BY violation_count DESC) AS rn
  FROM joined
)
SELECT
  age_group,
  driver_race,
  violation,
  violation_count,
  total_in_group,
  pct_of_group,
  rn
FROM ranked
ORDER BY age_group, driver_race, rn;
"""
    st.dataframe(beautify_headers(run_query(q_m2)), use_container_width=True)
 
    st.subheader("3️⃣ Time Period Analysis Of Stops (Year, Month, Hour)")
    q_m3 = """
WITH base AS (
    SELECT 
        stop_date, 
        stop_time, 
        country_name,
        CAST(EXTRACT(YEAR FROM stop_date) AS UNSIGNED) AS year,
        CAST(EXTRACT(MONTH FROM stop_date) AS UNSIGNED) AS month,
        DATE_FORMAT(stop_date, '%b') AS month_name,
        CAST(EXTRACT(HOUR FROM stop_time) AS UNSIGNED) AS hour_of_day
    FROM traffic_data
    WHERE stop_date IS NOT NULL
),
agg_period AS (
    SELECT 
        year, month, month_name, hour_of_day, country_name,
        COUNT(*) AS stops_count
    FROM base
    GROUP BY year, month, month_name, hour_of_day, country_name
),
agg_month AS (
    SELECT 
        year, month, month_name, country_name,
        SUM(stops_count) AS month_total_stops
    FROM agg_period
    GROUP BY year, month, month_name, country_name
),
final AS (
    SELECT 
        p.year, p.month, p.month_name, p.hour_of_day, p.country_name,
        p.stops_count, m.month_total_stops,
        ROUND(100.0 * p.stops_count / NULLIF(m.month_total_stops, 0), 2) AS pct_of_month,
        RANK() OVER (PARTITION BY p.year, p.month, p.country_name ORDER BY p.stops_count DESC) AS hour_rank_in_month,
        SUM(p.stops_count) OVER (PARTITION BY p.year, p.country_name) AS year_total_stops,
        ROUND(100.0 * p.stops_count / NULLIF(SUM(p.stops_count) OVER (PARTITION BY p.year, p.country_name), 0), 2) AS pct_of_year
    FROM agg_period p
    JOIN agg_month m 
        ON p.year = m.year AND p.month = m.month AND p.country_name = m.country_name
)
SELECT 
    year, country_name, month, month_name, hour_of_day,
    stops_count, month_total_stops, pct_of_month,
    hour_rank_in_month, year_total_stops, pct_of_year
FROM final
ORDER BY year, country_name, month, hour_rank_in_month;"""
    st.dataframe(beautify_headers(run_query(q_m3)), use_container_width=True)
 
    st.subheader("4️⃣ Violations With High Search And Arrest Rates (Percentile Scoring)")
    q_m4 = """
WITH per_violation AS (
    SELECT 
        violation,
        COUNT(*) AS total_occurrences,
        SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) AS searches,
        SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS arrests,
        ROUND(100.0 * SUM(CASE WHEN search_conducted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS search_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
    FROM traffic_data
    GROUP BY violation
),
ranked AS (
    SELECT 
        pv.*,
        PERCENT_RANK() OVER (ORDER BY pv.search_rate_pct) AS search_rate_pr,
        PERCENT_RANK() OVER (ORDER BY pv.arrest_rate_pct) AS arrest_rate_pr
    FROM per_violation pv
),
scored AS (
    SELECT *,
        (search_rate_pr + arrest_rate_pr) / 2.0 AS combined_pr
    FROM ranked
),
final AS (
    SELECT 
        violation,
        total_occurrences,
        searches,
        arrests,
        search_rate_pct,
        arrest_rate_pct,
        ROUND(CAST(search_rate_pr AS DECIMAL(10,3)), 3) AS search_rate_percentile,
        ROUND(CAST(arrest_rate_pr AS DECIMAL(10,3)), 3) AS arrest_rate_percentile,
        ROUND(CAST(combined_pr AS DECIMAL(10,3)), 3) AS combined_percentile_score,
        ROW_NUMBER() OVER (
            ORDER BY combined_pr DESC, arrest_rate_pct DESC, search_rate_pct DESC
        ) AS rn
    FROM scored
)
SELECT 
    violation,
    total_occurrences,
    searches,
    arrests,
    search_rate_pct,
    arrest_rate_pct,
    search_rate_percentile,
    arrest_rate_percentile,
    combined_percentile_score
FROM final
ORDER BY combined_percentile_score DESC, arrest_rate_pct DESC, search_rate_pct DESC;"""

    st.dataframe(beautify_headers(run_query(q_m4)), use_container_width=True)
 
    st.subheader("5️⃣ Driver Demographics By Country (Age / Gender / Race)")
    q_m5_age = """
SELECT
  country_name,
  age_group,
  total_in_bucket,
  ROUND(100.0 * total_in_bucket / NULLIF(country_total,0), 2) AS pct_of_country
FROM (
  SELECT
    country_name,
    CASE
      WHEN driver_age IS NULL THEN 'unknown'
      WHEN driver_age < 18 THEN '<18'
      WHEN driver_age BETWEEN 18 AND 24 THEN '18-24'
      WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
      WHEN driver_age BETWEEN 35 AND 44 THEN '35-44'
      WHEN driver_age BETWEEN 45 AND 54 THEN '45-54'
      WHEN driver_age BETWEEN 55 AND 64 THEN '55-64'
      ELSE '65+'
    END AS age_group,
    COUNT(*) AS total_in_bucket,
    SUM(COUNT(*)) OVER (PARTITION BY country_name) AS country_total
  FROM traffic_data
  GROUP BY country_name, age_group
) t
ORDER BY country_name, age_group;
"""
    st.dataframe(beautify_headers(run_query(q_m5_age)), use_container_width=True)
 
    st.subheader("5b️⃣ Driver Gender Distribution By Country")
    q_m5_gender = """
SELECT
  country_name,
  COALESCE(driver_gender, 'unknown') AS driver_gender,
  total_in_gender,
  ROUND(100.0 * total_in_gender / NULLIF(country_total,0), 2) AS pct_of_country
FROM (
  SELECT
    country_name,
    COALESCE(driver_gender, 'unknown') AS driver_gender,
    COUNT(*) AS total_in_gender,
    SUM(COUNT(*)) OVER (PARTITION BY country_name) AS country_total
  FROM traffic_data
  GROUP BY country_name, driver_gender
) t
ORDER BY country_name, total_in_gender DESC;
"""
    st.dataframe(beautify_headers(run_query(q_m5_gender)), use_container_width=True)
 
    st.subheader("5c️⃣ Driver Race Distribution By Country")
    q_m5_race = """
SELECT
  country_name,
  COALESCE(driver_race, 'unknown') AS driver_race,
  total_in_race,
  ROUND(100.0 * total_in_race / NULLIF(country_total,0), 2) AS pct_of_country
FROM (
  SELECT
    country_name,
    COALESCE(driver_race, 'unknown') AS driver_race,
    COUNT(*) AS total_in_race,
    SUM(COUNT(*)) OVER (PARTITION BY country_name) AS country_total
  FROM traffic_data
  GROUP BY country_name, driver_race
) t
ORDER BY country_name, total_in_race DESC;
"""
    st.dataframe(beautify_headers(run_query(q_m5_race)), use_container_width=True)
 
    st.subheader("6️⃣ Top 5 Violations With Highest Arrest Rates")
    q_m6 = """
WITH violation_stats AS (
  SELECT
    violation,
    COUNT(*) AS total_occurrences,
    SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS arrest_rate_pct
  FROM traffic_data
  WHERE violation IS NOT NULL
  GROUP BY violation
),
ranked AS (
  SELECT
    violation,
    total_occurrences,
    total_arrests,
    arrest_rate_pct,
    RANK() OVER (ORDER BY arrest_rate_pct DESC) AS rank_by_arrest_rate
  FROM violation_stats
)
SELECT
  violation,
  total_occurrences,
  total_arrests,
  arrest_rate_pct,
  rank_by_arrest_rate
FROM ranked
WHERE rank_by_arrest_rate <= 5
ORDER BY arrest_rate_pct DESC;
"""
    st.dataframe(beautify_headers(run_query(q_m6)), use_container_width=True)
 

