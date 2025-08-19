import streamlit as st
import pandas as pd
import numpy as np
import pymysql
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = "d93800bb-695a-4b72-945e-7af96682cb9e"
URL = "https://api.harvardartmuseums.org/object"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "harvard"
}

# DATABASE CREATION 
def create_database_if_not_exists():
    conn = pymysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    with conn.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS harvard")
    conn.close()

# Ensure DB exists
create_database_if_not_exists()

# STREAMLIT UI CONFIG 
st.set_page_config(layout="wide")
st.title("Harvard's Artifacts Collection")

if 'active_btn' not in st.session_state:
    st.session_state.active_btn = "choice"
if 'collected_data' not in st.session_state:
    st.session_state.collected_data = {"meta": None, "media": None, "color": None}
if 'inserted_data_all' not in st.session_state:
    st.session_state.inserted_data_all = {"meta": pd.DataFrame(), "media": pd.DataFrame(), "color": pd.DataFrame()}
if 'display_tables' not in st.session_state:
    st.session_state.display_tables = False

classifications = ["Paintings","Photographs","Sculpture","Prints","Drawings","Vessels","Coins"]

with st.form(key="collect_form", clear_on_submit=False):
    selected_class = st.selectbox("Select Classification", classifications)
    collect_data_btn = st.form_submit_button("Collect data")

#  DB TABLE SCHEMA 
def create_tables_if_not_exist(conn):
    with conn.cursor() as cursor:
         cursor.execute("""
            CREATE TABLE IF NOT EXISTS artifact_metadata (
                id INT PRIMARY KEY,
                title VARCHAR(255),
                culture VARCHAR(255),
                period VARCHAR(255),
                century VARCHAR(255),
                medium VARCHAR(255),
                dimensions VARCHAR(255),
                description VARCHAR(255),
                department VARCHAR(255),
                classification VARCHAR(255),
                accessionyear INT,
                accessionmethod VARCHAR(255))
        """)
         cursor.execute("""
            CREATE TABLE IF NOT EXISTS artifact_media (
                objectid INT PRIMARY KEY,
                imagecount INT,
                mediacount INT,
                colorcount INT,
                rank INT,
                datebegin INT,
                dateend INT,
                FOREIGN KEY (objectid) REFERENCES artifact_metadata(id))
        """)
         cursor.execute("""
            CREATE TABLE IF NOT EXISTS artifact_colors (
                objectid INT,
                color VARCHAR(255),
                spectrum VARCHAR(255),
                hue VARCHAR(255),
                percent REAL,
                css VARCHAR(255),
                PRIMARY KEY (objectid, color),
                FOREIGN KEY (objectid) REFERENCES artifact_metadata(id))
        """)
    conn.commit()

# API FETCH 
def fetch_page(page, classification):
    params = {
        "apikey": API_KEY,
        "size": 100,
        "page": page,
        "hasimage": 1,
        "classification": classification
    }
    try:
        response = requests.get(URL, params=params, timeout=15)
        if response.status_code == 200:
            return response.json().get("records", [])
    except Exception:
        return []
    return []

def collect_data(classification):
    meta, media, color = [], [], []
    page_list = list(range(1, 26))
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_page, page, classification) for page in page_list]
        for future in as_completed(futures):
            records = future.result()
            for i in records:
                obj_id = i.get("id")
                meta.append({
                    "id": obj_id,
                    "title": i.get("title"),
                    "culture": i.get("culture"),
                    "period": i.get("period"),
                    "century": i.get("century"),
                    "medium": i.get("medium"),
                    "dimensions": i.get("dimensions"),
                    "description": i.get("description"),
                    "classification": i.get("classification"),
                    "accessionyear": i.get("accessionyear"),
                    "accessionmethod": i.get("accessionmethod"),
                    "department": i.get("department") if "department" in i else None
                })
                media.append({
                    "objectid": obj_id,
                    "imagecount": i.get("imagecount"),
                    "mediacount": i.get("mediacount"),
                    "colorcount": i.get("colorcount"),
                    "rank": i.get("rank"),
                    "datebegin": i.get("datebegin"),
                    "dateend": i.get("dateend")
                })
                for j in i.get("colors", []):
                    color.append({
                        "objectid": obj_id,
                        "color": j.get("color"),
                        "spectrum": j.get("spectrum"),
                        "hue": j.get("hue"),
                        "percent": j.get("percent"),
                        "css": j.get("css")
                    })
    df_meta = pd.DataFrame(meta)
    df_media = pd.DataFrame(media)
    df_color = pd.DataFrame(color)
    return df_meta, df_media, df_color

#  DATA CLEANING 
def robust_clean(df):
    df = df.astype(object)
    df = df.replace({np.nan: None, pd.NA: None})
    df = df.where(pd.notnull(df), None)
    df = df.replace('nan', None)
    return df

# INSERT (ONE TIME ONLY) 
def insert_data_to_db(meta, media, color):
    meta = robust_clean(meta)
    media = robust_clean(media)
    color = robust_clean(color)

    conn = pymysql.connect(**DB_CONFIG)
    create_tables_if_not_exist(conn)
    cursor = conn.cursor()

    inserted_flag = False
    try:
        if not meta.empty:
            meta_tuples = [
                (
                    int(m['id']), m['title'], m['culture'], m['period'], m['century'],
                    m['medium'], m['dimensions'], m['description'], m['classification'],
                    m['accessionyear'], m['accessionmethod'], m.get('department')
                )
                for _, m in meta.iterrows()
            ]
            cursor.executemany(
                """
                INSERT IGNORE INTO artifact_metadata (
                    id, title, culture, period, century, medium,
                    dimensions, description, classification, accessionyear, accessionmethod, department
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                meta_tuples
            )
            if cursor.rowcount > 0:
                inserted_flag = True

        if not media.empty:
            media_tuples = [
                (
                    int(m['objectid']), m['imagecount'], m['mediacount'], m['colorcount'],
                    m['rank'], m['datebegin'], m['dateend']
                )
                for _, m in media.iterrows()
            ]
            cursor.executemany(
                """
                INSERT IGNORE INTO artifact_media (
                    objectid, imagecount, mediacount, colorcount, rank, datebegin, dateend
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                media_tuples
            )
            if cursor.rowcount > 0:
                inserted_flag = True

        if not color.empty:
            color_tuples = [
                (
                    int(c['objectid']), c['color'], c['spectrum'], c['hue'], c['percent'], c['css']
                )
                for _, c in color.iterrows()
            ]
            cursor.executemany(
                """
                INSERT IGNORE INTO artifact_colors (
                    objectid, color, spectrum, hue, percent, css
                ) VALUES (%s,%s,%s,%s,%s,%s)
                """,
                color_tuples
            )
            if cursor.rowcount > 0:
                inserted_flag = True
        conn.commit()
        return inserted_flag
    finally:
        conn.close()

#  UTILITY FUNCS 
def set_active_btn(new_btn):
    st.session_state.active_btn = new_btn
    st.session_state.display_tables = False

def safe_display_dataframe(df, name="Query Result"):
    st.subheader(name)
    if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
        st.dataframe(df)
    else:
        st.warning("No data available for the query result.")

def display_efficient(data, name, max_rows=500, full_table=False):
    st.subheader(name)
    if data is None or not isinstance(data, pd.DataFrame) or data.empty:
        st.warning(f"No {name} data to display.")
        return
    if full_table:
        st.dataframe(data)
    else:
        if len(data) > max_rows:
            json_str = json.dumps(json.loads(data.head(max_rows).to_json(orient="records")), indent=2)
            html_code = f"""<div style="height:700px; overflow:auto; border:1px solid #ccc; background:#E6F0ED; padding:10px; font-family:monospace; white-space:pre;">
            {json_str.replace(" ", "&nbsp;").replace("\\n", "<br>")}
            </div>"""
            st.markdown(html_code, unsafe_allow_html=True)
        else:
            st.dataframe(data)

# STREAMLIT BUTTONS 
with st.container():
    c1, c2, c3 = st.columns([3.5, 4, 3.5])
    with c1:
        st.button("Select Your Choice", use_container_width=True,
                  type="primary" if st.session_state.active_btn == "choice" else "secondary",
                  on_click=set_active_btn, args=("choice",))
    with c2:
        st.button("Migrate to SQL", use_container_width=True,
                  type="primary" if st.session_state.active_btn == "migrate" else "secondary",
                  on_click=set_active_btn, args=("migrate",))
    with c3:
        st.button("SQL Queries", use_container_width=True,
                  type="primary" if st.session_state.active_btn == "query" else "secondary",
                  on_click=set_active_btn, args=("query",))
st.markdown("---")

# COLLECT DATA 
if collect_data_btn:
    with st.spinner("Fetching data..."):
        meta, media, color = collect_data(selected_class)
        if len(meta) == 0:
            st.error("No data found for this classification.")
        else:
            st.session_state.collected_data = {"meta": meta, "media": media, "color": color}
            st.session_state.display_tables = False
            st.success(f"✅ Now migrate to SQL to insert.")

# INSERT DATA 
if st.session_state.active_btn == "migrate":
    st.subheader("Insert the collected data")
    insert_btn = st.button("Insert")
    meta = st.session_state.collected_data.get("meta")
    media = st.session_state.collected_data.get("media")
    color = st.session_state.collected_data.get("color")

    if insert_btn:
        if meta is not None and not meta.empty:
            try:
                inserted_any = insert_data_to_db(meta, media, color)
                if inserted_any:
                    st.success("Data inserted successfully!")
                    st.session_state.inserted_data_all["meta"] = pd.concat(
                        [st.session_state.inserted_data_all["meta"], meta], ignore_index=True)
                    st.session_state.inserted_data_all["media"] = pd.concat(
                        [st.session_state.inserted_data_all["media"], media], ignore_index=True)
                    st.session_state.inserted_data_all["color"] = pd.concat(
                        [st.session_state.inserted_data_all["color"], color], ignore_index=True)
                    st.session_state.display_tables = True
                else:
                    st.info("Data already inserted earlier, No duplicates added.")
            except Exception as e:
                st.error(f"Insert failed: {str(e)}")

    if st.session_state.display_tables and not st.session_state.inserted_data_all["meta"].empty:
        st.markdown("#### Inserted Data (All Classifications):")
        display_efficient(st.session_state.inserted_data_all["meta"], "Artifacts Metadata", full_table=True)
        display_efficient(st.session_state.inserted_data_all["media"], "Artifacts Media", full_table=True)
        display_efficient(st.session_state.inserted_data_all["color"], "Artifacts Colors", full_table=True)

# DISPLAY CHOICE 
elif st.session_state.active_btn == "choice":
    col1, col2, col3 = st.columns(3)
    meta = st.session_state.collected_data.get("meta")
    media = st.session_state.collected_data.get("media")
    color = st.session_state.collected_data.get("color")
    with col1: display_efficient(meta, "Metadata", max_rows=500, full_table=False)
    with col2: display_efficient(media, "Media", max_rows=500, full_table=False)
    with col3: display_efficient(color, "Color", max_rows=500, full_table=False)

# QUERIES 
elif st.session_state.active_btn == "query":
    st.subheader("SQL Queries")
    queries = [
        "List all artifacts from the 11th century belonging to Byzantine culture.",
        "What are the unique cultures represented in the artifacts?",
        "List all artifacts from the Archaic Period.",
        "List artifact titles ordered by accession year in descending order.",
        "How many artifacts are there per department?",
        "Which artifacts have more than 3 images?",
        "What is the average rank of all artifacts?",
        "Which artifacts have a higher mediacount than colorcount?",
        "List all artifacts created between 1500 and 1600.",
        "How many artifacts have no media files?",
        "What are all the distinct hues used in the dataset?",
        "What are the top 5 most used colors by frequency?",
        "What is the average coverage percentage for each hue?",
        "List all colors used for a given artifact ID.",
        "What is the total number of color entries in the dataset?",
        "List artifact titles and hues for all artifacts belonging to the Byzantine culture.",
        "List each artifact title with its associated hues.",
        "Get artifact titles, cultures, and media ranks where the period is not null.",
        "Find artifact titles ranked in the top 10 that include the color hue 'Grey'.",
        "How many artifacts exist per classification, and what is the average media count for each?",
        "Get count of artifacts by accession year?",
        "List artifacts with rank less than or equal to 5?",
        "Get all artifact IDs that have colors data?",
        "List artifact media entries with no colors?",
        "Count artifacts by medium?"
    ]
    sql_stmts = [
        "SELECT * FROM artifact_metadata WHERE century LIKE '%11%' AND culture LIKE '%Byzantine%'",
        "SELECT DISTINCT culture FROM artifact_metadata",
        "SELECT * FROM artifact_metadata WHERE period LIKE '%Archaic%'",
        "SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC",
        "SELECT department, COUNT(*) AS artifact_count FROM artifact_metadata GROUP BY department",
        """
        SELECT m.id, m.title, a.imagecount
        FROM artifact_metadata m
        JOIN artifact_media a ON m.id=a.objectid
        WHERE a.imagecount > 3
        """,
        "SELECT AVG(rank) AS avg_rank FROM artifact_media",
        "SELECT * FROM artifact_media WHERE colorcount > mediacount",
        """
        SELECT *
        FROM artifact_media a
        JOIN artifact_metadata m ON a.objectid=m.id
        WHERE a.datebegin >= 1500 AND a.dateend <= 1600
        """,
        "SELECT COUNT(*) AS no_media_count FROM artifact_media WHERE mediacount = 0",
        "SELECT DISTINCT hue FROM artifact_colors",
        """
        SELECT color, COUNT(*) AS freq
        FROM artifact_colors
        GROUP BY color
        ORDER BY freq DESC
        LIMIT 5
        """,
        """
        SELECT hue, AVG(percent) AS avg_coverage
        FROM artifact_colors
        GROUP BY hue
        """,
        "SELECT color FROM artifact_colors WHERE objectid = {artifact_id}",#14
        "SELECT COUNT(*) AS total_colors FROM artifact_colors", #15
        """
        SELECT m.title, c.hue
        FROM artifact_metadata m
        JOIN artifact_colors c ON m.id = c.objectid
        WHERE m.culture = 'Byzantine' 
        """,
        """
        SELECT m.title, c.hue
        FROM artifact_metadata m
        JOIN artifact_colors c ON m.id = c.objectid
        """,
        """
        SELECT m.title, m.culture, a.rank
        FROM artifact_metadata m
        JOIN artifact_media a ON m.id = a.objectid
        WHERE m.period IS NOT NULL AND m.period != ''
        """,
        """
        SELECT m.title
        FROM artifact_metadata m
        JOIN artifact_media a ON m.id = a.objectid
        JOIN artifact_colors c ON m.id = c.objectid
        WHERE a.rank <= 10 AND c.hue = 'Grey'
        """,
        """
        SELECT m.classification, COUNT(*) AS artifact_count, AVG(a.mediacount) AS avg_media_count
        FROM artifact_metadata m
        JOIN artifact_media a ON m.id = a.objectid
        GROUP BY m.classification
        """,
        """
        SELECT accessionyear, COUNT(*) FROM artifact_metadata 
        GROUP BY accessionyear
        ORDER BY accessionyear""",
        "SELECT * FROM artifact_media WHERE rank <= 5",
        """
        SELECT DISTINCT objectid FROM artifact_colors
        """,
        """
        SELECT * FROM artifact_media WHERE colorcount = 0
        """,
        """
        SELECT medium, COUNT(*) FROM artifact_metadata
        GROUP BY medium
        ORDER BY COUNT(*) DESC
        """
    ]

    selected_query_idx = st.selectbox(
        "Select a query",
        range(len(queries)),
        format_func=lambda x: f"{x+1}. {queries[x]}"
    )
    artifact_id = None
    sql_stmt = None

    if selected_query_idx == 13:
        artifact_id = st.text_input("Enter artifact ID:", "")
        if artifact_id and artifact_id.isdigit():
            sql_stmt = sql_stmts[selected_query_idx].format(artifact_id=int(artifact_id))
        else:
            st.info("Enter a valid artifact ID (integer) to see its colors.")
            sql_stmt = None
    else:
        sql_stmt = sql_stmts[selected_query_idx]

    if sql_stmt:
        conn = pymysql.connect(**DB_CONFIG)
        try:
            df = pd.read_sql(sql_stmt, conn)
            safe_display_dataframe(df)
        except Exception as e:
            st.error(f"Error executing query: {e}")
        finally:
            conn.close()