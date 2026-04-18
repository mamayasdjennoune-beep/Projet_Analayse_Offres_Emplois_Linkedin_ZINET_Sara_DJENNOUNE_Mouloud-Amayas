# Travail réalisé par : ZINET Sara, DJENNOUNE Mouloud-Amayas

#----------------------------------------------------------------------------------------------------------------

import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(layout="wide")

# ================================
# 🎨 STYLE GLOBAL
# ================================
st.markdown("""
    <style>
    .block-container {padding-top: 2rem;}
    h1, h2, h3 {font-weight: 600;}
    </style>
""", unsafe_allow_html=True)

st.title("💼 Job Market Insights")
st.caption("Real-time analytics powered by Snowflake")

# ================================
# 🔗 SNOWFLAKE SESSION
# ================================
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# ================================
# 🔧 INDUSTRY MAPPING
# ================================
industry_map = {
    1: "Defense & Space", 3: "Computer Hardware", 4: "Computer Software",
    5: "Computer Networking", 6: "Internet", 7: "Computer & Network Security",
    8: "Information Technology & Services", 9: "Semiconductors",
    10: "Telecommunications", 11: "Law Practice", 12: "Legal Services",
    13: "Management Consulting", 14: "Biotechnology", 15: "Medical Practice",
    16: "Hospital & Health Care", 17: "Pharmaceuticals", 18: "Veterinary",
    19: "Medical Devices", 20: "Cosmetics", 21: "Apparel & Fashion",
    22: "Sporting Goods", 23: "Tobacco", 24: "Supermarkets",
    25: "Food Production", 26: "Consumer Electronics", 27: "Consumer Goods",
    28: "Furniture", 29: "Retail", 30: "Entertainment",
    31: "Gambling & Casinos", 32: "Leisure, Travel & Tourism",
    33: "Hospitality", 34: "Restaurants", 35: "Sports",
    36: "Food & Beverages", 37: "Motion Pictures & Film",
    38: "Broadcast Media", 39: "Museums & Institutions",
    40: "Fine Art", 41: "Performing Arts",
    42: "Recreational Facilities & Services", 43: "Banking",
    44: "Insurance", 45: "Financial Services", 46: "Real Estate",
    47: "Investment Banking", 48: "Investment Management",
    49: "Accounting", 50: "Construction", 51: "Building Materials",
    52: "Architecture & Planning", 53: "Civil Engineering",
    54: "Aviation & Aerospace", 55: "Automotive", 56: "Chemicals",
    57: "Machinery", 58: "Mining & Metals", 59: "Oil & Energy",
    60: "Utilities", 61: "Shipbuilding", 62: "Packaging & Containers",
    63: "Railroad Manufacture", 64: "Renewables & Environment",
    65: "Glass, Ceramics & Concrete", 66: "Textiles", 67: "Warehousing",
    68: "Airlines/Aviation", 69: "Maritime",
    70: "Transportation/Trucking/Railroad",
    71: "Logistics & Supply Chain", 72: "Import & Export",
    73: "Primary/Secondary Education", 74: "Higher Education",
    75: "Education Management", 76: "Research", 77: "Military",
    78: "Legislative Office", 79: "Judiciary",
    80: "International Affairs", 81: "Government Administration",
    82: "Executive Office", 83: "Law Enforcement",
    84: "Public Safety", 85: "Public Policy",
    86: "Marketing & Advertising", 87: "Newspapers",
    88: "Publishing", 89: "Printing", 90: "Information Services",
    91: "Libraries", 92: "Environmental Services",
    93: "Package/Freight Delivery", 94: "Individual & Family Services",
    95: "Religious Institutions", 96: "Civic & Social Organization",
    97: "Consumer Services", 98: "Nonprofit Organization Management",
    99: "Fund-Raising", 100: "Program Development",
    101: "Writing & Editing", 102: "Staffing & Recruiting",
    103: "Professional Training & Coaching", 104: "Market Research",
    105: "Public Relations & Communications", 106: "Design",
    107: "Graphic Design", 108: "Photography", 109: "Arts & Crafts",
    110: "Animation", 111: "Music", 112: "Online Media",
    113: "Events Services", 114: "Business Supplies & Equipment",
    115: "E-Learning", 116: "Outsourcing/Offshoring",
    117: "Facilities Services", 118: "Human Resources",
    119: "Venture Capital & Private Equity", 120: "Think Tanks",
    121: "Nanotechnology", 122: "Computer Games",
    123: "Alternative Medicine", 124: "Health, Wellness & Fitness",
    125: "Alternative Dispute Resolution", 126: "Mental Health Care",
    127: "Philanthropy", 128: "International Trade & Development",
    129: "Wireless", 130: "Capital Markets",
    131: "Political Organization", 132: "Translation & Localization",
    133: "Computer & Network Security", 134: "Farming",
    135: "Ranching", 136: "Dairy", 137: "Fishery",
    138: "Paper & Forest Products", 139: "Forestry",
    140: "Luxury Goods & Jewelry", 141: "Renewables & Environment",
    142: "Mechanical or Industrial Engineering",
    143: "Industrial Automation",
    144: "Electrical/Electronic Manufacturing",
    145: "Plastics", 146: "Rubber & Plastics",
    147: "Wholesale", 148: "Commercial Real Estate",
    149: "Banking", 150: "Leasing Real Estate"
}

def map_industry(df):
    df["industry_name"] = df["INDUSTRY_ID"].map(industry_map).fillna("Other")
    return df

# ================================
# 🎯 1. TOP ROLES
# ================================
st.markdown("## 🎯 Top 10 des titres de postes les plus publiés par industrie")

query1 = """
SELECT industry_id, title, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND title IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY nb_jobs DESC) <= 10
"""

df1 = map_industry(session.sql(query1).to_pandas())

industry = st.selectbox("Industry", sorted(df1["industry_name"].unique()))

df1 = (
    df1[df1["industry_name"] == industry]
    .sort_values("NB_JOBS", ascending=False)
    .head(10)
)

chart1 = alt.Chart(df1).mark_bar(size=18).encode(
    x=alt.X("NB_JOBS:Q", title="Job Openings"),
    y=alt.Y("TITLE:N", sort='-x'),
    color=alt.value("#4C78A8"),
    tooltip=["TITLE", "NB_JOBS"]
)

st.altair_chart(chart1, use_container_width=True)

# ================================
# 💰 2. SALARY
# ================================
st.markdown("## 💰 Top 10 des postes les mieux rémunérés par industrie")

query2 = """
SELECT industry_id, title, ROUND(AVG(max_salary),0) AS avg_salary
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND max_salary IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY avg_salary DESC) <= 10
"""

df2 = map_industry(session.sql(query2).to_pandas())

industry2 = st.selectbox("Industry (Salary)", sorted(df2["industry_name"].unique()))

df2 = (
    df2[df2["industry_name"] == industry2]
    .sort_values("AVG_SALARY", ascending=False)
    .head(10)
)

chart2 = alt.Chart(df2).mark_bar(size=18).encode(
    x=alt.X("AVG_SALARY:Q", title="Salary"),
    y=alt.Y("TITLE:N", sort='-x'),
    color=alt.Color("AVG_SALARY:Q", scale=alt.Scale(scheme="goldgreen")),
    tooltip=["TITLE", "AVG_SALARY"]
)

st.altair_chart(chart2, use_container_width=True)

# ================================
# 🏢 3. COMPANY SIZE DISTRIBUTION
# ================================
st.markdown("## 🏢 Répartition des offres d'emploi par taille d'entreprise")

query3 = """
SELECT company_size, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY company_size
"""

df3 = session.sql(query3).to_pandas()

chart3 = alt.Chart(df3).mark_line(point=True).encode(
    x=alt.X("COMPANY_SIZE:O", title="Company Size"),
    y=alt.Y("NB_JOBS:Q", title="Number of Jobs"),
    tooltip=["COMPANY_SIZE", "NB_JOBS"]
)

st.altair_chart(chart3, use_container_width=True)

top_size = df3.sort_values("NB_JOBS", ascending=False).iloc[0]
st.success(f"👉 Most opportunities come from company size: {top_size['COMPANY_SIZE']}")

# ================================
# 🌍 4. INDUSTRY HIRING (WAFFLE CHART PREMIUM)
# ================================
st.markdown("## 🌍 Hiring by Industry")

query4 = """
SELECT industry_id, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL
GROUP BY industry_id
ORDER BY nb_jobs DESC
LIMIT 10
"""

df4 = session.sql(query4).to_pandas()

# ================================
# 🎨 PREPARATION WAFFLE
# ================================
df4 = map_industry(df4)

total = df4["NB_JOBS"].sum()
df4["pct"] = df4["NB_JOBS"] / total

waffle = []
for i, row in df4.iterrows():
    blocks = int(row["pct"] * 100)
    waffle += [(row["industry_name"], 1)] * blocks

df_waffle = pd.DataFrame(waffle, columns=["industry", "value"])
df_waffle["id"] = range(len(df_waffle))

# ================================
# 📊 WAFFLE VISUAL (COULEURS CORRIGÉES)
# ================================
chart4 = alt.Chart(df_waffle).mark_square(size=120).encode(
    x=alt.X("id:Q", axis=None),
    y=alt.Y("value:Q", axis=None),
    color=alt.Color(
        "industry:N",
        scale=alt.Scale(
            domain=sorted(df_waffle["industry"].unique()),
            range=[
                "#2563eb",  # bleu
                "#10b981",  # vert
                "#f59e0b",  # orange
                "#ef4444",  # rouge
                "#8b5cf6",  # violet
                "#06b6d4",  # cyan
                "#84cc16",  # lime
                "#f97316",  # orange foncé
                "#ec4899",  # rose
                "#64748b"   # gris
            ]
        ),
        legend=alt.Legend(title="Industries")
    ),
    tooltip=["industry"]
).properties(height=200)

st.altair_chart(chart4, use_container_width=True)

# ================================
# 📌 INSIGHT BOX
# ================================
top_ind = df4.iloc[0]

st.info(f"""
🌍 Industrie dominante : **{top_ind['industry_name']}**  
📊 Volume : **{int(top_ind['NB_JOBS']):,} offres**
""")

# ================================
# 📌 5. EMPLOIS PAR TYPE (VERSION PREMIUM FR)
# ================================
st.markdown("## 📌 Répartition des types de contrats")

query5 = """
SELECT formatted_work_type, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type
ORDER BY nb_jobs DESC
"""

df5 = session.sql(query5).to_pandas()

# ================================
# 🎨 STYLE KPI MODERNE (CARDS COLORÉES)
# ================================
st.markdown("""
<style>
.kpi-card {
    background: linear-gradient(135deg, #ffffff, #f8f9ff);
    padding: 18px;
    border-radius: 16px;
    box-shadow: 0 6px 18px rgba(0,0,0,0.06);
    text-align: center;
    border: 1px solid rgba(0,0,0,0.05);
}

.kpi-title {
    font-size: 13px;
    color: #6b7280;
    margin-bottom: 8px;
}

.kpi-value {
    font-size: 28px;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)

# ================================
# 🔥 GRAPHIQUE (SEULEMENT COULEURS MODIFIÉES)
# ================================
chart5 = alt.Chart(df5).mark_bar(
    cornerRadiusTopLeft=6,
    cornerRadiusTopRight=6
).encode(
    x=alt.X("NB_JOBS:Q", title="Nombre d'emplois"),
    y=alt.Y(
        "FORMATTED_WORK_TYPE:N",
        sort='-x',
        title="Type de contrat"
    ),
    color=alt.Color(
        "FORMATTED_WORK_TYPE:N",
        scale=alt.Scale(
            domain=[
                "Full-time",
                "Contract",
                "Part-time",
                "Internship",
                "Temporary",
                "Volunteer"
            ],
            range=[
                "#2563eb",  # bleu
                "#f59e0b",  # orange
                "#10b981",  # vert
                "#8b5cf6",  # violet
                "#ef4444",  # rouge
                "#ec4899"   # rose
            ]
        ),
        legend=None
    ),
    tooltip=["FORMATTED_WORK_TYPE", "NB_JOBS"]
).properties(height=260)

st.altair_chart(chart5, use_container_width=True)

# ================================
# 💎 KPI CARDS (INCHANGÉ)
# ================================
cols = st.columns(len(df5))

for i, row in df5.iterrows():

    label = str(row["FORMATTED_WORK_TYPE"])
    value = int(row["NB_JOBS"])

    color = "#64748b"

    if "Full" in label:
        color = "#2563eb"
    elif "Contract" in label:
        color = "#f59e0b"
    elif "Part" in label:
        color = "#10b981"
    elif "Intern" in label:
        color = "#8b5cf6"
    elif "Temp" in label:
        color = "#ef4444"
    elif "Volunteer" in label:
        color = "#ec4899"

    cols[i].markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Type de contrat</div>
        <div class="kpi-value" style="color:{color};">
            {value:,}
        </div>
        <div style="font-size:13px; margin-top:5px; color:#111;">
            {label}
        </div>
    </div>
    """, unsafe_allow_html=True)
