import pandas as pd
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

# =================
# 1. Load csv files
# =================
amplitude = pd.read_csv("/Users/Joseph.Jayakumar/Downloads/amplitude_user_ids.csv")
listing = pd.read_csv("/Users/Joseph.Jayakumar/Downloads/view_listing_detail_events.csv")
reservations = pd.read_csv("/Users/Joseph.Jayakumar/Downloads/reservations.csv")
searches = pd.read_csv("/Users/Joseph.Jayakumar/Downloads/all_search_events.csv")

############### Search → View Conversion ###############################################################################

# =====================
# 2. Count unique users
# =====================
unique_search_users = searches["merged_amplitude_id"].nunique()
unique_view_users = listing["merged_amplitude_id"].nunique()

# ==============================
# 3. Join on merged_amplitude_id
# ==============================
search_view_users = pd.merge(
    searches[["merged_amplitude_id"]].drop_duplicates(),
    listing[["merged_amplitude_id"]].drop_duplicates(),
    on="merged_amplitude_id",
    how="inner"
)

converted_users = search_view_users["merged_amplitude_id"].nunique()

# ==========================
# 4. Compute conversion rate
# ==========================
conversion_rate = converted_users / unique_search_users * 100

# ================
# 5. Print results
# ================
print(f"Unique users who searched: {unique_search_users}")
print(f"Unique users who viewed: {unique_view_users}")
print(f"Users who searched and viewed: {converted_users}")
print(f"Search → View conversion rate: {conversion_rate:.2f}%")

########################################################################################################################

########################## Attribution Channel Funnels #############################################################################
# ==============================
# 1. Join on merged_amplitude_id
# ==============================
search_amplitude_users = pd.merge(searches, amplitude, on="merged_amplitude_id", how="inner")

# ===========================
# 2. Only keep necessary keys
# ===========================
#search_attribution = search_amplitude_users[['user_id', 'first_attribution_channel', 'event_date']]
search_attribution = search_amplitude_users[['user_id', 'first_attribution_channel']]

# ======================
# 3. Dropping duplicates
# ======================
search_attribution = search_attribution.drop_duplicates()

# =============================================
# 4. Getting counts of each attribution channel
# =============================================
counts_search_attribution = search_attribution.groupby("first_attribution_channel").size().reset_index(name="count")

# reservations["created_at"] = pd.to_datetime(reservations["created_at"])
# reservations["new_date"] = reservations["created_at"].dt.date

# =============================================
# 5. Merging attribution channel to reservation
# =============================================
# reservation_attribution_channel = pd.merge(
#     reservations, search_attribution,
#     left_on=["renter_user_id", 'new_date'], right_on=["user_id", 'event_date'], how="inner")
reservation_attribution_channel = pd.merge(
    reservations, search_attribution,
    left_on=["renter_user_id"], right_on=["user_id"], how="inner")

# ===========================
# 6. Only keep necessary keys
# ===========================
reservation_channel = reservation_attribution_channel[['renter_user_id', 'first_attribution_channel']]

# ======================
# 7. Dropping duplicates
# ======================
reservation_channel = reservation_channel.drop_duplicates()

# =========================================================
# 8. Getting reservation counts of each attribution channel
# =========================================================
counts_reservation_channel = reservation_channel.groupby("first_attribution_channel").size().reset_index(name="count")

# ========================================
# 9. Merging counts on attribution channel
# ========================================
counts = pd.merge(counts_search_attribution, counts_reservation_channel, on="first_attribution_channel", how="outer").fillna(0)
counts = counts.rename(columns={"count_x": "search_count", "count_y": "reservation_count"})

# ================================
# 10. plot side-by-side bar graphs
# ================================
x = range(len(counts))
width = 0.35  # width of the bars
fig, ax = plt.subplots(figsize=(10, 6))
bars1 = ax.bar([i - width/2 for i in x], counts["search_count"], width=width, label="Searches")
bars2 = ax.bar([i + width/2 for i in x], counts["reservation_count"], width=width, label="Reservations")

# =======================================
# 11. Add value labels on top of each bar
# =======================================
for bar in bars1 + bars2:
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width()/2,
        height,
        f"{int(height)}",  # format as integer
        ha="center",
        va="bottom",
        fontsize=9
    )

# ===================
# 12.Formatting graph
# ===================
ax.set_xticks(x)
ax.set_xticklabels(counts["first_attribution_channel"], rotation=45, ha="right")
ax.set_ylabel("Count")
ax.set_title("Search vs Reservation Counts by Attribution Channel")
ax.legend()
plt.tight_layout()

# ===================
# 13.Displaying graph
# ===================
plt.show()

########################################################################################################################

########################## Channel Funnels By Top Ten Cities ###########################################################

import sqlite3
conn = sqlite3.connect("my_database.db")
query = """
WITH top_ten AS (
    SELECT *
    FROM (
        SELECT
            city,
            COUNT(*) AS reservation_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS city_rank
        FROM reservations
        WHERE successful_payment_collected_at IS NOT NULL
        GROUP BY city
    ) ranked_cities
    WHERE city_rank <= 10
),

attri_count AS (
    SELECT DISTINCT u.user_id, s.city, s.attribution_channel
    FROM searches s
    JOIN users u ON s.merged_amplitude_id = u.merged_amplitude_id
    WHERE s.city IN (SELECT city FROM top_ten)
)

SELECT city, attribution_channel, COUNT(*) AS user_count
FROM attri_count
GROUP BY city, attribution_channel
ORDER BY city, user_count DESC;
"""
# Execute query and load into a Pandas DataFrame
df = pd.read_sql_query(query, conn)

# Show results
print(df)
########################################################################################################################

#################### Payment Failure Rate ##############################################################################

failure_query = """
SELECT
    (SUM(CASE WHEN payment_successful IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS failure_rate
FROM
    reservations;
"""
# Execute query and load into a Pandas DataFrame
df = pd.read_sql_query(failure_query, conn)

# Show results
print(df)
########################################################################################################################