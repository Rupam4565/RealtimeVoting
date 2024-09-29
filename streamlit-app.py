import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import psycopg2

# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        st.error(f"Error creating Kafka consumer: {e}")
        return None

# Function to fetch voting statistics from PostgreSQL database
@st.cache_data
def fetch_voting_stats():
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()
        
        cur.execute("SELECT count(*) FROM voters")
        voters_count = cur.fetchone()[0]

        cur.execute("SELECT count(*) FROM candidates")
        candidates_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()

        return voters_count, candidates_count
    except Exception as e:
        st.error(f"Error fetching voting stats: {e}")
        return 0, 0

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    try:
        messages = consumer.poll(timeout_ms=1000)
        data = []
        for message in messages.values():
            for sub_message in message:
                data.append(sub_message.value)
        return data
    except Exception as e:
        st.error(f"Error fetching data from Kafka: {e}")
        return []

# Function to plot a colored bar chart for vote counts per candidate
def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.title('Vote Counts per Candidate')
    plt.xticks(rotation=90)
    st.pyplot(plt)

# Function to plot a donut chart for vote distribution
def plot_donut_chart(data, title='Vote Distribution'):
    labels = data['candidate_name']
    sizes = data['total_votes']
    
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')  # Equal aspect ratio ensures the pie chart is circular
    plt.title(title)
    st.pyplot(fig)

# Function to split a dataframe into chunks for pagination
@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    return [input_df.iloc[i:i + rows] for i in range(0, len(input_df), rows)]

# Function to paginate a table
def paginate_table(table_data):
    if table_data.empty:
        st.warning("No data available.")
        return

    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True)
        
    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = max(1, len(table_data) // batch_size)
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
        
    st.markdown(f"Page **{current_page}** of **{total_pages}** ")
    pages = split_frame(table_data, batch_size)
    st.dataframe(data=pages[current_page - 1], use_container_width=True)

# Function to update data displayed on the dashboard
def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()
    st.markdown("---")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    if consumer:
        data = fetch_data_from_kafka(consumer)
        if data:
            results = pd.DataFrame(data)
            results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
            leading_candidate = results.loc[results['total_votes'].idxmax()]

            st.markdown("---")
            st.header('Leading Candidate')
            col1, col2 = st.columns(2)
            with col1:
                st.image(leading_candidate['photo_url'], width=200)
            with col2:
                st.header(leading_candidate['candidate_name'])
                st.subheader(leading_candidate['party_affiliation'])
                st.subheader(f"Total Votes: {leading_candidate['total_votes']}")

            st.markdown("---")
            st.header('Statistics')
            results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']].reset_index(drop=True)
            col1, col2 = st.columns(2)

            with col1:
                plot_colored_bar_chart(results)

            with col2:
                plot_donut_chart(results)

            st.table(results)

    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    if location_consumer:
        location_data = fetch_data_from_kafka(location_consumer)
        if location_data:
            location_result = pd.DataFrame(location_data)
            location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()].reset_index(drop=True)

            st.header("Location of Voters")
            paginate_table(location_result)

    st.session_state['last_update'] = time.time()

# Sidebar layout
def sidebar():
    if 'last_update' not in st.session_state:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button('Refresh Data'):
        update_data()

# Title of the Streamlit dashboard
st.title('Real-time Election Dashboard')
sidebar()
update_data()
