import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
from sqlalchemy import create_engine
import time
from wordcloud import WordCloud


engine = create_engine("")


def fetch_data():
    query = "SELECT * FROM results_table"
    df = pd.read_sql(query, engine)
    return df


def plot_gender_chart(male_count, female_count):
    fig, ax = plt.subplots(figsize=(8, 6))
    gender_data = [male_count, female_count]
    ax.bar(['Male', 'Female'], gender_data, color=['skyblue', 'lightcoral'])
    ax.set_title('Gender Distribution')
    ax.set_ylabel('Count')
    return fig


def plot_level_chart(paid_count, free_count):
    fig, ax = plt.subplots(figsize=(8, 6))
    level_data = [paid_count, free_count]
    ax.bar(['Paid', 'Free'], level_data, color=['green', 'orange'])
    ax.set_title('User Level Distribution')
    ax.set_ylabel('Count')
    return fig


def generate_wordcloud(text):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    return wordcloud


def display_data():
    st.title("User Data Visualizations")
    
    
    col1, col2 = st.columns(2)

    
    gender_plot_container = col1.empty()
    level_plot_container = col2.empty()
    
    
    wordcloud_container = st.empty()

    
    df = fetch_data()

    
    all_top_songs = " ".join(df['top_song'].dropna())

    
    for i, row in df.iterrows():
        
        male_count = row['male_count']
        female_count = row['female_count']
        paid_count = row['paid_count']
        free_count = row['free_count']
        top_song = row['top_song']  

        
        all_top_songs += " " + str(top_song)

        
        wordcloud_fig = generate_wordcloud(all_top_songs)
        wordcloud_container.image(wordcloud_fig.to_array(), use_container_width=True)


        
        gender_fig = plot_gender_chart(male_count, female_count)
        gender_plot_container.pyplot(gender_fig)

        
        level_fig = plot_level_chart(paid_count, free_count)
        level_plot_container.pyplot(level_fig)

        
        time.sleep(2)  


if __name__ == "__main__":
    display_data()
