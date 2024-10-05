# Workshop002_spotify_grammy

This project consists of building an ETL pipeline using Apache Airflow 🚀. The goal is to extract data from a database, perform transformations on it 🔄, and ultimately create a dashboard visualizing the information from the database 📊.

For this challenge, we will work with specific datasets: one from Spotify 🎶 with audio characteristics and another from the Grammy Awards 🏆. Skills in using technologies such as Python 🐍, Apache Airflow, and visualization tools like Looker will be evaluated. The focus is on ensuring that the entire ETL process is managed correctly and that reports come from the final database, not the original files.

---

## Exploratory Data Analysis - Spotify

The exploratory analysis of the Spotify dataset revealed relevant information about the structure and quality of the data. The dataset contains **114,000 rows** and multiple columns covering numerical, categorical, and boolean features. During the process, it was verified that the **'Unnamed: 0'** column meets the criteria to be used as a primary key, ensuring the uniqueness of the records 🔑.

Several peculiarities were identified, such as the repetition of values in the **track_id** column, where the only variation corresponds to the assigned **genre**, reflecting Spotify's algorithm classification. Additionally, **albums with identical names but different artists** were detected, indicating possible versions or variations of the same works 🎤.

In terms of data quality, outliers were observed in columns like **time_signature**, where some records showed signatures of 0/4, which is musically nonsensical. It was also recommended to remove the **track_genre** column since its information is redundant and adds little value to the analysis ❌.

Finally, categorical columns like **key_column** and **time_signature**, although they have numerical values, should be treated as categorical variables. The analysis concluded that the dataset has good potential for deeper future transformations, although some inconsistencies will need to be resolved during the cleaning process 🧹.

## Explanation of the DataFrame Columns

1. **Unnamed: 0 (🔢)**
    - **Description:** Auto-incremental column that guarantees uniqueness.
    - **Data Type:** Integer.
    - **Unique Values:** True.
    - **Notes:** Candidate for primary key due to its unique and sequential property.
  
2. **Track ID (🎫)**
    - **Description:** Unique identifier for each song on Spotify.
    - **Data Type:** String.
    - **Unique Values:** False.
    - **Notes:** Contains duplicates with minimal differences in genre.
  
3. **Artists (🎤)**
    - **Description:** Names of the artists performing the songs.
    - **Data Type:** String.
    - **Unique Values:** False.
    - **Notes:** Includes 31,437 artists, with **The Beatles** appearing 279 times.
  
4. **Album Name (💿)**
    - **Description:** Name of the album where each track appears.
    - **Data Type:** String.
    - **Unique Values:** False.
    - **Notes:** 46,589 unique albums; "Alternative Christmas 2022" appears 195 times.
  
5. **Track Name (🎵)**
    - **Description:** Name of the song.
    - **Data Type:** String.
    - **Unique Values:** False.
    - **Notes:** 73,608 unique names.
  
6. **Explicit Column (🚫)**
    - **Description:** Indicates whether a song contains explicit lyrics.
    - **Data Type:** Boolean.
    - **Unique Values:** False.
    - **Notes:** 91.4% of the songs are not explicit.
  
7. **Key Column (🎹)**
    - **Description:** Tonality of the song according to standard notation.
    - **Data Type:** Integer - Categorical.
    - **Unique Values:** False.
    - **Notes:** 12 distinct tonalities, with the most common being **7** (13,245 times).
  
8. **Time Signature (⏱️)**
    - **Description:** Indicates the meter of the song (number of beats per measure).
    - **Data Type:** Integer - Categorical.
    - **Unique Values:** False.
    - **Notes:** The most common is **4/4** (101,843 songs).
  
9. **Mode (🎛️)**
    - **Description:** Indicates whether the song is in a major or minor mode.
    - **Data Type:** Boolean.
    - **Unique Values:** False.
    - **Notes:** 63.8% of the songs are in major mode.
  
10. **Track Genre (🎧)**
    - **Description:** Genre to which each song belongs.
    - **Data Type:** String.
    - **Unique Values:** False.
    - **Notes:** 114 genres, each associated with 1,000 songs.
  
11. **Energy (🔋)**
    - **Description:** Energy level of the song (0 to 1).
    - **Data Type:** Float.
    - **Notes:** Average of 0.64, indicating moderately energetic songs.
  
12. **Danceability (💃)**
    - **Description:** Measure of how danceable a song is.
    - **Data Type:** Float.
    - **Notes:** Average of 0.58, indicating that the songs are relatively danceable.
  
13. **Duration_ms (⏲️)**
    - **Description:** Duration of the track in milliseconds.
    - **Data Type:** Integer.
    - **Notes:** Average of 212,906 ms (3.54 minutes).
  
14. **Popularity (🎯)**
    - **Description:** Popularity of the song on a scale from 0 to 100.
    - **Data Type:** Integer.
    - **Notes:** Average of 35, indicating that the songs are not very well-known.
  
15. **Loudness (🔊)**
    - **Description:** Overall volume of the song in decibels (dB).
    - **Data Type:** Float.
    - **Notes:** Average of -7 dB, quieter than average.
  
16. **Speechiness (🗣️)**
    - **Description:** Proportion of spoken words in the song.
    - **Data Type:** Float.
    - **Notes:** Average of 0.048, indicating that most are primarily sung.
  
17. **Acousticness (🌲)**
    - **Description:** Confidence that the track is acoustic.
    - **Data Type:** Float.
    - **Notes:** Average of 0.16, suggesting a trend towards synthesized sounds.
  
18. **Instrumentalness (🎻)**
    - **Description:** Probability that the track contains no vocals.
    - **Data Type:** Float.
    - **Notes:** Average close to 0, indicating that 99% have vocal content.
  
19. **Liveness (🎤)**
    - **Description:** Detects if the track was recorded live.
    - **Data Type:** Float.
    - **Notes:** Average of 0.13, indicating a low likelihood of live recordings.
  
20. **Valence (😊)**
    - **Description:** Level of perceived positivity in the song.
    - **Data Type:** Float.
    - **Notes:** Average of 0.46, suggesting a mix of emotions in the songs.
  
21. **Tempo (🔄)**
    - **Description:** Speed of the song in beats per minute (BPM).
    - **Data Type:** Float.
    - **Notes:** Average of 122 BPM, indicating a preference for danceable songs.

---

## Exploratory Data Analysis - Grammy

The analysis of the Grammy Awards dataset revealed relevant information about the structure and quality of the data. The dataset contains **4,810 records** and **10 columns** detailing information about artists, categories, and historical trends. An exponential growth in nominations was observed starting in **2019**, driven by a rule change that increased the number of nominees per category from **5 to 8** 🆙.

Inconsistencies were identified in the **winners** column, where all records appear marked as winners, suggesting errors in the data extraction 🔍. Additionally, artists and albums were found to be nominated in multiple years or in multiple categories in a single edition, reflecting the diversity and repetition of some participants 🎶.

The analysis highlighted a decrease in the number of categories in **2011**, due to the elimination of several of them. This decision was made as part of a review of the event structure, which limited recognition opportunities in a widely followed event.

---

## Technologies Used

- **Python** 🐍
- **Apache Airflow** 🌪️
- **PostgreSQL** 🗄️
- **Looker** 📈
- **Pandas** 📊
- **NumPy** 🔢
- **Matplotlib** 📉

---
## How to Clone and Use the Repository? 🛠️

For a correct use it is necessary to follow the following steps:

1. Clone the repository📁 2.
    
    First, clone the repository and navigate to the project directory:
    
    ```bash
    git clone https://github.com/Juananalv205/Workshop002_spotify_grammy.git
    cd Workshop--001-Data-engineer
    
    ```
    
2. **Create a virtual environment 🐍**
    
    Next, create a virtual environment to install the project dependencies:
    
    ```bash
    python -m venv .venv
    source venv/bin/activate # On Windows use ``venv/scripts/activate`.
    ```
    
3. *Install the requirements 📦 **.
    
    Install the required libraries and dependencies from the 'requirements.txt' file:
    
    ```bash
    pip install -r requirements.txt
    ```
    
4. Add environment variables:🔐
    
    Set the environment variables required for the database connection in a `.env` file:
    
    ```bash
    # Database
    DatabaseHost=“***-mysql.services.clever-cloud.com” # The *** must be changed to the respective identifier.
    DatabaseName=“” #Add the database name, not the table name
    DatabaseUser=“” #Add the used user name
    DatabasePassword=“” #Add the password
    DatabasePort=“3306” #This port will always remain while using Mysql
    ```
    

This set of instructions will guide you through the configuration and preparation of the working environment for this project. By following these steps, you will be able to clone, configure and run the code on your local machine.

---
---

## Contributions

Contributions are welcome. Please open an **issue** or submit a **pull request** for any improvements you wish to make 🤝.

---

Thank you for your interest in this project! If you have any questions or suggestions, feel free to reach out. 😊
