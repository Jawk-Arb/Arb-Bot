import tensorflow as tf
import tensorflow_hub as hub
import pandas as pd
import re
from collections import Counter
import asyncio
import time
from openai import AsyncOpenAI
from getMarkets import PolyMarketAPI, KalshiAPI
import os
import logging
from dotenv import load_dotenv

load_dotenv()

class MarketMatcher:
    def __init__(self):
        # Load Universal Sentence Encoder
        self.model = hub.load('https://www.kaggle.com/models/google/universal-sentence-encoder/TensorFlow2/universal-sentence-encoder/2')
        self.kalshi_ids = set()
        self.polymarket_ids = set()

    def cosine_similarity(self, a, b):
        # Normalize and compute cosine similarity
        a_norm = tf.nn.l2_normalize(a, axis=1)
        b_norm = tf.nn.l2_normalize(b, axis=1)
        return tf.matmul(a_norm, b_norm, transpose_b=True)

    def find_similar_markets(self, poly_df, kalshi_df):
        # Encode all questions/titles
        poly_embeddings = self.model(poly_df['question'].tolist())
        kalshi_embeddings = self.model(kalshi_df['full_title'].tolist())

        # Calculate similarity matrix
        similarity_matrix = self.cosine_similarity(poly_embeddings, kalshi_embeddings)


        similar_pairs = []
        for i, _ in poly_df.iterrows():
            for j, _ in kalshi_df.iterrows():
                similarity_score = similarity_matrix[i][j]
                if poly_df.iloc[i]['id'] not in self.polymarket_ids and kalshi_df.iloc[j]['ticker'] not in self.kalshi_ids:
                    self.kalshi_ids.add(kalshi_df.iloc[j]['ticker'])
                    self.polymarket_ids.add(poly_df.iloc[i]['id'])
                    if similarity_score > 0.7:
                      similar_pairs.append({
                            'poly_question': poly_df.iloc[i]['question'],
                            'kalshi_title': kalshi_df.iloc[j]['full_title'],
                            'kalshi_id': kalshi_df.iloc[j]['ticker'],
                            'poly_id': poly_df.iloc[i]['id'],
                            'similarity_score': float(similarity_score),
                        })
        if len(similar_pairs) == 0:
            return None
        return pd.DataFrame(similar_pairs)

#given list of words, return df with word counts
def count_words(strings):
    # Initialize a Counter to store word counts
    word_count = Counter()

    # List of words to exclude (stopwords)
    STOPWORDS = {"a", "in", "with", "and", "the", "is", "to", "of", "for", "on", "at", "by", "as", "an", "it", "are", "will", "next", "be", "announced", "be", "most", "high", "time", "all", "no", "us", "no", "there", "or", "not", "has", "not", "between", "start", "than", "another", "end", "more", "during", "times", "this", "out", "say", "1", "end", "2", "more", "3", "reach", "than", "times", "year", "4", "oh","fewer","am","into","live","now","inside","seven","its","before","new","his","member","after"}

    # Iterate through each string in the list
    for sentence in strings:
        # Convert sentence to lowercase and split into words
        words = re.findall(r'\b\w+\b', sentence.lower())  # \b matches word boundaries

        # Filter out stopwords and numbers
        filtered_words = [word for word in words if word not in STOPWORDS and not word.isnumeric()]

        # Update word count with the filtered words
        word_count.update(filtered_words)

    # Convert Counter to a list of dictionaries suitable for DataFrame
    result = [{'Word': word, 'Occurrences': count} for word, count in word_count.items()]

    # Create a DataFrame
    df = pd.DataFrame(result)

    # Return the DataFrame
    return df

def optimize_market_search(key_word_df, kalshi_markets, polymarket_markets):
    # Preprocess Kalshi market titles into sets of words for faster lookups
    kalshi_markets['market_words'] = kalshi_markets['title'].str.lower().apply(lambda x: set(re.findall(r'\b\w+\b', x)))

    # Preprocess Polymarket market titles into sets of words for faster lookups
    polymarket_markets['market_words'] = polymarket_markets['slug'].str.lower().apply(lambda x: set(re.findall(r'\b\w+\b', x)))

    # Iterate over each keyword
    for index_keywords, keyword_row in key_word_df.iterrows():
        keyword = keyword_row[0].lower()  # Lowercase keyword for case-insensitive matching

        # Get Kalshi market IDs where the keyword is in the market name
        kalshi_market_ids = kalshi_markets[kalshi_markets['market_words'].apply(lambda x: keyword in x)]['ticker'].tolist()

        # Get Polymarket market IDs where the keyword is in the market name
        polymarket_market_ids = polymarket_markets[polymarket_markets['market_words'].apply(lambda x: keyword in x)]['id'].tolist()

        # Assign the list of IDs to the respective columns in key_word_df
        key_word_df.at[index_keywords, 'Kalshi_Market_IDs'] = kalshi_market_ids
        key_word_df.at[index_keywords, 'Polymarket_Market_IDs'] = polymarket_market_ids

    return key_word_df

def get_key_words(polymarket_markets, kalshi_markets):
    #import markets csvs
    polymarket_markets = polymarket_markets
    polymarket_market_titles = polymarket_markets['slug'].tolist()

    kalshi_markets = kalshi_markets
    kalshi_market_titles = kalshi_markets['title'].tolist()


    polymarket_word_count = count_words(polymarket_market_titles).rename(columns = {"Word":"Word", "Occurrences":"Polymarket Occurrences"})
    kalshi_word_count = count_words(kalshi_market_titles).rename(columns = {"Word":"Word", "Occurrences":"Kalshi Occurrences"})

    combo_output = pd.merge(left=polymarket_word_count, right= kalshi_word_count, left_on= "Word", right_on= "Word")
    combo_output["Total Occurrences"] = combo_output['Polymarket Occurrences'] + combo_output['Kalshi Occurrences']
    key_word_df = combo_output.sort_values(by='Total Occurrences', ascending = True)
    key_word_df['Kalshi_Market_IDs'] = [[] for _ in range(len(key_word_df))]
    key_word_df['Polymarket_Market_IDs'] = [[] for _ in range(len(key_word_df))]
    key_word_df = key_word_df.loc[key_word_df['Total Occurrences']<150]

    word_ids =  optimize_market_search(key_word_df, kalshi_markets, polymarket_markets)
    return word_ids

def run_market_matcher(polymarket_markets, kalshi_markets):
    wholeTime = time.time()
    word_ids = get_key_words(polymarket_markets, kalshi_markets)
    matcher = MarketMatcher()
    count = 0
    logs = []

    similar_markets_df = pd.DataFrame()  # Initialize empty DataFrame
    
    #for word in word ids, call market matcher & pass through a df of all kalshi markets where word is in title and a df all polymarket markets where word is in title
    for index_word, key_word in word_ids.iterrows():
        kalshi_markets_with_word = kalshi_markets.loc[kalshi_markets['ticker'].isin(key_word['Kalshi_Market_IDs'])].reset_index(drop=True)
        polymarket_markets_with_word = polymarket_markets.loc[polymarket_markets['id'].isin(key_word['Polymarket_Market_IDs'])].reset_index(drop=True)
        timer = time.time()
        temp_similar_markets = matcher.find_similar_markets(polymarket_markets_with_word, kalshi_markets_with_word)
        endTime = time.time() - timer
        word = key_word['Word']
        if temp_similar_markets is None:
            logs.append({"msg":f"No markets over threshold found for: {word}"})
            continue
        print(f"Time to find similar markets: {endTime} for {word}")
        logs.append({"msg": f"Time to find similar markets: {endTime} for {word}"})
        similar_markets_df = pd.concat([similar_markets_df, temp_similar_markets], ignore_index=True)
        count += 1
    
    return similar_markets_df
"""
async def check_similarity(client, question_1, question_2):
    formatted_prompt = (
        f"Given the two questions below, please confirm in a one-word (\"Yes\" or \"No\") response "
        f"whether these questions are identical, meaning there is no possibility that they have different results:\n\n"
        f"1. {question_1}\n\n"
        f"2. {question_2}\n"
    )

    try:
        completion = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": formatted_prompt}
            ]
        )
        return completion.choices[0].message.content
    except Exception as e:
        print(f"Error processing questions: {e}")
        return "Error"

async def process_questions(df):
    # Read CSV
    
    # Initialize async OpenAI client
    client = AsyncOpenAI(api_key='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')
    
    # Create tasks for all question pairs
    tasks = []
    for _, row in df.iterrows():
        task = check_similarity(
            client,
            row['poly_question'],
            row['kalshi_title']
        )
        tasks.append(task)
    
    # Run all tasks concurrently
    results = await asyncio.gather(*tasks)
    
    # Add results to original dataframe
    df['is_similar'] = results
    
    return df

async def run_similarity_checker(dataframe):
    df = await process_questions(dataframe)
    print(f"Processed {len(df)} question pairs")
    print("\nSample results:")
    print(df[['poly_question', 'kalshi_title', 'is_similar']].head())

    # Write back to original CSV
    return df
"""
   

async def main():
    logging.basicConfig(level=logging.INFO)
    polyMarketApi = PolyMarketAPI()
    kalshiApi = KalshiAPI(os.getenv("KALSHI_EMAIL"), os.getenv("KALSHI_PASSWORD"))
    
    logging.info("Fetching market data...")
    polyMarkets = polyMarketApi.get_markets()
    kalshiMarkets = kalshiApi.get_markets()
    
    logging.info(f"Total markets saved: {len(polyMarkets) + len(kalshiMarkets)}")
    df =  run_market_matcher(polyMarkets, kalshiMarkets)
    # final_results = await run_similarity_checker(df)
    df.to_csv('similar_markets.csv', index=False)

if __name__ == "__main__":
    asyncio.run(main())


