import requests
from pymongo import MongoClient
import asyncio
from time import sleep
import pandas as pd
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Define the NewsData fetching coroutine
async def fetch_news_data_and_process_ml():
    Ministries = {
        'Ministry of Agriculture and Farmers Welfare': ["Agriculture", 'Farmers', 'Soil'],
        'Ministry of Finance': ["Economic affairs", "Expenditure", "Financial Services"],
        'Ministry of Water Resources':['River','Sanitation','Water logging'],
        'Ministry of Education':['Student','Exam','Education'],
        "Ministry of Environment,Forest and climate change":['Environment','Forest','Disaster']
    }

    client = MongoClient("mongodb://localhost:27017/")  # Update with your MongoDB connection details
    db = client["SIH_DB"]
    collection = db["NEWS_API"]


    while True:
        count = 0
        for ministry in Ministries:
            departments = Ministries[ministry]
            for department in departments:
                url = "https://newsdata.io/api/1/news?apikey=pub_29519e597861d0908e59adaad566cbd825760&language=en&country=in&q=%s"%department
                for i in range(0,10):
                    if count == 199:
                        break
                    response = requests.get(url)
                    count = count + 1
                    print(count)
                    file1 = response.json()
                    try:
                        print(file1)
                        for news in file1['results']:
                            collection.insert_one({'title':news['title'],'content':news['content'],'description':news['description'],'link':news['link'],'language':news['language'],'department':department})
                        nextpage = file1.get('nextPage',None)
                        if not nextpage:
                            break
                        url = 'https://newsdata.io/api/1/news?apikey=pub_29519e597861d0908e59adaad566cbd825760&language=en&country=in&q=%s&page=%s'%(department,nextpage)
                    except TypeError:
                        count=count-1
                        print('Rate limit exceeded, switching to ML processing...',count) 

                        print('Resuming data fetching after 15 minutes...')
                        await asyncio.sleep(900)  # Sleep for 15 minutes after a rate limit error
                if count == 199:
                    break 
            if count == 199:
                break 
        
        await process_ml_data()

        

# Define the ML processing coroutine
async def process_ml_data():
    while True:
        mongo_uri = "mongodb://localhost:27017/"

        client = MongoClient(mongo_uri)
        db = client["SIH_DB"]
        collection = db["NEWS_API"]

        data = collection.find({})  

        df = pd.DataFrame(list(data))

        #tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")
        #model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")
        sentiment = pipeline("text-classification", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
        Ministries = {'Ministry of Agriculture and Farmers Welfare':["Agriculture",'Farmers','Soil'],
                     'Ministry of Finance':["Economic affairs","Expenditure","Financial Services"],
                     'Ministry of Water Resources':['River','Sanitation','Water logging'],
                    'Ministry of Education':['Student','Exam','Education'],
                    "Ministry of Environment,Forest and climate change":['Environment','Forest','Disaster']}
        news = df.copy()
        #news.drop(columns = 'sourceName',axis = 1,inplace = True)
        new_df = news.copy()
        new_df.drop_duplicates(subset = 'title',keep = 'first',inplace = True)
        new_df.dropna(inplace = True)
        length = new_df['title'].apply(lambda x: len(x))
        sentiments = new_df['title'].apply(lambda x: sentiment(x)[0]['label'])
        new_df['Sentiment'] = sentiments
        ministries = new_df['department'].apply(lambda x: next((ministry for ministry, depts in Ministries.items() if x in depts), None))
        new_df['Ministries'] = ministries

        mongo_uri = "mongodb://localhost:27017/"

        try:
            client = MongoClient(mongo_uri)

            db = client["SIH_DB"]

            new_collection = db["Processed_News"]

            # Convert the processed DataFrame (new_df) to a list of dictionaries
            processed_data = new_df.to_dict(orient='records')

            # Insert the processed data into the new collection
            new_collection.insert_many(processed_data)

            print(f"{len(processed_data)} documents successfully inserted into the new collection.")

        except Exception as e:
            print(f"Error: {str(e)}")

        finally:
            # Close the MongoDB connection
            client.close()

        await asyncio.sleep(86400) 

# Create and run both coroutines concurrently

# Create and run the combined coroutine
async def main():
    await fetch_news_data_and_process_ml()

if __name__ == "__main__":
    asyncio.run(main())



