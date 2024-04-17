import requests
from pymongo import MongoClient
from time import sleep

Ministries = {
    'Ministry of Agriculture and Farmers Welfare': ["Agriculture", 'Farmers', 'Soil'],
    'Ministry of Finance': ["Economic affairs", "Expenditure", "Financial Services"]
}

client = MongoClient("mongodb://localhost:27017/")  # Update with your MongoDB connection details
db = client["SIH_DB"]
collection = db["NEWS_API"]
news_data = []

Ministries = {'Ministry of Agriculture and Farmers Welfare':["Agriculture",'Farmers','Soil'],
             'Ministry of Finance':["Economic affairs","Expenditure","Financial Services"]}
news_data = []
for ministry in Ministries:
    departments = Ministries[ministry]
    for department in departments:
        url = "https://newsdata.io/api/1/news?apikey=pub_29486374675aa2dc4cd5f6a047a072a0cafcd&language=en&country=in&q=%s"%department
        for articles in range(0,10):
            response = requests.get(url)
            file1 = response.json()
            try:
                print(file1)
                for news in file1['results']:
                    collection.insert_one({'title':news['title'],'content':news['content'],'description':news['description'],'link':news['link'],'language':news['language'],'department':department})
                nextpage = file1.get('nextPage',None)
                if not nextpage:
                    break
                url = 'https://newsdata.io/api/1/news?apikey=pub_29486374675aa2dc4cd5f6a047a072a0cafcd&language=en&country=in&q=%s&page=%s'%(department,nextpage)
            except TypeError:
                print('Rate limit exceeded')
                sleep(900)