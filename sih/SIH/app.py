from flask import Flask, render_template, request, redirect, url_for
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB configuration
mongo_uri = "mongodb://localhost:27017/"
client = MongoClient(mongo_uri)
db = client["SIH_DB"]
collection = db["Processed_News"]

@app.route("/")
def front():
    return render_template("front.html")
@app.route("/index.html", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        selected_ministry = request.form.get("ministry")
        selected_sentiment = request.form.get("sentiment")

        # Redirect to the news_titles route with query parameters for ministry and sentiment
        return redirect(url_for("news_titles", ministry=selected_ministry, sentiment=selected_sentiment))

    # Handle the initial load of the page here
    return render_template("index.html", ministries=get_ministries())


@app.route("/news_titles")
def news_titles():
    ministry = request.args.get("ministry")
    sentiment = request.args.get("sentiment")

    # Handle cases where ministry or sentiment is None (e.g., set default values)
    if not ministry or not sentiment:
        ministry = "Ministry of Finance"
        sentiment = "positive"

    # Query the database to get news titles based on selected ministry and sentiment
    news_data = get_news_data(ministry, sentiment)  

    return render_template("news_titles.html", news_data=news_data)


def get_ministries():
    # You can retrieve the list of ministries from your database or define it here
    ministries = ["Ministry of Agriculture and Farmers Welfare", "Ministry of Finance"]
    return ministries

def get_news_data(ministry, sentiment):
    # Query the database for news titles based on ministry and sentiment
    # You will need to implement this query based on your data structure
    news_data = []

    # Sample query (you should modify this to match your database structure)
    query = {"Ministries": ministry, "Sentiment": sentiment}
    results = collection.find(query)

    for result in results:
        news_data.append({"title": result["title"],"description": result.get("description", ""),"link": result.get("link", "")})

    return news_data
def get_news_description(title):
    description = ""
    query = {"title":title}
    result= collection.find_one(query)
    if result:
        description = result.get("description", "")
    return description
def replace_special_characters(title):
    return ''.join(c if c.isalnum() else '_' for c in title)
app.jinja_env.filters['replace_special_characters'] = replace_special_characters
# Loop through the documents and convert each ObjectID to a string
    


        


if __name__ == "__main__":
    app.run(debug=True)