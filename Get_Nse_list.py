import requests
import pandas as pd
import io
import pymongo

USERNAME = "XXXXXXXXX"
DB_PASS = "XXXXXXXXX"

def fetchlist(db):
    response = requests.get("https://www1.nseindia.com/content/equities/EQUITY_L.csv")

    csv = response.content

    csv_df = pd.read_csv(io.StringIO(csv.decode('utf-8')))

    db.Nse_Stocks_List.find_one_and_update({},{"$set": {"List" : csv_df["SYMBOL"].tolist()}})

def main():
	client = pymongo.MongoClient(
            f"mongodb+srv://{USERNAME}:{DB_PASS}@cluster0.toqqc.mongodb.net/Cluster0?retryWrites=true&w=majority"
        )
	db = client.Stocks
	fetchlist(db)
    
if __name__ == "__main__":
    main()
    