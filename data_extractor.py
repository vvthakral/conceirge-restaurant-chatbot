import requests
import json
from datetime import datetime

result = []


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r
my_token = #Token here

restaurants = ['japanese','indian','chinese','italian','mexican','thai']
results = []

#[id name display_address coordinates review_count rating zip_code]
for r in restaurants:
    offset = 0
    while offset<1000:
        response = requests.get(f'https://api.yelp.com/v3/businesses/search?term={r}&categories=restaurants&location=manhattan&limit=50&offset={offset}', auth=BearerAuth(my_token))
        rj = response.json()['businesses']
        print(f"Done! {r} : {offset}")
        for i in range(len(rj)):
            present = {}
            present['id'] = rj[i]['id']
            present['name'] = rj[i]['name']
            present['display_address'] = rj[i]['location']['display_address']
            present['coordinates'] = rj[i]['coordinates']
            present['review_count'] = rj[i]['review_count']
            present['rating'] = rj[i]['rating']
            present['zip_code'] = rj[i]['location']['zip_code']
            present['category'] = r
            present['insertedAtTimestamp'] = datetime.now().isoformat()
            results.append(present)            
        offset+=50
        if r =='indian' and offset>=900:
            break
print(len(results))
a = {}
a['restaurants'] = results
with open('data.json', 'w') as f:
    json.dump(a, f)