import psycopg2
import random
import requests
from confluent_kafka import SerializingProducer
import json


BASE_URL = 'https://randomuser.me/api/?nat=in' # random api which generate random user

PARTIES = ['Bharosa Jumla Party ','Confuse Party','Angry Aadmi Party']

random.seed(21)
def create_table(conn,cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates(
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT, 
            photo_url TEXT
        )
        """
    )
   
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    
    conn.commit()

def generate_candidate_data(candidate_number,total_parties):
    response = requests.get(BASE_URL +'&gender=' + ('female' if candidate_number %2 == 1 else 'male') )
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        
        return {
            'candidate_id':user_data['login']['uuid'],
            'candidate_name':f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': PARTIES[candidate_number % total_parties],
            'biography': 'sab chor hai',
            'campaign_platform':"promises and lies",
            'photo_url':user_data['picture']['large']
            
            
        }
    else:
        return f"Error while fetching the data with status code {response.status_code}"
    
def generate_voters_data():
    
    res = requests.get(BASE_URL)
    if res.status_code == 200:
         user_data = res.json()['results'][0]
         return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return f"Error while fetching the data with status code {res.status_code}"
    
    
def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()

def delivery_report(err,msg):
    if err is not None:
        print(f"producer failed with {err}")
        
    else:
        print(f"message delivered to topic{ msg.topic()} at partition {msg.partition()}")
        
if __name__ == "__main__":
    producer = SerializingProducer({"bootstrap.servers":"localhost:9092"})
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()
        
        create_table(conn, cur)
        print("table created successfully")
        cur.execute(
    """
    SELECT * FROM candidates
    """
        )
        candidates = cur.fetchall()
        
        if len(candidates) == 0:
            for i in range(3):
                candidate = generate_candidate_data(i,3)
                print(i)
                print(candidate)
                cur.execute(
    """
    INSERT INTO candidates(candidate_id, candidate_name, party_affiliation, biography,campaign_platform,photo_url)
    VALUES(%s, %s, %s, %s, %s, %s)
    """,(
        candidate['candidate_id'], candidate['candidate_name'],candidate['party_affiliation'],
        candidate['biography'],candidate['campaign_platform'],candidate['photo_url']
    )
                )
        conn.commit()
     # generating voter data and inserting into the respective table 
        for i in range(500): # generating random 500 voters data
            voter_data =  generate_voters_data()
            insert_voters(conn,cur,voter_data)
            try:  
                producer.produce(
                    topic="voters-topic",
                    key=voter_data["voter_id"],
                    value=json.dumps(voter_data),
                    on_delivery=delivery_report
                )
            except Exception as e:
              print(f"error while connecting broker with error {e}")
              
              
            print(f"Produced voter {i} with data {voter_data}")
            
            producer.flush()
    except Exception as e:
        print(f"error - {e}")