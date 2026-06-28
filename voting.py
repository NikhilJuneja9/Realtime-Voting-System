import psycopg2
from confluent_kafka import Consumer , KafkaError, KafkaException, SerializingProducer
import simplejson as json
import random
import pytz
from main import delivery_report
import time

from datetime import datetime

conf = {
    'bootstrap.servers':'localhost:9092'
}

consumer = Consumer(conf | { # PIPE operator is used to merge two dict
    'group.id':'voting-group',
    'auto.offset.reset':'earliest',
    'enable.auto.commit':False
})

producer = SerializingProducer(conf=conf)

if __name__ ==  "__main__":
    
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres" )
    curr = conn.cursor()
    
    # converting the candidate column and row into json
    curr.execute(
    """ select row_to_json(cols) from (SELECT * FROM candidates) as cols
    
    """
    )
    candidates = [candidate[0] for candidate in curr.fetchall()]
    
    if len(candidates) ==0:
        raise Exception("No candidate found in the database")
  
        
    # consumer.subscribe(['voters-topic'])
    try:
            curr.execute("select row_to_json(cols) FROM (SELECT * FROM voters) as cols")
            voters = curr.fetchall()
            
            for voter in voters:  
                voter = voter[0]

                chosen_candidate =  random.choice(candidates)
                print("candidate is chosen")
                vote =  voter | chosen_candidate | {
                    "voting_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
                    "vote":1
                }
                print(vote)
                
                try:
                    print('user {} is voting for candidate {}'.format(vote['voter_id'], vote['candidate_id']))
                    curr.execute("""
                                    INSERT INTO votes(voter_id,candidate_id,voting_time)
                                    VALUES (%s, %s, %s)
                                    """,(vote['voter_id'], vote['candidate_id'],vote['voting_time']))
                    conn.commit()
            
                except  Exception as e:
                    print(f"error while storing the vote in psql with exception {e}")
                
                try:
                    producer.produce(
                        topic='votes_topic',
                        key = vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    producer.flush()
    
                except  Exception as e:
                    print(f"error while sending the vote over kafka with exception {e}")

                time.sleep(20)

    except Exception as e:
        print(e)
    