from kafka import KafkaConsumer, KafkaProducer
import json
import time



def score_transaction(tx):
    score = 0
    rules = []
    # TWÓJ KOD — zaimplementuj reguły R1, R2, R3
    # R1: Wysoka kwota
    if tx['amount'] > 3000:
        score += 3
        rules.append("R1: Kwota > 3000")

    # R2: Elektronika i kwota>1500
    if tx['category'] == 'elektronika'and tx['amount'] > 1500:
        score += 2
        rules.append("R2: Elektronika i kwota > 1500")

    # R3: Godzina nocna 
    if 0 <= tx['hour'] <= 5:
        score += 2
        rules.append("R3: Godzina nocna")
    
    return score, rules

# Test
#test_tx = {'tx_id': 'TX999', 'amount': 4500.0, 'category': 'elektronika',
#           'timestamp': '2026-04-01T03:15:00', 'hour': 3}
#print(score_transaction(test_tx))  # powinno dać score >= 5


consumer = KafkaConsumer('koniec', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    tx = message.value
    tx_score, rules = score_transaction(tx)
    
    if tx_score >= 3:
        alert_payload = {
            'tx_id': tx['tx_id'],
            'score': tx_score,
            'rules': rules,
            'original_tx': tx  # dołączamy dane transakcji dla analityka
        }
        alert_producer.send('alerts', value=alert_payload)
        print(f"UWAGA! Wysłano alert dla {tx['tx_id']} (Score: {tx_score})")

    time.sleep(1)
        
