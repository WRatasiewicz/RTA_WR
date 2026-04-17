from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'koniec',
    bootstrap_servers='broker:9092', #serverS bo jednym konsumentem moge pobierac info z wielu brokerow
    #uto_offset_reset='earliest',
    #group_id='filter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) #binarny kod na utf8
)

# TWÓJ KOD
# Napisz konsumenta, który dodaje pole risk_level: - amount > 3000 → “HIGH” - amount > 1000 → “MEDIUM” - reszta → “LOW”
for message in consumer:
    #wyciagniecie danych transakcji - slownik
    tx = message.value
    wartosc_transakcji = message.value['amount']
    
    if wartosc_transakcji > 3000:
        risk = 'HIGH'
    elif wartosc_transakcji > 1000:
        risk = 'MEDIUM'
    else:
        risk = 'LOW' 
    
    tx['risk_level'] = risk

    print(f"ID: {tx['tx_id']} | Kwota: {tx['amount']:>8.2f} | Ryzyko: {tx['risk_level']}")
