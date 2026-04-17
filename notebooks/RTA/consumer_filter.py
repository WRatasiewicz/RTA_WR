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
# Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT
for message in consumer:
    wartosc_transakcji = message.value['amount']
    if wartosc_transakcji> 1000:
        print("%.2f: ALERT" % wartosc_transakcji)
