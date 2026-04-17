from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'koniec',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. store_counts[store] += 1
#   2. total_amount[store] += amount
#   3. Co 10 wiadomości: print tabela

for message in consumer:
    tx = message.value
    store = tx['store']
    store_counts[store] += 1
    total_amount[store] += tx['amount']
    if msg_count % 10 == 0:  # co 10 wiadomości
        
        print(f"\n{'--- RAPORT SPRZEDAŻY':^45}") # ^ centruje tekst
            # Nagłówek tabeli: < oznacza wyrównanie do lewej, > do prawej, cyfra to szerokość
        print(f"{'Sklep':<15} | {'Transakcje':<12} | {'Suma (PLN)':>12}")
        print("-" * 45)
        
        for store in sorted(store_counts.keys()):
            count = store_counts[store]
            amount = total_amount[store]
            print(f"{store:<15} | {count:<12} | {amount:>12.2f}")
        
        print("-" * 45 + "\n")

    msg_count += 1

