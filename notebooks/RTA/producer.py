#jak odpale komorke to ona sie nie uruchomi, kod zapisze się do pliku producer.py
from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') #serializacja, zeby broker (napisany w javie) odbieral slowniki napisane w pythonie
)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def generate_transaction():
    #slownik, jakbysmy chcieli w projekcie uzyc danych np. z csv to musielibysmy je zamienic na slownik, przed przesylaniem ich strumieniowo
    czas = datetime.now() # Pobieramy czas
    tx = {
        'tx_id': f'TX{random.randint(1000,9999)}',
        'user_id': f'u{random.randint(1,20):02d}',
        'amount': round(random.uniform(5.0, 5000.0), 2),
        'store': random.choice(sklepy),
        'category': random.choice(kategorie),
        'hour': czas.hour,
        'timestamp': czas.isoformat(), #znacznik czasowy - czas wygenerowania zdarzenia
    }

    # 2. dla 5% transakcji nadpisujemy pola tak, żeby spełniała warunki podejrzanej
    
    if random.random() < 0.05:
        tx['amount'] = round(random.uniform(3000.0, 5000.0), 2)
        tx['category'] = 'elektronika'
        tx['hour'] = random.randint(0, 5)
    return tx



for i in range(100):
    tx = generate_transaction()
    producer.send('koniec', value=tx) #koniec - nazwa topicu
    print(f"[{i+1}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
    time.sleep(1)

producer.flush()
producer.close()
