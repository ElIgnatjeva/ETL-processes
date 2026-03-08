import random
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from faker import Faker

fake = Faker()
client = MongoClient('mongodb://admin:admin123@localhost:27017/')
db = client['ecommerce_db']

for collection in ['user_sessions', 'event_logs', 'support_tickets', 
                   'user_recommendations', 'moderation_queue']:
    db[collection].delete_many({})

USER_IDS = [f"user_{i:03d}" for i in range(1, 51)]
PRODUCT_IDS = [f"prod_{i:03d}" for i in range(1, 21)]
PAGES = ['/home', '/products', '/products/{id}', '/cart', '/checkout', 
         '/profile', '/support', '/about']
ACTIONS = ['login', 'view_product', 'add_to_cart', 'remove_from_cart', 
           'checkout_start', 'payment', 'logout', 'search']

print("Генерация UserSessions...")
sessions_collection = db['user_sessions']
sessions = []

for i in range(1, 201): 
    user_id = random.choice(USER_IDS)
    start_time = fake.date_time_between(start_date='-30d', end_date='now')
    duration = random.randint(60, 3600) 
    end_time = start_time + timedelta(seconds=duration)
    
    pages_visited = []
    current_time = start_time
    while current_time < end_time:
        page = random.choice(PAGES).format(id=random.choice(PRODUCT_IDS)[5:])
        pages_visited.append(page)
        current_time += timedelta(minutes=random.randint(1, 10))
    
    actions = random.sample(ACTIONS, random.randint(2, 5))
    
    session = {
        "session_id": f"sess_{i:03d}",
        "user_id": user_id,
        "start_time": start_time.isoformat() + "Z",
        "end_time": end_time.isoformat() + "Z",
        "pages_visited": pages_visited[:10],
        "device": {"type": random.choice(['mobile', 'desktop', 'tablet'])},
        "actions": actions,
        "created_at": datetime.utcnow()
    }
    sessions.append(session)

sessions_collection.insert_many(sessions)
print(f"  Добавлено {len(sessions)} сессий")

print("Генерация EventLogs...")
events_collection = db['event_logs']
events = []

event_types = ['click', 'page_view', 'api_call', 'error', 'purchase']
for i in range(1, 1001):
    event_type = random.choice(event_types)
    details = {}
    
    if event_type == 'click':
        details = {"element": random.choice(['button', 'link', 'image']), 
                   "page": random.choice(PAGES)}
    elif event_type == 'purchase':
        details = {"order_id": f"order_{i:04d}", 
                   "amount": round(random.uniform(10, 500), 2)}
    
    event = {
        "event_id": f"evt_{i:04d}",
        "timestamp": fake.date_time_between(start_date='-30d', end_date='now').isoformat() + "Z",
        "event_type": event_type,
        "user_id": random.choice(USER_IDS),
        "session_id": f"sess_{random.randint(1, 200):03d}",
        "details": details
    }
    events.append(event)

events_collection.insert_many(events)
print(f"  Добавлено {len(events)} событий")

print("Генерация SupportTickets...")
tickets_collection = db['support_tickets']
tickets = []

issue_types = ['payment', 'technical', 'delivery', 'return', 'account']
statuses = ['open', 'in_progress', 'resolved', 'closed']

for i in range(1, 101):
    user_id = random.choice(USER_IDS)
    created_at = fake.date_time_between(start_date='-30d', end_date='now')
    updated_at = created_at + timedelta(hours=random.randint(1, 48))
    
    messages = []
    msg_count = random.randint(1, 5)
    for j in range(msg_count):
        msg_time = created_at + timedelta(hours=j * random.randint(2, 8))
        messages.append({
            "sender": random.choice(['user', 'support']),
            "message": fake.sentence(),
            "timestamp": msg_time.isoformat() + "Z"
        })
    
    ticket = {
        "ticket_id": f"ticket_{i:03d}",
        "user_id": user_id,
        "status": random.choice(statuses),
        "issue_type": random.choice(issue_types),
        "messages": messages,
        "created_at": created_at.isoformat() + "Z",
        "updated_at": updated_at.isoformat() + "Z"
    }
    tickets.append(ticket)

tickets_collection.insert_many(tickets)
print(f"  Добавлено {len(tickets)} тикетов")

print("Генерация UserRecommendations...")
recs_collection = db['user_recommendations']
recommendations = []

for user_id in USER_IDS:
    rec = {
        "user_id": user_id,
        "recommended_products": random.sample(PRODUCT_IDS, random.randint(3, 6)),
        "last_updated": datetime.utcnow().isoformat() + "Z"
    }
    recommendations.append(rec)

recs_collection.insert_many(recommendations)
print(f"  Добавлено {len(recommendations)} рекомендаций")

print("Генерация ModerationQueue...")
moderation_collection = db['moderation_queue']
moderation_items = []

for i in range(1, 151):
    moderation_status = random.choice(['pending', 'approved', 'rejected'])
    flags = []
    if random.random() > 0.7:
        flags.append('contains_images')
    if random.random() > 0.8:
        flags.append('contains_links')
    
    review = {
        "review_id": f"rev_{i:03d}",
        "user_id": random.choice(USER_IDS),
        "product_id": random.choice(PRODUCT_IDS),
        "review_text": fake.paragraph(),
        "rating": random.randint(1, 5),
        "moderation_status": moderation_status,
        "flags": flags,
        "submitted_at": fake.date_time_between(start_date='-30d', end_date='now').isoformat() + "Z"
    }
    moderation_items.append(review)

moderation_collection.insert_many(moderation_items)
print(f"  Добавлено {len(moderation_items)} отзывов")

print("Создание индексов...")
db['user_sessions'].create_index([('user_id', ASCENDING)])
db['user_sessions'].create_index([('start_time', ASCENDING)])
db['event_logs'].create_index([('user_id', ASCENDING)])
db['event_logs'].create_index([('timestamp', ASCENDING)])
db['support_tickets'].create_index([('user_id', ASCENDING)])
db['support_tickets'].create_index([('status', ASCENDING)])

print("✅ Генерация данных завершена успешно!")

print("\n📊 Статистика по коллекциям:")
for collection in db.list_collection_names():
    count = db[collection].count_documents({})
    print(f"  {collection}: {count} документов")