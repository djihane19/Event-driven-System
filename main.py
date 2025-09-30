from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
import json 
import consumers
import time
from typing import Optional 

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)
#add th infos from redis db 
redis = get_redis_connection(
    host="redis-...redis-cloud.com",
    port=...,
    password="uX9Ee..3zO",
    decode_responses=True
)

class Delivery(HashModel):
    budget: int = 0
    notes: str = ''

    class Meta:
        database = redis 

class Event(HashModel):
    delivery_id: Optional[str] = None 
    type: str 
    data: str
    timestamp: float = None

    class Meta:
        database = redis

    def __init__(self, **data):
        if 'timestamp' not in data or data['timestamp'] is None:
            data['timestamp'] = time.time()
        super().__init__(**data)

@app.get('/deliveries/{pk}/status')
async def get_state(pk: str):
    # Check cache first
    state = redis.get(f'delivery:{pk}')
    if state is not None:
        return json.loads(state)
    
    # Build state from events
    state = build_state(pk)
    # Cache the result
    redis.set(f'delivery:{pk}', json.dumps(state))
    return state

def build_state(pk: str):
    # Get ALL event PKs from Redis
    pks = Event.all_pks()
    
    all_events = []
    for event_pk in pks:
        try:
            event = Event.get(event_pk)
            all_events.append(event)
        except Exception as e:
            print(f"Error loading event {event_pk}: {e}")
            continue
    
    # Filter events for this specific delivery
    events = [event for event in all_events if event.delivery_id == pk]
    
    # Sort events by timestamp (oldest first)
    events.sort(key=lambda x: x.timestamp)
    
    # Apply events in chronological order
    state = {}
    for event in events:
        try:
            state = consumers.CONSUMERS[event.type](state, event)
        except Exception as e:
            print(f"Error processing event {event.pk}: {e}")
            # You might want to handle this differently based on your needs
            continue
    
    return state

@app.post('/deliveries/create')
async def create(request: Request):
    body = await request.json()
    delivery = Delivery(budget=body['data']['budget'], notes=body['data']['notes']).save()
    event = Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data'])).save()
    
    # Process the event to get initial state
    state = consumers.CONSUMERS[event.type]({}, event)
    
    # Cache the state
    redis.set(f'delivery:{delivery.pk}', json.dumps(state))
    return state

@app.post('/event')
async def dispatch(request: Request):
    body = await request.json()
    delivery_id = body['delivery_id']
    
    # Create the event
    event = Event(
        delivery_id=delivery_id,
        type=body['type'],
        data=json.dumps(body['data'])
    ).save()
    
    # Get current state
    state = await get_state(delivery_id)
    
    # Apply the new event to get new state
    new_state = consumers.CONSUMERS[event.type](state, event)
    
    # Update cache
    redis.set(f'delivery:{delivery_id}', json.dumps(new_state))
    return new_state