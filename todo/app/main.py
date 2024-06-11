
# main.py
from fastapi import FastAPI, Depends, HTTPException, Body
from contextlib import asynccontextmanager
from typing import Union, Optional, Annotated
from app import settings
from sqlmodel import Field, Session, SQLModel, create_engine, select
from typing import AsyncGenerator

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

class Todo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    content: str = Field(index=True)


# only needed for psycopg 3 - replace postgresql
# with postgresql+psycopg in settings.DATABASE_URL
connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

# recycle connections after 5 minutes
# to correspond with the compute scale down
engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)

def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)

async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-group",
        auto_offset_reset='earliest'
    )

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print(f"Consumer Received message: {message.value.decode()} on topic {message.topic}")
            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()    


# The first part of the function, before the yield, will
# be executed before the application starts.
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables........")
    task = asyncio.create_task(consume_messages('todos', 'broker:19092'))
    create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan, title="API with DB")    

# app = FastAPI(lifespan=lifespan, title="API with DB", 
#     version="0.0.1",
#     servers=[
#         {
#             "url": "http://127.0.0.1:8001", # ADD NGROK URL Here Before Creating GPT Action
#             "description": "Development Server"
#         }
#         ])

def get_session():
    with Session(engine) as session:
        yield session

@app.get("/")
def read_root():
    return {"Kafka Messages": "Postgres DB"}

# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@app.post("/api_todos/", response_model=Todo)
async def create_todo(todo: Todo, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)])->Todo:
        todo_dict = {field: getattr(todo, field) for field in todo.dict()}
        todo_json = json.dumps(todo_dict).encode("utf-8")
        print("todoJSON :  ", todo_json)
        # Produce message
        await producer.send_and_wait("todos", todo_json)
        session.add(todo)
        session.commit()
        session.refresh(todo)
        return todo

@app.get("/api_todos/", response_model=list[Todo])
def read_todos(session: Annotated[Session, Depends(get_session)]):
        todos = session.exec(select(Todo)).all()
        return todos

# @app.put("/api_update_todo", response_model=Todo)
# async def update_todo(todo : Todo, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)] )->Todo:
#         todo_dict = {field: getattr(todo, field) for field in todo.dict()}
#         todo_json = json.dumps(todo_dict).encode("utf-8")
#         print("todoJSON :  ", todo_json)
#         # Produce message
#         await producer.send_and_wait("todos", todo_json)
#         get_id : Todo = select(Todo).where(Todo.id == todo)
#         update_todo = session.exec(get_id).first()
#         if not update_todo:
#             raise HTTPException(status_code=404, detail=(f"Todo Id: {todo} Not Found In DB"))
#         else:
#             update_todo.content = todo.content
#             session.add(update_todo)
#             session.commit()
#             session.refresh(update_todo)
#             return  update_todo


