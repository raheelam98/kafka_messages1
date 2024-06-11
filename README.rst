kafka messaging
===============

AIOKafkaProducer Todo
*********************

.. code-block:: python

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


AIOKafkaConsumer Todo
*********************
.. code-block:: python

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


add aiokafka::

    poetry add aiokafka

clone repository::
    
    git clone <repository-url>    


https://github.com/aio-libs/aiokafka