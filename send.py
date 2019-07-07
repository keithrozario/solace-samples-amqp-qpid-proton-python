import amqp

if __name__ == '__main__':

    messages = [f"Message: {x}" for x in range(100)]
    client = amqp.Client()
    response = client.send_messages(messages=messages,
                                    address="Que_One",
                                    url="amqps://mr8ksiwsp23vv.messaging.solace.cloud:21037")
    print(response)
