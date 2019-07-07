import json

from proton.handlers import MessagingHandler
from proton.reactor import Container


class Recv(MessagingHandler):
    def __init__(self, url, address, count, username, password):
        super(Recv, self).__init__()

        # amqp broker host url
        self.url = url

        # amqp node address
        self.address = address

        # authentication credentials
        self.username = username
        self.password = password

        # messaging counters
        self.expected = count
        self.received = 0

    def on_start(self, event):
        # select authentication options for connection
        if self.username:
            # basic username and password authentication
            conn = event.container.connect(url=self.url,
                                           user=self.username,
                                           password=self.password,
                                           allow_insecure_mechs=True)
        else:
            # Anonymous authentication
            conn = event.container.connect(url=self.url)
        # create receiver link to consume messages
        if conn:
            event.container.create_receiver(conn, source=self.address)

    def on_message(self, event):
        if event.message.id and event.message.id < self.received:
            # ignore duplicate message
            return
        if self.expected == 0 or self.received < self.expected:
            print(event.message.body)
            self.received += 1
            if self.received == self.expected:
                print('received all', self.expected, 'messages')
                event.receiver.close()
                event.connection.close()

    # the on_transport_error event catches socket and authentication failures
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)
        MessagingHandler.on_transport_error(self, event)

    def on_disconnected(self, event):
        print("Disconnected")


def get_config():
    with open('credentials.json', 'r') as config_file:
        config = json.loads(config_file.read())

    return config


if __name__ == '__main__':
    config = get_config()

    try:
        Container(Recv(url=config['url'],
                       address=config['addresses'][0],
                       count=10,
                       username=config['username'],
                       password=config['password'])).run()
    except KeyboardInterrupt:
        pass
