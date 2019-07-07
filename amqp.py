import json
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


class Client:

    def __init__(self, username=None, password=None, credential_file='credentials.json'):

        if username is None and password is None:
            try:
                config = self.get_config(credential_file)
                self.username = config['username']
                self.password = config['password']
            except FileNotFoundError:
                print(f"Unable to find file {credential_file}")
                exit(1)
            except KeyError as e:
                print(str(e))
                exit(1)
        else:
            self.username = username
            self.password = password

    def get_config(self, credential_file):

        with open(credential_file, 'r') as config_file:
            config = json.loads(config_file.read())

        return config

    def send_messages(self, url, messages, address, QoS=1):
        sender_class = Send(url=url,
                            address=address,
                            messages=messages,
                            username=self.username,
                            password=self.password,
                            QoS=QoS)
        Container(sender_class).run()

        return {"messages": sender_class.total_messages,
                "messages_sent": sender_class.sent,
                "messages_confirmed": sender_class.confirmed,
                "messages_accepted": sender_class.accepted,
                "messages_rejected": sender_class.rejected}


class Send(MessagingHandler):
    def __init__(self, url, address, messages, username=None, password=None, QoS=1):
        super(Send, self).__init__()

        # amqp broker host url
        self.url = url

        # target amqp node address
        self.address = address

        # authentication credentials
        self.username = username
        self.password = password

        # the message durability flag must be set to True for persistent messages
        self.message_durability = True if QoS == 2 else False

        # messaging counters
        self.messages = messages
        self.total_messages = len(messages)
        self.current_message = 0

        self.sent = 0
        self.confirmed = 0  # all messages that were confirmed (rejected or accepted)
        self.accepted = 0  # confirmed messages that were accepted
        self.rejected = 0  # confirmed messages that were rejected

        # status
        self.conn_status = 0

    def on_start(self, event):
        # select connection authenticate
        if self.username is not None:
            # creates and establishes an amqp connection with the user credentials
            conn = event.container.connect(url=self.url,
                                           user=self.username,
                                           password=self.password,
                                           allow_insecure_mechs=True)
        else:
            # creates and establishes an amqp connection with anonymous credentials
            print("Warning: Attempting to connect **without** username and password")
            conn = event.container.connect(url=self.url)

        if conn:
            # attaches sender link to transmit messages
            event.container.create_sender(conn, target=self.address)

    def on_sendable(self, event):
        """
        on_sendable is only called when que is free to be sent
        it might be stopped in the middle of transmitting messages and resumed later on
        """
        while event.sender.credit and self.sent < self.total_messages:
            msg = Message(id=(self.sent+1),
                          body=self.messages[self.current_message],
                          durable=self.message_durability)
            event.sender.send(msg)
            self.current_message += 1
            self.sent += 1

        self.conn_status = event.connection.state

    def on_accepted(self, event):
        self.confirmed += 1
        self.accepted += 1
        if self.confirmed == self.total_messages:
            print("all messages confirmed")
            event.connection.close()

        self.conn_status = event.connection.state

    def on_rejected(self, event):
        self.confirmed += 1
        self.rejected += 1
        print("Broker", self.url, "Reject message:", event.delivery.tag)
        if self.confirmed == self.total_messages:
            event.connection.close()

        self.conn_status = event.connection.state

    # catches event for socket and authentication failures
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)
        MessagingHandler.on_transport_error(self, event)
        self.conn_status = event.connection.state

    def on_disconnected(self, event):
        if event.transport and event.transport.condition:
            print('disconnected with error : ', event.transport.condition)
            event.connection.close()

        self.sent = self.confirmed
        self.conn_status = event.connection.state


