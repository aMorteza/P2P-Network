import threading


class Neighbor(threading.Thread):
    """The class Neighbor contains the client node socket, ip and host and hold the server node id. Communication
    is done by this class. The server node sending message, pass through to the main node.
    Instantiates a new Neighbor.
    main_node: The Node class that received a connection.
    sock: The socket that is associated with the client connection.
    id: The id of the connected node.
    host: The ip of the main node.
    port: The port of the server of the main node."""

    def __init__(self, main_node, sock, id, host, port, last_hello_time):
        super(Neighbor, self).__init__()

        self.host = host
        self.port = port
        self.main_node = main_node
        self.sock = sock
        self.last_hello_time = last_hello_time
        self.terminate_flag = threading.Event()

        # The connected node index
        self.id = id

        self.main_node.debug_print(
            "Neighbor Node:" + str(self.id) + " initialized for Node:" + str(self.main_node.id))

    def stop(self):
        """Terminates the connection and the thread is stopped."""
        self.terminate_flag.set()
