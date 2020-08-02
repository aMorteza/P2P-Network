import socket
import time
import random
import threading
import json
import datetime
from neighbor import Neighbor

""" host search till N neighbors, if a host has less than N neighbors try to connect to random address (IP + Port).
a host keep its neighbors list and send hello heartbeat to each neighbors every 2 seconds
if a host do'nt get a hello form a neighbor till 8 seconds, the neighbor will deleted from the host neighbors list.
neighbor = bidirectional link """


class Node(threading.Thread):

    def __init__(self, host, port, index, debug=True):
        super().__init__()

        # When this flag is set, the node will stop and close
        self.terminate_flag = threading.Event()

        # Server details, host, port and id to bind
        self.host = host
        self.port = port
        self.id = index
        self.sender_data = {"id": self.id, "ip": self.host, "port": self.port}
        self.last_send_times = {}
        self.last_recv_times = {}

        # Nodes that have established a connection with this node, and vise-versa
        self.neighbors = []  # Nodes that are connect with us (US)<->N

        # Message counters to make sure everyone is able to track the total messages
        self.message_count_send = 0
        self.message_count_recv = 0

        # to calculate node accessibility during time
        self.stop_time = 0
        self.stopped = False

        # to calculate topology from neighbors
        self.matrix = [[0 for j in range(0, 6)] for i in range(0, 6)]
        # Debugging on or off!
        self.debug = debug

        # Create a datagram socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.init_server()
        # UDP server is listening

    def init_server(self):
        """Initialization of the UDP server to receive connections. It binds to the given host and port."""
        self.debug_print("Initialisation of the Node on port: " + str(self.port) + " on node (" + str(self.id) + ")")
        self.sock.setblocking(False)
        # Bind to address and ip
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(350.0)

    def debug_print(self, message):
        """When the debug flag is set to True, all debug messages are printed in the console."""
        if self.debug:
            print("\nDEBUG: " + message)

    def delete_terminated_nodes_from_neighbors(self):
        """Checks whether the connected nodes have been terminated or did'nt send hello during 8 seconds.
         If so, clean the neighbors list of the nodes."""
        for neighbor in self.neighbors:
            if neighbor.terminate_flag.is_set() or (time.time() - neighbor.last_hello_time > 8):
                neighbor.join()
                del self.neighbors[self.neighbors.index(neighbor)]

    def send_hello_to_neighbors(self):
        """ Send a message to all the nodes that are connected with this node. data is a python variable which is
            converted to JSON that is send over to the other node."""
        for neighbor in self.neighbors:
            self.send_hello_to_node(neighbor.host, neighbor.port)

    def get_hello_serializable_data(self, host, port):
        return {"id": self.sender_data.get("id"), "ip": self.sender_data.get("ip"),
                "port": self.sender_data.get("port"),
                "neighbor_ids": [neighbor.id for neighbor in self.neighbors],
                "last-send-to-" + host + ":" + str(port): self.last_send_times.get(host + str(port), None),
                "last-recv-from-" + host + ":" + str(port): self.last_recv_times.get(host + str(port), None)}

    def send_hello_to_node(self, host, port):
        """ Send the hello data to a node which not need be neighbor."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.debug_print("Node:" + str(self.id) + " sending hello to %s:%s" % (host, port))
            sock.connect((host, port))
            hello_data = self.get_hello_serializable_data(host, port)
            json_data = json.dumps(hello_data)
            json_data = json_data.encode('utf-8')
            sock.sendto(json_data, (host, port))
            self.message_count_send += 1
            self.last_send_times.update({host + str(port): time.time()})
            return sock
        except Exception as e:
            self.debug_print("Node send_hello_to_node:" + str(e))

    def connect_with_node(self, host, port):
        """ Make a connection with another node that is running on host and port."""
        if not (host == self.host and port == self.port) and not self.node_has_neighbor(host, port):
            sock = self.send_hello_to_node(host, port)
            recv_data, address = sock.recvfrom(4096)
            recv_data = json.loads(recv_data.decode('utf-8'))
            connected_node_id = recv_data.get("id")
            self.message_count_recv += 1
            last_hello = time.time()
            self.last_recv_times.update({host + str(port): last_hello})
            thread_client = Neighbor(self, sock, connected_node_id, host, port, last_hello)
            thread_client.start()
            self.neighbors.append(thread_client)

    def disconnect_with_neighbor(self, neighbor):
        """Disconnect the UDP connection with the specified node. It stops the node and joins the thread."""
        if neighbor in self.neighbors:
            neighbor.terminate_flag.set()
            neighbor.join()
            if neighbor in self.neighbors:
                del self.neighbors[self.neighbors.index(neighbor)]
        else:
            self.debug_print(
                "disconnect_with_neighbor: cannot disconnect with a node which is not in neighbors.")

    def stop(self):
        """Stop this node and terminate all the connected nodes."""
        self.terminate_flag.set()

    def node_message(self, node, data):
        """This method is invoked when a node send us a message."""
        self.debug_print("node_message: " + node.id + ": " + str(data))

    def node_has_neighbor(self, host, port):
        exists = False
        for neighbor in self.neighbors:
            if host == neighbor.host and port == neighbor.port:
                exists = True
                break
        return exists

    def get_random_not_neighbor_node(self, init_host="localhost", init_port=9090):
        rand_host = init_host
        rand_port = init_port + random.randint(0, 5)
        if (rand_host == self.host and rand_port == self.port) or self.node_has_neighbor(rand_host, rand_port):
            return self.get_random_not_neighbor_node(init_host, init_port)
        else:
            return rand_host, rand_port

    def stop_node_randomly(self, init_host="localhost", init_port=9090):
        rand_host = init_host
        rand_port = init_port + random.randint(0, 5)
        if rand_host == self.host and rand_port == self.port \
                and len(self.neighbors) > 0 and not self.stopped:
            lock = threading.Lock()
            with lock:
                self.stopped = True
                self.debug_print("Stopping Node:" + str(self.id) + " activity for 20 seconds")
                before_stop = time.time()
                for neighbor in self.neighbors:
                    self.disconnect_with_neighbor(neighbor)
                time.sleep(20)  # It blocks the thread
                after_stop = time.time()
                self.stop_time += after_stop - before_stop
            self.stopped = False

    def log_all_neighbors_history(self):
        with open("data/node_neighbors_history/node" + str(self.id) + ".json", "a", encoding='utf-8') as f:
            for neighbor in self.neighbors:
                line = {"date": str(datetime.datetime.now()), "ip": neighbor.host, "port": neighbor.port,
                        "message_count_send": neighbor.main_node.message_count_send,
                        "message_count_recv": neighbor.main_node.message_count_recv
                        }
                json.dump(line, f, ensure_ascii=False, indent=4)

    def log_present_neighbors(self):
        with open("data/node_present_neighbors/node" + str(self.id) + ".json", "w", encoding='utf-8') as f:
            for neighbor in self.neighbors:
                line = {"date": str(datetime.datetime.now()), "ip": neighbor.host, "port": neighbor.port,
                        "message_count_send": neighbor.main_node.message_count_send,
                        "message_count_recv": neighbor.main_node.message_count_recv
                        }
                json.dump(line, f, ensure_ascii=False, indent=4)

    def log_accessibility_time(self, run_time):
        with open("data/node_accessibility_time/node" + str(self.id) + ".json", "w", encoding='utf-8') as f:
            line = {"accessibility_rate": "%{0:.2f}".format(float(run_time - self.stop_time) * 100 / run_time)}
            json.dump(line, f, ensure_ascii=False, indent=4)

    def log_matrix(self):
        with open("data/node_topology/node" + str(self.id) + ".json", "w", encoding='utf-8') as f:
            for line in self.matrix:
                json.dump(line, f, ensure_ascii=False, indent=4)

    def run(self):
        """The main loop of the thread that deals with connections from other nodes on the network."""
        start_time = time.time()
        while not self.terminate_flag.is_set():  # Check whether the thread needs to be closed
            try:
                """Go for random nodes to reach neighbors limit"""
                if len(self.neighbors) < 3:
                    rand_host, rand_port = self.get_random_not_neighbor_node()
                    self.send_hello_to_node(rand_host, rand_port)

                self.debug_print("Node:" + str(self.id) + " Wait for incoming hello")
                rand_int = random.randint(1, 100)
                if rand_int not in range(1, 6):
                    recv_data, address = self.sock.recvfrom(4096)
                    recv_data = json.loads(recv_data.decode('utf-8'))
                    connected_node_id = recv_data.get("id")
                    self.debug_print("Node:" + str(self.id) + " Got hello from Node:" + str(connected_node_id))
                    host = recv_data.get("ip")
                    port = recv_data.get("port")
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.connect((host, port))
                    last_hello_time = time.time()
                    self.last_recv_times.update({host + str(port): last_hello_time})
                    self.message_count_recv += 1
                    if not self.node_has_neighbor(host, port) and len(self.neighbors) < 3:
                        """If the node which sent hello is not neighbor, try to be"""
                        self.send_hello_to_node(host, port)
                        thread_client = Neighbor(self, sock, connected_node_id, host, port, last_hello_time)
                        thread_client.start()
                        self.neighbors.append(thread_client)

                    """Use received hello neighbors list to improve topology matrix"""
                    neighbor_ids = recv_data.get("neighbor_ids")
                    if len(neighbor_ids) > 0:
                        for neighbor_id in neighbor_ids:
                            if self.node_has_neighbor(host, port):
                                """It's a trusted neighbor so connection in bidirectional"""
                                self.matrix[neighbor_id][connected_node_id] = 1
                                self.matrix[connected_node_id][neighbor_id] = -1
                            else:
                                """It's not our neighbor, just sent hello, so connection is unidirectional"""
                                self.matrix[neighbor_id][connected_node_id] = 1

                self.delete_terminated_nodes_from_neighbors()
                self.sender_data.update({"neighbors": self.neighbors})

                """Every 2 seconds send hello to all neighbors like a heartbeat"""
                threading.Timer(2, self.send_hello_to_neighbors).start()

                """Use node neighbors list to improve topology matrix (Not asked)"""
                # if len(self.neighbors) > 0:
                #     for neighbor in self.neighbors:
                #         self.matrix[self.id][neighbor.id] = 1
                #         self.matrix[neighbor.id][self.id] = -1

                """Every 10 seconds a random node stop (empty the neighbors list and not sending any
                hello to others) for 20 seconds """
                threading.Timer(10, self.stop_node_randomly).start()

                self.log_all_neighbors_history()

            except socket.timeout:
                self.debug_print('Node: Connection timeout!')
            except Exception as e:
                raise e

        self.log_present_neighbors()
        self.log_matrix()
        end_time = time.time()
        run_time = end_time - start_time
        self.log_accessibility_time(run_time)

        self.debug_print("Node:" + str(self.id) + " stopping...")
        self.sock.settimeout(None)
        self.sock.close()
        self.debug_print("Node:" + str(self.id) + " stopped")
