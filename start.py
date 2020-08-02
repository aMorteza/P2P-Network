from node import Node
import time
import os

""" N hosts search others till have 3 neighbors, if a host has less than N neighbors try to connect to random address
 (IP + Port). a host keep its neighbors list and send Hello heartbeat to each neighbors every 2 seconds if a host 
 do'nt get a Hello form a neighbor till 8 seconds, the neighbor will deleted from the host neighbors list.
 neighbor = bidirectional"""
N = 6
ports = 9090
host = "localhost"

"""Clear last run history (except .gitignore) and other directory files will be overwrite during run time."""
history_dir = "data/node_neighbors_history"
list(map(os.unlink, (os.path.join(history_dir, f) for f in next(os.walk(history_dir))[2][1:])))

print("Starting network..")
nodes = [Node(host, ports + i, i) for i in range(0, N)]

"""calling run method foreach node thread"""
for node in nodes:
    node.start()

"""run network for 5 minutes"""
time.sleep(5 * 60)

for node in nodes:
    node.stop()

for node in nodes:
    node.join()
print("Network stopped.")

"""Merge json files from /data to create network report"""
for i in range(0, N):
    try:
        with open("node" + str(i) + ".json", "w") as outfile:
            outfile.write("{\n")
            for directory in next(os.walk('data'))[1]:
                path = "data/"+directory+"/node" + str(i) + ".json"
                if os.path.isfile(path):
                    with open(path, "r", encoding="utf-8") as infile:
                        outfile.write('"{}"'.format(directory) + ":{\n ")
                        for line in infile.read().splitlines(keepends=True):
                            if line == "}{\n":
                                line = "},\n{\n"
                            outfile.write(line)
                        outfile.write("\n},\n\n")
            outfile.write("\n}")
    except Exception as e:
        print(str(e))