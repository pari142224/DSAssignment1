from flask import Flask, request
import threading
import time

# --------- VectorClock Class ---------

class VectorClock:
    def __init__(self, node_id, all_nodes):
        self.clock = {nid: 0 for nid in all_nodes}
        self.node_id = node_id

    def increment(self):
        self.clock[self.node_id] += 1

    def update(self, received_clock):
        for node, val in received_clock.items():
            self.clock[node] = max(self.clock.get(node, 0), val)

    def is_causally_ready(self, received_clock, sender_id):
        for node in self.clock:
            if node == sender_id:
                if received_clock[node] != self.clock[node] + 1:
                    return False
            else:
                if received_clock[node] > self.clock[node]:
                    return False
        return True

    def get_clock(self):
        return self.clock.copy()

# --------- Globals ---------

app = Flask(__name__)
store = {}           # Key-value data store
buffer = []          # Buffer for causally premature messages
node_id = None       # Current node's ID
vector_clock = None  # VectorClock instance
all_nodes = []       # All node IDs

# --------- Flask Endpoints ---------

@app.route('/put', methods=['POST'])
def put():
    global store, vector_clock
    data = request.get_json()
    key = data['key']
    value = data['value']
    received_clock = data['clock']
    sender_id = data['sender']

    if vector_clock.is_causally_ready(received_clock, sender_id):
        store[key] = value
        vector_clock.update(received_clock)
        print(f"[{node_id}] Applied write: {key}={value}, clock={vector_clock.clock}")
        return {'status': 'applied'}
    else:
        buffer.append(data)
        print(f"[{node_id}] Buffered write: {key}={value} from {sender_id}")
        return {'status': 'buffered'}

@app.route('/get', methods=['GET'])
def get():
    key = request.args.get('key')
    value = store.get(key, None)
    return {'value': value}

@app.route('/replicate', methods=['POST'])
def replicate():
    return put()

# --------- Background Buffer Processing ---------

def process_buffer():
    global buffer
    while True:
        time.sleep(0.5)
        for entry in buffer[:]:  # Iterate over a copy
            if vector_clock.is_causally_ready(entry['clock'], entry['sender']):
                store[entry['key']] = entry['value']
                vector_clock.update(entry['clock'])
                buffer.remove(entry)
                print(f"[{node_id}] Buffered write applied: {entry['key']}={entry['value']}")

# --------- Node Initialization ---------

def start_node(my_id, node_list):
    global node_id, all_nodes, vector_clock
    node_id = my_id
    all_nodes = node_list
    vector_clock = VectorClock(node_id, all_nodes)

    print(f"[{node_id}] Node started with clock: {vector_clock.clock}")
    threading.Thread(target=process_buffer, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)

# --------- Entry Point ---------

if __name__ == '__main__':
    import sys
    my_node_id = sys.argv[1]            # e.g., "node1"
    node_ids = sys.argv[2].split(',')   # e.g., "node1,node2,node3"
    start_node(my_node_id, node_ids)