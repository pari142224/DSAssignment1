
import requests
import time

NODES = {
    "node1": "http://localhost:5001",
    "node2": "http://localhost:5002",
    "node3": "http://localhost:5003"
}

def put(node, key, value, sender, clock):
    url = f"{NODES[node]}/put"
    data = {
        "key": key,
        "value": value,
        "sender": sender,
        "clock": clock
    }
    res = requests.post(url, json=data)
    print(f"[PUT to {node}] {key}={value}, status={res.json()['status']}")

def replicate(from_node, to_node, key, value, sender, clock):
    url = f"{NODES[to_node]}/replicate"
    data = {
        "key": key,
        "value": value,
        "sender": sender,
        "clock": clock
    }
    res = requests.post(url, json=data)
    print(f"[Replicate {key}={value} from {from_node} to {to_node}] status={res.json()['status']}")

def get(node, key):
    url = f"{NODES[node]}/get"
    res = requests.get(url, params={'key': key})
    print(f"[GET from {node}] {key} => {res.json()['value']}")

if __name__ == "__main__":
    # Simulate a causal write from node1, then replicate out of order

    # Vector clocks
    vc1 = {"node1": 1, "node2": 0, "node3": 0}
    vc2 = {"node1": 1, "node2": 1, "node3": 0}

    print("---- Step 1: node1 writes x=A ----")
    put("node1", "x", "A", "node1", vc1)

    time.sleep(1)
    print("---- Step 2: replicate x=A to node3 (delayed arrival expected) ----")
    replicate("node1", "node3", "x", "A", "node1", vc1)

    print("---- Step 3: node2 writes x=B (based on x=A, causal) ----")
    put("node2", "x", "B", "node2", vc2)

    time.sleep(1)
    print("---- Step 4: replicate x=B to node3 (should be buffered) ----")
    replicate("node2", "node3", "x", "B", "node2", vc2)

    time.sleep(3)
    print("---- Step 5: get x from node3 ----")
    get("node3", "x")