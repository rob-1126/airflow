from airflow.utils.state_cache import BaseStateCacheBackend

# imports for our poc state backend
import tempfile
import pathlib
from queue import Queue, Empty
from threading import Thread
from os import getpid, path
import pytz

# EventBus = resolve_event_bus_backend()
# event_bus = event_bus()

import socket
import asyncio
import json
from datetime import datetime

existing_client = False

tz = pytz.timezone("UTC")

class StateCache(BaseStateCacheBackend):
    def __init__(self):
        # will be set later when called by app.create_app calls .start()
        socket_filename = path.join(tempfile.mkdtemp(), "airflow-cache.sock")
        self.socket_filename = socket_filename

    def get_state_cache_client(self):
        global existing_client
        if not existing_client:
            raise Exception("State cache client has not yet be initialized.")
        return existing_client

    def init_cache_client(self, state_backend_client_config):
        global existing_client
        if not existing_client:
            client = self.get_sync_client(state_backend_client_config)
            client.connect()
            existing_client = client
            print("initialized new init_state_cache_client")
            return existing_client
        else:
            raise Exception("State cache client can only be initialized once.")

    def start(self):
        """This runs within a thread started by the cli"""
        print("Starting asyncio temporary state backend")
        print("Connecting to event bus")
        key_vals = {}
        key_vals["favorite_color"] = "blue"

        async def handle_invalidate_dag_cache():
            key_vals["universal_last_updated"] = tz.localize(datetime.utcnow()).isoformat()
            print("Invalidating dag cache.")

        async def _get_key(key):
            print(f"Server retrieving value for {key}")
            val = key_vals[key]
            return val.encode("utf-8")

        async def _read_until(reader, needle):
            cmd_buf = bytearray()
            while True:
                bytes = await reader.read(1)
                # break when we get a space
                if len(bytes) > 0:
                    if bytes[0] == needle:
                        break
                cmd_buf.extend(bytes)
            return cmd_buf

        async def client_connected_cb(reader, writer):
            SPACE = ord(" ")
            while True:
                cmd_buf = await _read_until(reader, SPACE)
                cmd = cmd_buf.decode("utf-8")
                if cmd == "hello":
                    await reader.readline()
                elif cmd == "get":
                    rest_of_line = await reader.readline()
                    key = rest_of_line.decode("utf-8").strip()
                    result = await _get_key(key)
                    writer.write(result)
                    writer.write(b"\r\n")
                    result = await _get_key(key)
                    data = result.encode('utf-8')
                    writer.write(data)
                    writer.write(b"\r\n")
                elif cmd == "get_dag_last_updated":
                    rest_of_line = await reader.readline()
                    key = rest_of_line.decode("utf-8").strip()
                    if "universal_last_updated" in key_vals:
                        result = key_vals["universal_last_updated"]
                    else:
                        result = None
                    data = json.dumps({ "value": result })
                    writer.write(data.encode("utf-8"))
                    writer.write(b"\r\n")
                else:
                    await reader.readline()
                    raise UnknownCommandFromTempStateClient(cmd)
                #await writer.drain()
                #print("Drained")
                # print("Closing conn")
                # writer.close()
                # await writer.wait_closed()

        async def main_cache_server_loop():
            from airflow.utils.eventbus import resolve_event_bus_backend

            EventBusBackend = resolve_event_bus_backend()
            event_bus = EventBusBackend()
            await event_bus.connect()
            message = await event_bus.subscribe("secondary")
            print(f"Starting a socket server on {self.socket_filename}")
            server = await asyncio.start_unix_server(
                client_connected_cb=client_connected_cb, path=self.socket_filename
            )
            serve_cache_clients = server.serve_forever()
            while True:
                print("Awaiting message")
                message = await event_bus.get_message("secondary")
                print("Got message", message, type(message))
                data = message["data"]
                payload = json.loads(data)
                print("GOT OBJ", payload)
                op = payload["op"]
                if op == "invalidate_dag_cache":
                    await handle_invalidate_dag_cache()
            await serve_cache_clients

        asyncio.run(main_cache_server_loop())

        print(f"listening on {self.socket_filename}")
        backlog = 20

        print(f"Starting socket on {self.socket_filename}")

    def set_client_config(self, client_config):
        self.client_config = client_config
        print("Set client config to", client_config)

    def make_client_config(self):
        """clients can and will usually be local forked processes"""
        return {"socket_path": str(pathlib.Path(self.socket_filename))}

    def get_client_config_as_string(self):
        return str(pathlib.Path(self.socket_filename))

    def connect(self, client_config):
        return TemporaryStateBackendClient(client_config)

    def get_sync_client(self, client_config):
        return TemporarySyncStateBackendClient(client_config)

    def make_client_config_from_string(self, val):
        """used to turn a socket_path into a client config object"""
        return {"socket_path": val}


class TemporaryStateBackendClient:
    def __init__(self, client_config):
        self.client_config = client_config
        self.local_event_queue = Queue()
        print("Created new state backend client for", self.client_config)
        print("ZAR PREPARING TO RUN")
        self.run()

    def run_sync(self, dag_bag):
        """runs inside the remote thread and processes events from the message queue"""
        # todo prune obsolete events from queue
        local_event_queue = self.local_event_queue
        print("RUNNING REQUEST FOR ", local_event_queue)
        while True:
            try:
                item = local_event_queue.get(block=False)
                print("FOUND SOMETHING")
            except Empty:
                print("WAS EMPTY")
                break
            print("ZAR Got item", item, getpid())

            if item == "invalidate_dagbag":
                print("ZAR INVALIDATING", getpid())
                dag_bag.invalidate_cache()
                local_event_queue.task_done()

    def run(self):
        import time

        socket_path = self.client_config["socket_path"]

        def run_thread(local_event_queue):
            print("ZAR RUNNING thread")
            while True:
                print("ZAR SUBMITTING INVALIDATE CB")
                local_event_queue.put("invalidate_dagbag")
                time.sleep(15)

        # really shoudlnt be a daemon thread, may cause ops to not be shut down cleanly
        # but if not a daemon thread we will need to hault it properly during shutdown
        self.thread = Thread(target=run_thread, args=(self.local_event_queue,), daemon=True)
        self.thread.start()


class UnknownCommandFromTempStateClient(Exception):
    pass


class TemporarySyncStateBackendClient:
    def __init__(self, client_config):
        self.client_config = client_config
        print("Created new state backend client for", self.client_config)
        print("ZAR PREPARING TO RUN")

    def _read_line(self):
        #read_buffer = bytearray()
        return self.socket.makefile().readline()
        #while True:
        #    chunk = self.socket.recv(4096)
        #    read_buffer.extend(chunk)
        #    if len(read_buffer) > 1:
        #        last_two_chars = read_buffer[-2:]
        #        if last_two_chars == b"\r\n":
        #            break
        return read_buffer

    def connect(self):
        socket_path = self.client_config["socket_path"]
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        print(f"Waiting for socket at {socket_path}.")
        connected = False
        while not connected:
            try:
                print(f"Connecting to {socket_path}")
                self.socket.connect(socket_path)
                connected = True
            except ConnectionRefusedError:
                pass
            except FileNotFoundError:
                pass

    def get_key(self, key_name):
        encoded_key_name = key_name.encode("utf-8")
        self.socket.send(b"get ")
        self.socket.send(encoded_key_name)
        self.socket.send(b"\r\n")
        print("SENDING GET")
        response = self._read_line().strip()
        return response

    def get_dag_last_updated(self, dag_id):
        encoded_dag_id = dag_id.encode("utf-8")
        self.socket.send(b"get_dag_last_updated ")
        self.socket.send(encoded_dag_id)
        self.socket.send(b"\r\n")
        response = self._read_line().strip()
        print("GETTING RAW",repr(response),type(response))
        response_data = json.loads(response)
        print("GOT RESPONSE", response_data)

        raw_value = response_data["value"]
        if raw_value is None:
          return None
        else:
          return datetime.fromisoformat(raw_value)

    def frob(self):
        print("FROBBING")


# quick test
if __name__ == "__main__":
    import time

    StateBackend = StateCache
    state_backend = StateBackend()
    print("Starting server")
    server_thread = Thread(target=state_backend.start, args=(), daemon=True)
    server_thread.start()
    print("Server started")
    client_config = state_backend.make_client_config()
    sync_state_client = state_backend.get_sync_client(client_config)
    sync_state_client.connect()
    last_updated = sync_state_client.get_dag_last_updated("my_dag")
    print("GOT A LAST UPDATE", last_updated)
    favorite_color = sync_state_client.get_key("favorite_color")
    print("1. Got favorite color", repr(favorite_color))
    favorite_color = sync_state_client.get_key("favorite_color")
    print("2. Got favorite color", repr(favorite_color))
    time.sleep(5)
    # state_backend.connect()
    # print(state_backend.get_client_config())
