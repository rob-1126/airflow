# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import asyncio
import json
import pathlib

# EventBus = resolve_event_bus_backend()
# event_bus = event_bus()
import socket

# imports for our poc state backend
import tempfile
from datetime import datetime
from os import getpid, path
from queue import Empty, Queue
from threading import Thread

import pytz

from airflow.utils.state_cache import BaseStateCacheBackend

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
            return existing_client
        else:
            raise Exception("State cache client can only be initialized once.")

    def start(self):
        """This runs within a thread started by the cli"""
        key_vals = {}
        key_vals["favorite_color"] = "blue"

        async def handle_invalidate_dag_cache():
            key_vals["universal_last_updated"] = tz.localize(datetime.utcnow()).isoformat()

        async def _get_key(key):
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
                    data = result.encode("utf-8")
                    writer.write(data)
                    writer.write(b"\r\n")
                elif cmd == "get_dag_last_updated":
                    rest_of_line = await reader.readline()
                    key = rest_of_line.decode("utf-8").strip()
                    if "universal_last_updated" in key_vals:
                        result = key_vals["universal_last_updated"]
                    else:
                        result = None
                    data = json.dumps({"value": result})
                    writer.write(data.encode("utf-8"))
                    writer.write(b"\r\n")
                else:
                    await reader.readline()
                    raise UnknownCommandFromTempStateClient(cmd)

        async def main_cache_server_loop():
            from airflow.utils.eventbus import resolve_event_bus_backend

            EventBusBackend = resolve_event_bus_backend()
            event_bus = EventBusBackend()
            await event_bus.connect()
            message = await event_bus.subscribe("secondary")
            server = await asyncio.start_unix_server(
                client_connected_cb=client_connected_cb, path=self.socket_filename
            )
            serve_cache_clients = server.serve_forever()
            while True:
                message = await event_bus.get_message("secondary")
                data = message["data"]
                payload = json.loads(data)
                op = payload["op"]
                if op == "invalidate_dag_cache":
                    await handle_invalidate_dag_cache()
            await serve_cache_clients

        asyncio.run(main_cache_server_loop())

    def set_client_config(self, client_config):
        self.client_config = client_config

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
        self.run()

    def run_sync(self, dag_bag):
        """runs inside the remote thread and processes events from the message queue"""
        # todo prune obsolete events from queue
        local_event_queue = self.local_event_queue
        while True:
            try:
                item = local_event_queue.get(block=False)
            except Empty:
                break

            if item == "invalidate_dagbag":
                dag_bag.invalidate_cache()
                local_event_queue.task_done()

    def run(self):
        import time

        self.client_config["socket_path"]

        def run_thread(local_event_queue):
            while True:
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

    def _read_line(self):
        return self.socket.makefile().readline()

    def connect(self):
        socket_path = self.client_config["socket_path"]
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connected = False
        while not connected:
            try:
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
        response = self._read_line().strip()
        return response

    def get_dag_last_updated(self, dag_id):
        encoded_dag_id = dag_id.encode("utf-8")
        self.socket.send(b"get_dag_last_updated ")
        self.socket.send(encoded_dag_id)
        self.socket.send(b"\r\n")
        response = self._read_line().strip()
        response_data = json.loads(response)

        raw_value = response_data["value"]
        if raw_value is None:
            return None
        else:
            return datetime.fromisoformat(raw_value)
