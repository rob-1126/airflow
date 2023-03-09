#
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

import logging

from typing import TYPE_CHECKING, Any, Generator, Iterable, cast, overload

from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin

log = logging.getLogger(__name__)


class BaseEventBusBackend(LoggingMixin):
    async def subscribe(self, channel):
        log.debug(f"Subscribing to channel {channel}")
        pass
    async def get_message(self, channel):
        log.debug(f"Checking for message on {channel}")
        pass
    async def unsubscribe(self, channel):
        log.debug(f"Unsubscribing from channel {channel}")
        pass
    async def connect(self):
        log.debug(f"Connecting to event bus.")
        pass
    async def send(self, channel, message):
        log.debug(f"Sending message to event bus channel {channel}: {message}")


def resolve_event_bus_backend() -> type[BaseEventBus]:
    """Resolves custom EventBus class

    Confirms that custom EventBusBackend class extends the BaseEventBusBackend.
    Compares the function signature of the custom EventBus serialize_value to the base EventBus serialize_value.
    """
    clazz = conf.getimport(
        "core", "event_bus_backend", fallback=f"airflow.models.event_bus.{BaseEventBusBackend.__name__}"
    )
    if not clazz:
        return BaseEventBusBackend
    if not issubclass(clazz, BaseEventBusBackend):
        raise TypeError(
            f"Your custom EventBus class `{clazz.__name__}` is not a subclass of `{BaseEventBusBackend.__name__}`."
        )
    return clazz


if TYPE_CHECKING:
    EventBus = BaseEventBusBackend  # Hack to avoid Mypy "Variable 'EventBus' is not valid as a type".
else:
    EventBus = resolve_event_bus_backend()
