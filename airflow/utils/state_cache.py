from airflow.configuration import conf
from typing import TYPE_CHECKING, Any, Generator, Iterable, cast, overload

class BaseStateCacheClient():
  def get_dag_last_updated(self, dag_id):
      return None

class BaseStateCacheBackend:
    def get_state_cache_client(client_config):
        return BaseStateCacheClient()

def resolve_state_cache_backend() -> type[BaseStateCacheBackend]:
    """Resolves custom StateCache class

    Confirms that custom StateCacheBackend class extends the BaseStateCacheBackend.
    Compares the function signature of the custom StateCache serialize_value to the base StateCache serialize_value.
    """
    clazz = conf.getimport(
        "core", "state_cache_backend", fallback=f"airflow.utils.state_cache.{BaseStateCacheBackend.__name__}"
    )
    if not clazz:
        return BaseStateCacheBackend
    if not issubclass(clazz, BaseStateCacheBackend):
        raise TypeError(
            f"Your custom StateCache class `{clazz.__name__}` is not a subclass of `{BaseStateCacheBackend.__name__}`."
        )
    return clazz


if TYPE_CHECKING:
    StateCache = BaseStateCacheBackend  # Hack to avoid Mypy "Variable 'StateCache' is not valid as a type".
else:
    StateCache = resolve_state_cache_backend()
