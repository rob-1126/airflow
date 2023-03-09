from airflow.dag_processing.manager import BaseDagFileProcessorManager
from os import PathLike
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileSystemEventHandler
from airflow.configuration import conf

import requests
  
class Handler(FileSystemEventHandler):
    def __init__(self, dag_file_processor_manager, *args, **kwargs):
        self.dag_file_processor_manager = dag_file_processor_manager
        super().__init__()#, *args, **kwargs)
    def on_any_event(self, event):
        log = self.dag_file_processor_manager.log
        if event.is_directory:
            return None
        elif event.event_type == 'created':
            log.debug(f"Watchdog received created event - {event.src_path}")
            self.dag_file_processor_manager.needs_to_update = True
        elif event.event_type == 'modified':
            log.debug(f"Watchdog received modified event - {event.src_path}")
            self.dag_file_processor_manager.needs_to_update = True
              

class MyDagFileProcessorManager(BaseDagFileProcessorManager):
    def __init__(
        self,
        dag_directory: PathLike,
        *args,
        **kwargs,
    ):
        watchdog_config = conf.getjson(
            section="astronomer",
            key="watchdog_config",
            fallback={},
        )
        self.needs_to_update = True
        super().__init__(dag_directory, *args, **kwargs)
        self.log.debug(f"Starting Rob's dag parser watching {dag_directory} with {watchdog_config}")
        self.log.debug(f"Rob with {watchdog_config['name']}")
        event_handler = Handler(self)
        observer = Observer()
        observer.schedule(event_handler, dag_directory, recursive=True)
        observer.start()
        #observer.join()
    def _refresh_dag_dir(self):
        if self.needs_to_update:
          self.needs_to_update = False
          super()._refresh_dag_dir()
          self._invalidate_remote_caches()
    def _invalidate_remote_caches(self):
      self.log.debug("Sending dagbag invalidation request to airflow api via hard-wired basic auth")
      url = "http://localhost:8080/api/v1/example_invalidate_dagbag_cache"
      data = {}
      basic_auth =('admin', 'admin')
      response =  requests.post(url, json = data, auth=basic_auth)
      self.log.debug(f"Airflow API responded to dagbag cache invalidation request {response}")
