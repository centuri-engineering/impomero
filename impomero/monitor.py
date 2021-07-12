"""Filesystem monitoring
"""

import logging
import sqlite3
import time
from pathlib import Path

from omero.gateway import BlitzGateway
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from .annotation_job import auto_annotate, update_annotation
from .collector import get_configuration, is_annotation
from .importer_job import auto_import

log = logging.getLogger(__name__)


# had to do:
# sudo sysctl fs.inotify.max_user_watches=100000


class TomlCreatedEventHandler(PatternMatchingEventHandler):
    def __init__(self, transfer=None, dry_run=False, import_db=None):
        self.transfer = transfer
        self.dry_run = dry_run
        self.import_db = import_db
        super().__init__(patterns=["*.toml"])

    def on_created(self, event):
        log.info(f"Toml file {event.src_path} created")
        if not is_annotation(event.src_path):
            log.info(f"{event.src_path} was not an annotation file")
            return

        base_dir = Path(event.src_path).parent

        log.info("~~~~~~~~~####~~~~~~~~~")
        log.info(f"importing from {base_dir}")
        log.info("~~~~~~~~~####~~~~~~~~~")

        with sqlite3.connect(self.import_db) as sql_con:
            try:
                ids = [
                    val[0]
                    for val in sql_con.execute(
                        f"select id from annotated where base_dir='{base_dir}'"
                    )
                ]
            except sqlite3.OperationalError:
                ids = []
        if ids:
            self.update_imported(ids, event.src_path)
        else:
            self.fresh_import(base_dir)

    def on_modified(self, event):
        return self.on_created(event)

    def fresh_import(self, base_dir):

        conf, import_table = auto_import(
            base_dir=base_dir,
            dry_run=self.dry_run,
            # We do not want to clean temp files
            # as we want to data annotate after
            clean=False,
            transfer=self.transfer,
        )

        log.info("~~~~~~~~~####~~~~~~~~~")
        log.info("Annotating ... ")
        log.info("~~~~~~~~~####~~~~~~~~~")
        with BlitzGateway(
            host=conf["server"],
            port=conf["port"],
            username="root",
            passwd=conf["admin_passwd"],
            secure=True,
        ) as conn:
            annotated = auto_annotate(conn, import_table)

        annotated["base_dir"] = base_dir.resolve().as_posix()

        with sqlite3.connect(self.import_db) as sql_con:
            try:
                annotated.to_sql("annotated", con=sql_con, if_exists="append")
            except sqlite3.InterfaceError as e:
                print(e)
                for col in annotated.head().columns:
                    print(col, annotated[col])

        # TODO: spawn a new Observer for that base_dir

    def update_imported(self, ids, toml_path):

        conf = get_configuration()
        with BlitzGateway(
            host=conf["server"],
            port=conf["port"],
            username="root",
            passwd=conf["admin_passwd"],
            secure=True,
        ) as conn:
            for img_id in ids:
                update_annotation(conn, img_id, toml_path, object_type="Image")


def start_toml_observer(path, transfer=None, dry_run=False, import_db=None):
    toml_handler = TomlCreatedEventHandler(
        transfer=transfer, dry_run=dry_run, import_db=import_db
    )

    # We use the polling observer as inotify
    # does not see remote file creation events
    observer = Observer()
    observer.schedule(toml_handler, path, recursive=True)
    print("Starting observer")
    observer.start()
    print("Observer started")
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
