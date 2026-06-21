"""Monitor-tab-only saved DB connections.

These are database connection profiles created from the **Monitor tab's**
"Add Database" button. They are deliberately stored in a separate file
(``<DBASSISTANT_HOME>/connections/monitor_db.json``) from the core
``db.json`` so they stay **isolated** to the Monitoring module:

* The SQL Editor, Data Migration and AI Query tabs read the core
  :class:`~common.connection_manager.ConnectionManager` (``db.json``) and so
  never see Monitor-tab connections.
* The Monitor tab reads *both* stores (core ``db.json`` for visibility into
  Connections-tab profiles **and** this store for its own private ones).

No connection logic is duplicated: this subclass only redirects the storage
file. Encryption still uses the shared ``db.key`` via
:class:`~common.connection_manager.ConnectionManager`.
"""

from __future__ import annotations

from common import paths as _paths
from common.connection_manager import ConnectionManager


class MonitorDBConnectionManager(ConnectionManager):
    """A :class:`ConnectionManager` backed by the Monitor-only store file."""

    def __init__(self):
        # ConnectionManager joins a relative ``config_file`` to connections_dir;
        # pass the monitor_db.json filename so we reuse all its add/update/
        # delete/lookup logic against the isolated store.
        super().__init__(config_file=_paths.monitor_db_connections_path().name)
