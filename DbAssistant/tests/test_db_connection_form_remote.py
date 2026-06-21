"""DBConnectionFormDialog remote (SSH tunnel) collection logic."""

from __future__ import annotations

from common.ui.tk.db_connection_form import DBConnectionFormDialog


class _FakeMgr:
    def __init__(self):
        self.saved = None

    def connection_exists(self, name):
        return False

    def add_connection(self, params):
        self.saved = params.to_profile(include_password=True)
        return True, "ok"


class _FakeWidget:
    def __init__(self, value=""):
        self.value = value
        self.config_values = {}

    def get(self):
        return self.value

    def set(self, value):
        self.value = value

    def insert(self, index, value):
        self.value = self.value[:index] + value + self.value[index:]

    def delete(self, *_args):
        self.value = ""

    def config(self, **kwargs):
        self.config_values.update(kwargs)

    def grid(self, *_args, **_kwargs):
        pass

    def grid_remove(self):
        pass


class _FakeVar:
    def __init__(self, value=None):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class _FakeWindow:
    destroyed = False

    def destroy(self):
        self.destroyed = True


def _dialog(mgr, remote):
    dlg = DBConnectionFormDialog.__new__(DBConnectionFormDialog)
    dlg.manager = mgr
    dlg.on_saved = None
    dlg.saved_name = None
    dlg.win = _FakeWindow()

    dlg.location_combo = _FakeWidget(
        "Remote host (SSH tunnel)" if remote else "Localhost / direct"
    )
    dlg.name_entry = _FakeWidget("c")
    dlg.db_type_combo = _FakeWidget("PostgreSQL")
    dlg.host_entry = _FakeWidget("localhost")
    dlg.port_entry = _FakeWidget("5432")
    dlg.service_entry = _FakeWidget("appdb")
    dlg.user_entry = _FakeWidget("app")
    dlg.password_entry = _FakeWidget("pw")
    dlg.save_password_var = _FakeVar(True)

    dlg.ssl_mode_combo = _FakeWidget("disable")
    dlg.ssl_ca_entry = _FakeWidget()
    dlg.ssl_cert_entry = _FakeWidget()
    dlg.ssl_key_entry = _FakeWidget()
    dlg.wallet_entry = _FakeWidget()
    dlg.mongo_tls_var = _FakeVar(False)
    dlg.mongo_tls_ca_entry = _FakeWidget()

    dlg.ssh_auth_var = _FakeVar("password")
    dlg.ssh_host_entry = _FakeWidget()
    dlg.ssh_port_entry = _FakeWidget("22")
    dlg.ssh_user_entry = _FakeWidget()
    dlg.ssh_password_entry = _FakeWidget()
    dlg.ssh_key_entry = _FakeWidget()
    dlg.ssh_password_label = _FakeWidget()
    dlg.ssh_key_label = _FakeWidget()
    dlg.ssh_key_browse = _FakeWidget()
    return dlg


def test_local_mode_has_no_tunnel():
    dlg = _dialog(_FakeMgr(), remote=False)
    params = dlg._collect()
    assert params is not None
    assert "ssh_tunnel" not in params


def test_remote_key_auth_collects_tunnel():
    dlg = _dialog(_FakeMgr(), remote=True)
    dlg.ssh_auth_var.set("key")
    dlg._update_ssh_auth_visibility()
    dlg.ssh_host_entry.insert(0, "bastion")
    dlg.ssh_user_entry.insert(0, "ubuntu")
    dlg.ssh_key_entry.insert(0, "/home/me/.ssh/id_rsa")
    params = dlg._collect()
    assert params["ssh_tunnel"]["ssh_host"] == "bastion"
    assert params["ssh_tunnel"]["ssh_key_file"] == "/home/me/.ssh/id_rsa"
    assert params["ssh_tunnel"]["ssh_password"] == ""
    assert "ssh_tunnel" in dlg._connect_params(params)


def test_remote_missing_ssh_host_fails_validation(monkeypatch):
    import common.ui.tk.db_connection_form as mod

    monkeypatch.setattr(mod.messagebox, "showwarning", lambda *a, **k: None)
    dlg = _dialog(_FakeMgr(), remote=True)
    dlg.ssh_user_entry.insert(0, "ubuntu")
    dlg.ssh_password_entry.insert(0, "pw")
    assert dlg._collect() is None  # ssh_host missing


def test_remote_save_passes_tunnel_to_manager(monkeypatch):
    import common.ui.tk.db_connection_form as mod

    monkeypatch.setattr(mod.messagebox, "showinfo", lambda *a, **k: None)
    mgr = _FakeMgr()
    dlg = _dialog(mgr, remote=True)
    dlg.ssh_host_entry.insert(0, "bastion")
    dlg.ssh_user_entry.insert(0, "ubuntu")
    dlg.ssh_password_entry.insert(0, "sshpw")
    dlg._save()
    assert mgr.saved is not None
    assert mgr.saved["ssh_tunnel"]["ssh_host"] == "bastion"
    assert mgr.saved["ssh_tunnel"]["ssh_password"] == "sshpw"
