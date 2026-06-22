"""CloudMonitorMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class CloudMonitorMixin:
    def update_cloud_db_listbox(self):
        """Refresh the monitoring listbox — shows only active_cloud_databases."""


        self.cloud_db_listbox.delete(0, tk.END)
        for display_name, entry in self.active_cloud_databases.items():
            provider = entry.get("provider", "")
            label = f"[{provider}] {display_name}" if provider else display_name
            self.cloud_db_listbox.insert(tk.END, label)

    # ------------------------------------------------------------------
    # Cloud provider monitoring schemas
    #
    # Purpose: identify a cloud-hosted database *resource* so its metrics
    # and logs can be fetched via the provider's monitoring API
    # (CloudWatch, Azure Monitor, GCP Cloud Monitoring).
    # No direct DB connection is made.
    #
    # Schema per provider:
    #   'resource'  – fields that identify the specific DB resource
    #   'keys_auth' – API key / token / service-account authentication
    #   'pwd_auth'  – username + password authentication (some providers)
    #
    # Each field tuple: (label, key, show)
    #   show = ""   → plain text entry
    #   show = "*"  → masked password entry
    # ------------------------------------------------------------------
    # Shared schemas — see common/cloud/schemas.py
    _CLOUD_PROVIDER_SCHEMAS = CLOUD_PROVIDER_SCHEMAS
    _MFA_TYPES = MFA_TYPES

    def add_cloud_database(self):
        """
        Cloud connection wizard (shared with Connections tab via adapter).

        Options in ``self._cloud_wizard_opts``:
          purpose, require_db_identifier, target_kind, allow_target_kinds, on_saved
        """
        opts = getattr(self, "_cloud_wizard_opts", None) or {}
        purpose = opts.get("purpose", PURPOSE_MONITOR)
        require_db = opts.get("require_db_identifier")
        target_kind = opts.get("target_kind", TARGET_CLOUD_DB)
        allow_kinds = opts.get(
            "allow_target_kinds", list(MONITOR_TARGET_KINDS.keys())
        )

        if purpose == PURPOSE_MONITOR and len(allow_kinds) > 1:
            kind_picker = tk.Toplevel(self.root)
            kind_picker.title("Add Cloud Resource – Target Type")
            kind_picker.geometry("460x220")
            kind_picker.resizable(False, False)
            kind_picker.transient(self.root)
            kind_picker.grab_set()
            ttk.Label(
                kind_picker,
                text="What are you monitoring?",
                font=("Arial", 12, "bold"),
            ).pack(pady=(16, 8))
            kind_var = tk.StringVar(value=target_kind)
            for key, label in MONITOR_TARGET_KINDS.items():
                if key in allow_kinds:
                    ttk.Radiobutton(
                        kind_picker, text=label, variable=kind_var, value=key
                    ).pack(anchor=tk.W, padx=24, pady=2)
            picked: dict[str, str | None] = {"kind": None}

            def _kind_ok():
                picked["kind"] = kind_var.get()
                kind_picker.destroy()

            bf = ttk.Frame(kind_picker)
            bf.pack(pady=12)
            ttk.Button(bf, text="Next →", command=_kind_ok, width=12).pack(
                side=tk.LEFT, padx=4
            )
            ttk.Button(bf, text="Cancel", command=kind_picker.destroy, width=10).pack(
                side=tk.LEFT, padx=4
            )
            kind_picker.wait_window()
            if not picked["kind"]:
                return
            target_kind = picked["kind"]
            if require_db is None:
                require_db = target_kind == TARGET_CLOUD_DB

        if require_db is None:
            require_db = purpose == PURPOSE_CONNECTIONS

        self._cloud_wizard_opts = {
            **opts,
            "purpose": purpose,
            "require_db_identifier": require_db,
            "target_kind": target_kind,
        }

        title = (
            "Add Cloud DB Connection"
            if purpose == PURPOSE_CONNECTIONS
            else "Add Cloud Resource"
        )
        subtitle = (
            "Register a cloud database for Connections (DB identifier required).\n"
            "Use Objects / SQL Editor after resolving the endpoint."
            if purpose == PURPOSE_CONNECTIONS
            else (
                "VM metrics via cloud APIs — provide instance/VM identifiers (no database fields)."
                if target_kind == TARGET_VM
                else (
                    "Metrics via the provider API — enter the cloud resource type and identifier."
                    if target_kind == TARGET_CLOUD_SERVICE
                    else "Metrics via the provider API — DB identifier required only for cloud databases."
                )
            )
        )

        picker = tk.Toplevel(self.root)
        picker.title(f"{title} – Select Provider")
        picker.geometry("440x260")
        picker.resizable(False, False)
        picker.transient(self.root)
        picker.grab_set()

        ttk.Label(
            picker, text="Select Cloud Service Provider", font=("Arial", 12, "bold")
        ).pack(pady=(18, 6))
        ttk.Label(
            picker,
            text=subtitle,
            foreground="gray",
            font=("Arial", 9),
            justify=tk.CENTER,
        ).pack(pady=(0, 10))

        provider_var = tk.StringVar(value="AWS")
        rb_frame = ttk.Frame(picker)
        rb_frame.pack(pady=4)
        for p, info in self._CLOUD_PROVIDER_SCHEMAS.items():
            ttk.Radiobutton(
                rb_frame, text=f"{p}  ({info['label']})", variable=provider_var, value=p
            ).pack(anchor=tk.W, padx=20, pady=2)

        chosen: dict[str, str | None] = {"provider": None}

        def on_next():
            chosen["provider"] = provider_var.get()
            picker.destroy()

        btn_r = ttk.Frame(picker)
        btn_r.pack(pady=12)
        ttk.Button(
            btn_r, text="Next →", command=on_next, style="Primary.TButton", width=14
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_r, text="Cancel", command=picker.destroy, width=14).pack(
            side=tk.LEFT, padx=6
        )

        picker.update_idletasks()
        picker.geometry(
            f"+{(picker.winfo_screenwidth()  - picker.winfo_width())  // 2}"
            f"+{(picker.winfo_screenheight() - picker.winfo_height()) // 2}"
        )
        picker.wait_window()

        provider = chosen["provider"]
        if not provider:
            return

        self._open_cloud_provider_form(provider)

    def _open_cloud_provider_form(self, provider: str, edit_name: str | None = None):
        """
        Show the provider-specific form for registering (or editing) a cloud DB resource.

        edit_name: if set, pre-fills the form with the existing entry of that name and
                   saves back over it on submit (supports renaming via Display Name change).
        """
        schema = self._CLOUD_PROVIDER_SCHEMAS[provider]
        prefill = self.cloud_databases.get(edit_name, {}) if edit_name else {}
        is_edit = bool(edit_name)
        wopts = getattr(self, "_cloud_wizard_opts", None) or {}
        target_kind = wopts.get("target_kind", TARGET_CLOUD_DB)
        form_kind_title = TARGET_KIND_FORM_TITLES.get(target_kind, "Cloud Resource")

        dialog = tk.Toplevel(self.root)
        dialog.title(f"{'Edit' if is_edit else 'Add'} {form_kind_title} – {provider}")
        dialog.resizable(True, True)
        dialog.transient(self.root)
        dialog.grab_set()

        # ── header (fixed top) ────────────────────────────────────────────────
        hdr = ttk.Frame(dialog)
        hdr.pack(fill=tk.X, padx=14, pady=(12, 4))
        ttk.Label(hdr, text=schema["label"], font=("Arial", 11, "bold")).pack(
            anchor=tk.W
        )
        ttk.Label(
            hdr,
            text=f"Monitoring via: {schema['api']}",
            foreground="gray",
            font=("Arial", 9),
        ).pack(anchor=tk.W)
        ttk.Separator(dialog, orient=tk.HORIZONTAL).pack(
            fill=tk.X, padx=10, pady=(4, 0)
        )

        # ── footer (fixed bottom — packed before canvas so it stays pinned) ──
        ttk.Separator(dialog, orient=tk.HORIZONTAL).pack(
            side=tk.BOTTOM, fill=tk.X, padx=10
        )
        footer = ttk.Frame(dialog)
        footer.pack(side=tk.BOTTOM, fill=tk.X, pady=8)

        test_status_var = tk.StringVar(value="")
        test_status_lbl = ttk.Label(
            footer, textvariable=test_status_var, foreground="gray", font=("Arial", 9)
        )
        test_status_lbl.pack(pady=(0, 4))

        btn_row = ttk.Frame(footer)
        btn_row.pack()

        # ── scrollable body (fills remaining space between header and footer) ─
        scroll_area = ttk.Frame(dialog)
        scroll_area.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)

        body_frame = make_scrollable(scroll_area)

        entries: dict[str, ttk.Entry] = {}
        LBL_W = 28
        # Match width of the regular "Add Database Connection" form
        # (master_shell.py uses width=35 for host/port/user/etc.).
        FIELD_W = 35
        # Fields rendered as editable comboboxes that discovery repopulates.
        # (db_service_type is excluded — it already renders as a fixed-choice
        # combobox, and discovery still fills it via _apply_resource_fields.)
        _DISCOVERABLE_BY_PROVIDER = {
            "AWS": {"region", "sso_profile", "resource_name"},
            "Azure": {
                "subscription_id",
                "resource_group",
                "region",
                "resource_name",
            },
            "GCP": {"project_id", "region", "resource_name"},
        }
        discoverable_keys = _DISCOVERABLE_BY_PROVIDER.get(provider, set())

        def _add_field(parent, field_tuple: tuple):
            """Render a label + entry/combobox + optional help line from a field definition tuple.

            Tuple forms:
              (label, key, show, help_text)              — plain Entry / variant
              (label, key, show, help_text, [choices])   — Combobox (show ignored)

            ``show`` markers:
              ""       → plain text Entry
              "*"      → masked password Entry
              "file"   → Entry + 'Browse…' button (opens a file picker)
              "multi"  → multi-line tk.Text widget (for pasting JSON / certs)
            """
            lbl_text, key, show, *rest = field_tuple
            help_text = rest[0] if rest else ""
            choices = rest[1] if len(rest) > 1 else None
            grp = ttk.Frame(parent)
            grp.pack(fill=tk.X, pady=(2, 0), padx=4)

            if show == "multi" and not choices:
                ttk.Label(grp, text=lbl_text, anchor=tk.W).pack(
                    fill=tk.X, padx=2, pady=(2, 2)
                )
                widget = tk.Text(grp, height=6, wrap="word", font=self.ui_font)
                widget.pack(fill=tk.X, expand=True, padx=2)
                entries[key] = widget
                if help_text:
                    ttk.Label(
                        grp,
                        text=help_text,
                        foreground="#888888",
                        font=("Arial", 7),
                        wraplength=470,
                        justify=tk.LEFT,
                    ).pack(anchor=tk.W, padx=2, pady=(0, 2))
                return

            row = ttk.Frame(grp)
            row.pack(fill=tk.X)
            ttk.Label(row, text=lbl_text, width=LBL_W, anchor=tk.W).pack(side=tk.LEFT)
            if choices:
                var = tk.StringVar(value=choices[0])
                widget = ttk.Combobox(
                    row,
                    textvariable=var,
                    values=choices,
                    state="readonly",
                    width=FIELD_W - 2,
                )
                widget.pack(side=tk.LEFT)
                # Expose a .get() compatible with Entry so _collect_fields works unchanged
                entries[key] = widget
            elif show == "file":
                widget = ttk.Entry(row, width=FIELD_W)
                widget.pack(side=tk.LEFT)
                entries[key] = widget

                def _browse(_w=widget):
                    path = filedialog.askopenfilename(
                        parent=dialog,
                        title="Select file",
                        filetypes=[
                            ("JSON / PEM / All files",
                             "*.json *.pem *.key *.crt *.txt *"),
                            ("All files", "*.*"),
                        ],
                    )
                    if path:
                        _w.delete(0, tk.END)
                        _w.insert(0, path)

                ttk.Button(
                    row, text="Browse…", width=10, command=_browse
                ).pack(side=tk.LEFT, padx=(4, 0))
            elif key in discoverable_keys and not choices:
                widget = ttk.Combobox(
                    row,
                    width=FIELD_W - 2,
                    state="normal",
                )
                widget.pack(side=tk.LEFT)
                entries[key] = widget
            else:
                kw: dict = {"width": FIELD_W}
                if show == "*":
                    kw["show"] = "*"
                widget = ttk.Entry(row, **kw)
                widget.pack(side=tk.LEFT)
                entries[key] = widget
            if help_text:
                ttk.Label(
                    grp,
                    text=help_text,
                    foreground="#888888",
                    font=("Arial", 7),
                    wraplength=470,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, padx=(LBL_W * 7, 0), pady=(0, 2))

        # ── resource identification ───────────────────────────────────────────
        res_frame = ttk.LabelFrame(
            body_frame,
            text=RESOURCE_SECTION_TITLES.get(
                target_kind, "Cloud Resource Identification"
            ),
            padding=6,
        )
        res_frame.pack(fill=tk.X, pady=(6, 6), padx=4)
        for field in resource_fields_for(provider, target_kind):
            _add_field(res_frame, field)

        # ── authentication (tabbed) ───────────────────────────────────────────
        auth_frame = ttk.LabelFrame(body_frame, text="API Authentication", padding=6)
        auth_frame.pack(fill=tk.X, pady=(0, 6), padx=4)

        auth_nb = ttk.Notebook(auth_frame)
        auth_nb.pack(fill=tk.BOTH, expand=True)

        keys_tab = ttk.Frame(auth_nb, padding=4)
        env_tab = ttk.Frame(auth_nb, padding=4)
        pwd_tab = ttk.Frame(auth_nb, padding=4)
        sso_tab = ttk.Frame(auth_nb, padding=4)

        env_schema = schema.get("env_auth", {})
        env_label = env_schema.get("tab_label", "Environment / Instance Role")
        env_fields = env_schema.get("fields", [])
        env_help = env_schema.get("help", "")
        sso_schema = schema.get("sso_auth", {})
        sso_label = sso_schema.get("tab_label", "SSO / OIDC")
        sso_fields = sso_schema.get("fields", [])

        auth_tab_defs = [
            ("keys", " 🔑 Access Keys / Tokens ", keys_tab),
            ("env", f" 🌐 {env_label} ", env_tab),
            ("pwd", " 👤 Username / Password ", pwd_tab),
            ("sso", f" 🌐 {sso_label} ", sso_tab),
        ]
        auth_tab_modes = [mode for mode, _, _ in auth_tab_defs]
        for _mode, tab_text, tab_frame in auth_tab_defs:
            auth_nb.add(tab_frame, text=tab_text)

        for field in schema["keys_auth"]:
            _add_field(keys_tab, field)

        if env_help:
            ttk.Label(
                env_tab,
                text=env_help,
                foreground="gray",
                font=("Arial", 8),
                wraplength=430,
                justify=tk.LEFT,
            ).pack(anchor=tk.W, pady=(2, 6), padx=2)

        discover_status_var = tk.StringVar(value="")
        discover_status_lbl = ttk.Label(
            env_tab,
            textvariable=discover_status_var,
            foreground="gray",
            font=("Arial", 9),
            wraplength=460,
        )
        discover_status_lbl.pack(anchor=tk.W, padx=2, pady=(0, 4))

        discovered_resources: list[dict] = []
        discovered_resource_var = tk.StringVar()
        disc_row = ttk.Frame(env_tab)
        disc_row.pack(fill=tk.X, pady=(0, 4), padx=2)
        ttk.Label(
            disc_row, text="Discovered Resource", width=LBL_W, anchor=tk.W
        ).pack(side=tk.LEFT)
        discovered_resource_combo = ttk.Combobox(
            disc_row,
            textvariable=discovered_resource_var,
            state="normal",
            width=FIELD_W - 2,
        )
        discovered_resource_combo.pack(side=tk.LEFT)

        def _apply_resource_fields(fields: dict):
            for key, val in (fields or {}).items():
                if val is None:
                    continue
                val = str(val)
                w = entries.get(key)
                if w is None:
                    continue
                if isinstance(w, ttk.Combobox):
                    w.set(val)
                elif isinstance(w, tk.Text):
                    w.delete("1.0", tk.END)
                    w.insert("1.0", val)
                else:
                    w.delete(0, tk.END)
                    w.insert(0, val)

        def _on_discovered_resource_pick(_event=None):
            sel = discovered_resource_var.get()
            for res in discovered_resources:
                if res.get("label") == sel:
                    _apply_resource_fields(res.get("fields") or {})
                    break

        discovered_resource_combo.bind(
            "<<ComboboxSelected>>", _on_discovered_resource_pick
        )

        def _apply_discovery_result(result):
            nonlocal discovered_resources
            if result.error:
                discover_status_var.set(result.error)
                discover_status_lbl.config(foreground="#c62828")
                return

            discovered_resources = list(result.resources or [])
            labels = [
                r.get("label", "") for r in discovered_resources if r.get("label")
            ]
            discovered_resource_combo["values"] = labels
            if labels:
                discovered_resource_var.set(labels[0])
                _apply_resource_fields(discovered_resources[0].get("fields") or {})

            detected = result.detected or {}
            _apply_resource_fields(detected)

            account_key = {
                "AWS": "sso_profile",
                "Azure": "subscription_id",
                "GCP": "project_id",
            }.get(provider)
            if account_key and result.accounts:
                acct_ids = [a.get("id", "") for a in result.accounts if a.get("id")]
                w = entries.get(account_key)
                if isinstance(w, ttk.Combobox) and acct_ids:
                    w["values"] = acct_ids
                    if detected.get(account_key):
                        w.set(detected[account_key])
                    elif acct_ids:
                        w.set(acct_ids[0])

            if provider == "AWS" and detected.get("profiles"):
                w = entries.get("sso_profile")
                if isinstance(w, ttk.Combobox):
                    w["values"] = detected["profiles"]
                    if detected.get("sso_profile"):
                        w.set(detected["sso_profile"])

            region_w = entries.get("region")
            if isinstance(region_w, ttk.Combobox) and result.regions:
                region_w["values"] = result.regions
                if detected.get("region"):
                    region_w.set(detected["region"])

            resource_w = entries.get("resource_name")
            if isinstance(resource_w, ttk.Combobox) and discovered_resources:
                res_ids = [
                    (r.get("fields") or {}).get("resource_name", "")
                    for r in discovered_resources
                ]
                res_ids = [rid for rid in res_ids if rid]
                if res_ids:
                    resource_w["values"] = res_ids

            parts = []
            if result.regions:
                parts.append(f"{len(result.regions)} region(s)")
            if result.accounts:
                parts.append(f"{len(result.accounts)} account(s)")
            if discovered_resources:
                parts.append(f"{len(discovered_resources)} resource(s)")
            msg = "✓ Discovery complete"
            if parts:
                msg += " — " + ", ".join(parts)
            if result.warnings:
                msg += "\nWarnings: " + "; ".join(result.warnings[:3])
            discover_status_var.set(msg)
            discover_status_lbl.config(foreground="#2e7d32")

        def do_discover():
            data = _collect_fields()
            data["auth_mode"] = "env"
            discover_status_var.set("Discovering…")
            discover_status_lbl.config(foreground="gray")
            dialog.update_idletasks()

            def _run():
                result = CloudProviderRegistry.discover(data, target_kind)

                def _update():
                    _apply_discovery_result(result)

                dialog.after(0, _update)

            threading.Thread(target=_run, daemon=True).start()

        ttk.Button(
            env_tab,
            text="Auto-detect & List Resources",
            command=do_discover,
        ).pack(anchor=tk.W, padx=2, pady=(0, 6))

        for field in env_fields:
            _add_field(env_tab, field)

        for field in schema["pwd_auth"]:
            _add_field(pwd_tab, field)

        # SSO tab: note + fields
        if provider == "AWS":
            sso_info = (
                "Two options:\n"
                "• aws login (recommended): leave Start URL blank, optionally set a\n"
                "  Profile, then click 'Test Connection' to run `aws login` (opens a\n"
                "  browser). Works with the AWS CLI v2 `aws login` command.\n"
                "• IAM Identity Center (SSO): fill in Start URL + SSO Region to start\n"
                "  the device-authorization flow instead."
            )
        elif provider == "Azure":
            sso_info = (
                "Azure AD Device Code flow — opens a browser for interactive\n"
                "login (supports MFA and Conditional Access policies)."
            )
        elif provider == "GCP":
            sso_info = (
                "Workforce Identity / gcloud ADC device flow — opens a browser\n"
                "for Google sign-in. Leave fields blank to use gcloud ADC default."
            )
        else:
            sso_info = (
                "OIDC device-authorization flow — enter your OIDC provider details."
            )

        ttk.Label(
            sso_tab,
            text=sso_info,
            foreground="gray",
            font=("Arial", 8),
            wraplength=430,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(2, 6), padx=2)

        for field in sso_fields:
            _add_field(sso_tab, field)

        # ── MFA ───────────────────────────────────────────────────────────────
        mfa_frame = ttk.LabelFrame(
            body_frame, text="Multi-Factor Authentication (MFA)", padding=6
        )
        mfa_frame.pack(fill=tk.X, pady=(0, 6), padx=4)

        mfa_enabled_var = tk.BooleanVar(value=False)
        mfa_type_var = tk.StringVar(value=schema.get("mfa_hint", self._MFA_TYPES[0]))

        mfa_check_row = ttk.Frame(mfa_frame)
        mfa_check_row.pack(fill=tk.X, pady=(2, 4), padx=4)
        ttk.Checkbutton(
            mfa_check_row,
            text="Require MFA / 2FA on activation",
            variable=mfa_enabled_var,
            command=lambda: _toggle_mfa(),
        ).pack(side=tk.LEFT)
        ttk.Label(
            mfa_check_row,
            text=f"({'recommended' if schema.get('mfa_common') else 'optional'})",
            foreground="gray",
            font=("Arial", 8),
        ).pack(side=tk.LEFT, padx=6)

        mfa_type_row = ttk.Frame(mfa_frame)
        mfa_type_row.pack(fill=tk.X, pady=2, padx=4)
        ttk.Label(mfa_type_row, text="MFA Type", width=LBL_W, anchor=tk.W).pack(
            side=tk.LEFT
        )
        mfa_type_combo = ttk.Combobox(
            mfa_type_row,
            textvariable=mfa_type_var,
            values=self._MFA_TYPES,
            state="readonly",
            width=32,
        )
        mfa_type_combo.pack(side=tk.LEFT)

        mfa_note = ttk.Label(
            mfa_frame,
            text="You will be prompted to enter the MFA code each time you activate monitoring.",
            foreground="gray",
            font=("Arial", 8),
            wraplength=460,
        )
        mfa_note.pack(anchor=tk.W, padx=4, pady=(2, 0))

        def _toggle_mfa():
            mfa_type_combo.config(
                state="readonly" if mfa_enabled_var.get() else "disabled"
            )

        _toggle_mfa()

        # ── SQL connection (Connections tab only) ─────────────────────────────
        wizard_purpose = (getattr(self, "_cloud_wizard_opts", None) or {}).get(
            "purpose", PURPOSE_MONITOR
        )
        if wizard_purpose == PURPOSE_CONNECTIONS:
            sql_frame = ttk.LabelFrame(
                body_frame,
                text="SQL Connection (for Load Saved / Objects / SQL Editor)",
                padding=6,
            )
            sql_frame.pack(fill=tk.X, pady=(0, 6), padx=4)
            ttk.Label(
                sql_frame,
                text="Host/port are auto-filled from AWS RDS when possible. "
                "Enter database username and password for direct SQL access.",
                foreground="gray",
                font=("Arial", 8),
                wraplength=460,
                justify=tk.LEFT,
            ).pack(anchor=tk.W, padx=4, pady=(0, 4))

            for field in (
                ("DB Type", "sql_db_type", "", "MySQL, MariaDB, or PostgreSQL", ["MySQL", "MariaDB", "PostgreSQL", "Oracle"]),
                ("SQL Host", "sql_host", "", "RDS endpoint hostname — use Resolve if blank.", None),
                ("SQL Port", "sql_port", "", "Database port, usually 3306.", None),
                ("Database Name", "sql_database", "", "Default schema/database to connect to.", None),
                ("DB Username", "sql_username", "", "Database user (not IAM user).", None),
                ("DB Password", "sql_password", "*", "Database password.", None),
            ):
                if field[4]:
                    _add_field(sql_frame, field[:4] + [field[4]])
                else:
                    _add_field(sql_frame, field[:4])

            def _resolve_sql_endpoint():
                from common.cloud.sql_bridge import resolve_aws_rds_sql_endpoint

                data = _collect_fields()
                if (data.get("provider") or "").upper() != "AWS":
                    messagebox.showinfo(
                        "Resolve Endpoint",
                        "Auto-resolve is supported for AWS RDS only. "
                        "Enter the SQL host manually for other providers.",
                        parent=dialog,
                    )
                    return
                resolved = resolve_aws_rds_sql_endpoint(data)
                if not resolved:
                    messagebox.showwarning(
                        "Resolve Endpoint",
                        "Could not resolve RDS endpoint. Check region, DB identifier, "
                        "and API credentials, then try Test Connection first.",
                        parent=dialog,
                    )
                    return
                mapping = {
                    "sql_host": resolved.get("host", ""),
                    "sql_port": resolved.get("port", ""),
                    "sql_db_type": resolved.get("db_type", ""),
                }
                for key, val in mapping.items():
                    w = entries.get(key)
                    if w is None or not val:
                        continue
                    if isinstance(w, ttk.Combobox):
                        w.set(val)
                    else:
                        w.delete(0, tk.END)
                        w.insert(0, val)
                test_status_var.set(
                    f"Resolved SQL endpoint: {resolved.get('host')}:{resolved.get('port')}"
                )
                test_status_lbl.config(foreground="#2e7d32")

            ttk.Button(
                sql_frame,
                text="Resolve SQL endpoint from cloud",
                command=_resolve_sql_endpoint,
            ).pack(anchor=tk.W, padx=4, pady=(4, 0))

        # ── pre-fill fields when editing an existing entry ────────────────────
        if prefill:
            sql_prefill = prefill.get("sql_connection") or {}
            if sql_prefill:
                prefill = dict(prefill)
                prefill.setdefault("sql_db_type", sql_prefill.get("db_type", ""))
                prefill.setdefault("sql_host", sql_prefill.get("host", ""))
                prefill.setdefault("sql_port", sql_prefill.get("port", ""))
                prefill.setdefault("sql_database", sql_prefill.get("service_or_db", ""))
                prefill.setdefault("sql_username", sql_prefill.get("username", ""))
                prefill.setdefault("sql_password", sql_prefill.get("password", ""))
            for key, widget in entries.items():
                val = prefill.get(key, "")
                if val:
                    if isinstance(widget, ttk.Combobox):
                        widget.set(val)
                    elif isinstance(widget, tk.Text):
                        widget.delete("1.0", tk.END)
                        widget.insert("1.0", val)
                    else:
                        widget.delete(0, tk.END)
                        widget.insert(0, val)
            # Restore MFA settings
            mfa_enabled_var.set(bool(prefill.get("mfa_enabled", False)))
            if prefill.get("mfa_type"):
                mfa_type_var.set(prefill["mfa_type"])
            _toggle_mfa()
            # Restore active auth tab
            saved_mode = prefill.get("auth_mode", "keys")
            try:
                auth_nb.select(auth_tab_modes.index(saved_mode))
            except ValueError:
                auth_nb.select(0)

        # ── notes ─────────────────────────────────────────────────────────────
        ttk.Label(
            body_frame,
            text="* Required.  Credentials are kept in memory only and never written to disk.",
            foreground="gray",
            font=("Arial", 8),
            wraplength=460,
        ).pack(anchor=tk.W, padx=4, pady=(0, 6))

        def _collect_fields() -> dict:
            """Collect all current form values into a flat dict."""

            def _get(key: str) -> str:
                w = entries.get(key)
                if w is None:
                    return ""
                if isinstance(w, tk.Text):
                    return w.get("1.0", tk.END).strip()
                return w.get().strip()

            active_tab = auth_nb.index(auth_nb.select())
            auth_mode = (
                auth_tab_modes[active_tab]
                if 0 <= active_tab < len(auth_tab_modes)
                else "keys"
            )
            data: dict = {
                "provider": provider,
                "auth_mode": auth_mode,
                "mfa_enabled": mfa_enabled_var.get(),
                "mfa_type": mfa_type_var.get() if mfa_enabled_var.get() else "",
                "monitoring": False,
            }
            for f in resource_fields_for(provider, target_kind):
                data[f[1]] = _get(f[1])
            for f in schema["keys_auth"]:
                data[f[1]] = _get(f[1])
            for f in env_fields:
                data[f[1]] = _get(f[1])
            for f in schema["pwd_auth"]:
                data[f[1]] = _get(f[1])
            for f in sso_fields:
                data[f[1]] = _get(f[1])
            wopts = getattr(self, "_cloud_wizard_opts", {}) or {}
            data["purpose"] = wopts.get("purpose", PURPOSE_MONITOR)
            data["target_kind"] = wopts.get("target_kind", TARGET_CLOUD_DB)
            if data["purpose"] == PURPOSE_CONNECTIONS:
                data["sql_connection"] = {
                    "db_type": _get("sql_db_type"),
                    "host": _get("sql_host"),
                    "port": _get("sql_port"),
                    "service_or_db": _get("sql_database"),
                    "username": _get("sql_username"),
                    "password": _get("sql_password"),
                }
            return data

        def _validate_required(data: dict) -> str | None:
            wopts = getattr(self, "_cloud_wizard_opts", {}) or {}
            return validate_cloud_profile(
                data,
                provider,
                schema,
                require_db_identifier=bool(
                    wopts.get("require_db_identifier", False)
                ),
                target_kind=wopts.get("target_kind", TARGET_CLOUD_DB),
            )

        def do_test():
            data = _collect_fields()
            err = _validate_required(data)
            if err:
                messagebox.showwarning("Missing Field", err, parent=dialog)
                return
            test_status_var.set("Testing…")
            test_status_lbl.config(foreground="gray")
            dialog.update_idletasks()

            def _run():
                result, status = self._run_cloud_api_test(data)
                colour = {
                    "ok": "#2e7d32",
                    "auth": "#e65100",
                    "sso": "#1565c0",
                    "error": "#c62828",
                }.get(status, "gray")

                def _update():
                    test_status_var.set(result)
                    test_status_lbl.config(foreground=colour)

                dialog.after(0, _update)

            threading.Thread(target=_run, daemon=True).start()

        def do_save():
            data = _collect_fields()
            err = _validate_required(data)
            if err:
                messagebox.showwarning("Missing Field", err, parent=dialog)
                return

            display_name = data["display_name"]
            resource_name = data["resource_name"]

            if is_edit:
                # Remove old entry (handles rename: old_name may differ from display_name)
                if edit_name != display_name:
                    # Name changed — remove old key, update active references
                    self.cloud_databases.pop(edit_name, None)
                    if edit_name in self.active_cloud_databases:
                        self.active_cloud_databases[display_name] = (
                            self.active_cloud_databases.pop(edit_name)
                        )
                    if edit_name in self.active_cloud_monitors:
                        self.active_cloud_monitors[display_name] = (
                            self.active_cloud_monitors.pop(edit_name)
                        )
                    self.update_cloud_db_listbox()
                # If currently active, drop the old monitor so it re-authenticates on next poll
                self.active_cloud_monitors.pop(display_name, None)
                self._clear_cloud_liveness_state(display_name)
                sync_msg = ""
                if data.get("purpose") == PURPOSE_CONNECTIONS:
                    from common.cloud.sql_bridge import enrich_sql_connection, sync_cloud_db_to_saved_connections

                    data = enrich_sql_connection(data)
                    cm = getattr(self, "connection_manager", None)
                    if cm is not None:
                        _ok, sync_msg = sync_cloud_db_to_saved_connections(data, cm)
                        if _ok:
                            cm.connections = cm.load_connections()
                self.cloud_databases[display_name] = data
                self.cloud_connection_manager.save_cloud_databases(self.cloud_databases)
                self.update_status(
                    f"Cloud connection '{display_name}' ({provider}) updated.",
                    "success",
                )
                update_note = (
                    (sync_msg + "\n\n" if sync_msg else "")
                    + "Use Load Saved on the Connections tab to connect."
                    if data.get("purpose") == PURPOSE_CONNECTIONS
                    else "If it was active in the monitoring list, re-authenticate by removing\n"
                    "and re-adding it via 'Select Database'."
                )
                messagebox.showinfo(
                    "Connection Updated",
                    f"'{display_name}' has been updated.\n\n{update_note}",
                    parent=dialog,
                )
            else:
                if display_name in self.cloud_databases:
                    messagebox.showwarning(
                        "Duplicate",
                        f"'{display_name}' is already registered.",
                        parent=dialog,
                    )
                    return
                self.cloud_databases[display_name] = data
                self.cloud_connection_manager.save_cloud_databases(self.cloud_databases)
                sync_msg = ""
                if data.get("purpose") == PURPOSE_CONNECTIONS:
                    from common.cloud.sql_bridge import enrich_sql_connection, sync_cloud_db_to_saved_connections

                    enriched = enrich_sql_connection(data)
                    self.cloud_databases[display_name] = enriched
                    self.cloud_connection_manager.save_cloud_databases(self.cloud_databases)
                    cm = getattr(self, "connection_manager", None)
                    if cm is not None:
                        _ok, sync_msg = sync_cloud_db_to_saved_connections(enriched, cm)
                        if _ok:
                            cm.connections = cm.load_connections()
                self.update_status(
                    f"Cloud connection '{display_name}' ({provider}) saved.", "success"
                )
                mfa_line = (
                    f"MFA      : {data['mfa_type']}"
                    if data["mfa_enabled"]
                    else "MFA      : disabled"
                )
                messagebox.showinfo(
                    "Connection Saved",
                    f"'{display_name}' has been saved.\n\n"
                    f"Provider : {provider}\n"
                    f"Resource : {resource_name or '(optional)'}\n"
                    f"Auth     : {data['auth_mode']}\n"
                    f"{mfa_line}\n\n"
                    + (
                        (sync_msg + "\n\n" if sync_msg else "")
                        + "Use Load Saved on the Connections tab to connect."
                        if data.get("purpose") == PURPOSE_CONNECTIONS
                        else "Use 'Select Database' to add it to the monitoring list."
                    ),
                    parent=dialog,
                )
            cb = (getattr(self, "_cloud_wizard_opts", None) or {}).get("on_saved")
            if cb:
                try:
                    cb(data)
                except Exception:
                    pass
            dialog.destroy()

        # ── wire up buttons (btn_row created in footer block above) ─────────
        ttk.Button(btn_row, text="Test Connection", command=do_test, width=16).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(
            btn_row, text="Save", command=do_save, style="Primary.TButton", width=12
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_row, text="Cancel", command=dialog.destroy, width=10).pack(
            side=tk.LEFT, padx=4
        )

        # ── initial size and centre ───────────────────────────────────────────
        dialog.update_idletasks()
        field_count = (
            len(schema["resource"])
            + max(len(schema["keys_auth"]), len(schema["pwd_auth"]))
            + 3
        )
        h = min(780, 240 + field_count * 40)
        dialog.geometry(f"600x{h}")
        dialog.update_idletasks()
        cx = (dialog.winfo_screenwidth() - dialog.winfo_width()) // 2
        cy = (dialog.winfo_screenheight() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{cx}+{cy}")

    def _cloud_db_key_from_index(self, index: int) -> str | None:
        """Return the active_cloud_databases key for a given monitoring listbox row index."""
        keys = list(self.active_cloud_databases.keys())
        return keys[index] if index < len(keys) else None

    def _run_cloud_api_test(self, data: dict) -> tuple[str, str]:
        """
        Make an authenticated API call to the provider's monitoring endpoint.

        Returns (message, status) where status is:
          'ok'    – authenticated, resource confirmed reachable
          'auth'  – credentials rejected (401/403)
          'sso'   – SSO device-auth flow started (browser action required)
          'error' – network failure, missing fields, or unexpected error
        """
        provider = data.get("provider", "")
        auth_mode = data.get("auth_mode", "keys")
        region = data.get("region", "us-east-1")

        # ── shared SSO device-auth helper ─────────────────────────────────────
        def _normalize_sso_start_url(raw_url: str) -> str:
            """Clean up a pasted IAM Identity Center portal URL.

            AWS's start_device_authorization rejects anything but the bare
            portal URL (``https://<host>/start``). Users frequently paste the
            value with surrounding whitespace, a trailing slash, or the
            ``/#/`` SPA fragment the portal shows in the address bar — all of
            which trigger "invalid start url provided".
            """
            url = (raw_url or "").strip()
            if not url:
                return url
            # Add scheme if the user pasted a bare host.
            if not re.match(r"^https?://", url, re.IGNORECASE):
                url = "https://" + url
            # Drop any SPA fragment (e.g. ".../start/#/" or ".../start#/...").
            url = url.split("#", 1)[0]
            # Strip trailing slashes so ".../start/" becomes ".../start".
            url = url.rstrip("/")
            return url

        def _aws_cli_login() -> tuple[str, str]:
            """Authenticate via the AWS CLI ``aws login`` command.

            This is the modern "Login for local development using AWS
            Management Console credentials" flow (AWS CLI v2). It opens a
            browser, stores a refresh token, and the resulting credentials are
            picked up automatically by boto3's default credential chain
            (botocore's 'login' provider). Mirrors the Azure ``az login`` path.
            """
            profile = (data.get("sso_profile", "") or "").strip()
            aws_cmd = ["aws", "login"]
            if profile:
                aws_cmd += ["--profile", profile]
            try:
                result = subprocess.run(
                    aws_cmd, capture_output=True, text=True, timeout=300
                )
                if result.returncode != 0:
                    err = (result.stderr or result.stdout or "").strip()
                    return (f"aws login failed: {err[:300]}", "error")

                import boto3

                session = boto3.Session(
                    profile_name=profile or None, region_name=region
                )
                sts = session.client("sts")
                identity = sts.get_caller_identity()
                instance_id = data.get("resource_name", "")
                if instance_id:
                    rds = session.client("rds", region_name=region)
                    resp = rds.describe_db_instances(
                        DBInstanceIdentifier=instance_id
                    )
                    instances = resp.get("DBInstances", [])
                    if instances:
                        state = instances[0].get("DBInstanceStatus", "unknown")
                        return (
                            f"✓ aws login complete — authenticated as {identity.get('Arn')}\n"
                            f"RDS '{instance_id}' — status: {state}.",
                            "ok",
                        )
                return (
                    f"✓ aws login complete — authenticated as {identity.get('Arn')}",
                    "ok",
                )
            except FileNotFoundError:
                return (
                    "'aws' command not found. Install AWS CLI v2: "
                    "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
                    "error",
                )
            except subprocess.TimeoutExpired:
                return ("aws login timed out after 5 minutes.", "error")
            except ImportError:
                return (
                    "boto3 is required for AWS authentication. Install: pip install boto3",
                    "error",
                )
            except Exception as exc:
                return (f"aws login verification failed: {exc}", "error")

        def _aws_sso_device_auth() -> tuple[str, str]:
            sso_start_url = _normalize_sso_start_url(data.get("sso_start_url", ""))
            sso_account_id = data.get("sso_account_id", "").strip()
            sso_role_name = data.get("sso_role_name", "").strip()
            sso_region = (data.get("sso_region", "") or "").strip()
            # No Identity Center Start URL → use the `aws login` flow instead.
            if not sso_start_url:
                return _aws_cli_login()
            if not all([sso_account_id, sso_role_name]):
                return (
                    "Account ID and Role Name are required for IAM Identity Center "
                    "(or leave Start URL blank to authenticate with `aws login`).",
                    "error",
                )
            if not sso_region:
                return (
                    "SSO Region is required for IAM Identity Center. Enter the region "
                    "where your Identity Center instance is hosted (the same value as "
                    "'sso_region' in your ~/.aws/config) — it may differ from the RDS region.",
                    "error",
                )
            try:
                import boto3

                # The sso-oidc client MUST be created in the SAME region as the
                # Identity Center instance, otherwise AWS rejects the start URL
                # with "invalid start url provided".
                sso_oidc = boto3.client("sso-oidc", region_name=sso_region)
                client_reg = sso_oidc.register_client(
                    clientName="DbManagementTool",
                    clientType="public",
                    scopes=["sso:account:access"],
                )
                dev_auth = sso_oidc.start_device_authorization(
                    clientId=client_reg["clientId"],
                    clientSecret=client_reg["clientSecret"],
                    startUrl=sso_start_url,
                )
                return (
                    f"SSO device flow started — open this URL in your browser:\n"
                    f"{dev_auth['verificationUriComplete']}\n"
                    f"User code: {dev_auth['userCode']}",
                    "sso",
                )
            except ImportError:
                return (
                    "boto3 is required for IAM Identity Center SSO. Install: pip install boto3",
                    "error",
                )
            except Exception as exc:
                msg = str(exc)
                if "invalid start url" in msg.lower():
                    return (
                        "SSO init failed: invalid start URL. Verify that the Start URL "
                        f"('{sso_start_url}') exactly matches your AWS access portal URL, "
                        f"and that the SSO Region ('{sso_region}') is the region hosting your "
                        "Identity Center instance. A region/URL mismatch is the most common cause "
                        "of this error.",
                        "error",
                    )
                return (f"SSO init failed: {exc}", "error")

        def _azure_device_code_auth() -> tuple[str, str]:
            # Run `az login --tenant <id>` — same as running it from the terminal,
            # browser opens automatically and satisfies Conditional Access / MFA.
            tenant_id = data.get("tenant_id", "")
            az_cmd = ["az", "login"]
            if tenant_id:
                az_cmd += ["--tenant", tenant_id]
            try:
                result = subprocess.run(
                    az_cmd, capture_output=True, text=True, timeout=300
                )
                if result.returncode != 0:
                    err = (result.stderr or result.stdout or "").strip()
                    return (f"az login failed: {err[:300]}", "error")
                # Verify the token is usable
                from azure.identity import AzureCliCredential

                cred = AzureCliCredential()
                cred.get_token("https://management.azure.com/.default")
                return (
                    "✓ Azure CLI login completed — token obtained. Use 'Select Database' to activate monitoring.",
                    "ok",
                )
            except FileNotFoundError:
                return (
                    "'az' command not found. Install Azure CLI: https://aka.ms/installazureclimacos",
                    "error",
                )
            except subprocess.TimeoutExpired:
                return ("az login timed out after 5 minutes.", "error")
            except Exception as exc:
                return (f"Azure CLI login error: {exc}", "error")

        def _gcp_device_auth() -> tuple[str, str]:
            """Run the interactive gcloud ADC device login.

            Pops up a dialog with the verification URL, an 'Open in Browser'
            button, and an entry field for the authorization code that Google
            returns after sign-in.  Falls back to Google's OAuth2 device-code
            endpoint when gcloud is not installed.
            """
            ok, msg = self._gcp_sso_callback()
            if not ok:
                # If gcloud isn't installed, try the OAuth2 device-code fallback
                if "not installed" in msg.lower() and data.get("sso_client_id"):
                    return _gcp_oauth_device_code_fallback()
                return (f"✗ {msg}", "error")

            project_id = data.get("project_id", "")
            resource_name = data.get("resource_name", "")
            try:
                from googleapiclient import discovery
                import google.auth

                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                sqladmin = discovery.build(
                    "sqladmin",
                    "v1beta4",
                    credentials=creds,
                    cache_discovery=False,
                )
                if resource_name:
                    sqladmin.instances().get(
                        project=project_id, instance=resource_name
                    ).execute()
                else:
                    sqladmin.instances().list(project=project_id).execute()
                return (
                    f"✓ gcloud sign-in complete — Cloud SQL project "
                    f"'{project_id}' reachable.",
                    "ok",
                )
            except Exception as exc:
                return (
                    "✓ gcloud sign-in complete, but Cloud SQL Admin API test "
                    f"failed: {exc}",
                    "auth",
                )

        def _gcp_oauth_device_code_fallback() -> tuple[str, str]:
            client_id = data.get("sso_client_id", "")
            try:
                import urllib.request, urllib.error, urllib.parse, ssl, json

                ctx = ssl.create_default_context()
                body = urllib.parse.urlencode(
                    {
                        "client_id": client_id,
                        "scope": "https://www.googleapis.com/auth/cloud-platform",
                    }
                ).encode()
                req = urllib.request.Request(
                    "https://oauth2.googleapis.com/device/code",
                    data=body,
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                    dc = json.loads(resp.read())
                return (
                    f"GCP device code flow:\nOpen: {dc.get('verification_url', '')}\n"
                    f"Code: {dc.get('user_code', '')}",
                    "sso",
                )
            except urllib.error.HTTPError as he:
                body = he.read(512).decode(errors="replace")
                return (
                    f"GCP device code request failed (HTTP {he.code}): {body[:160]}",
                    "error",
                )
            except Exception as exc:
                return (f"GCP device code error: {exc}", "error")

        def _generic_oidc_device_auth() -> tuple[str, str]:
            endpoint = data.get("sso_endpoint", "")
            client_id = data.get("sso_client_id", "")
            client_sec = data.get("sso_client_sec", "") or None
            if not endpoint or not client_id:
                return ("OIDC Endpoint and Client ID are required.", "error")
            try:
                import urllib.request, urllib.error, urllib.parse, ssl, json

                ctx = ssl.create_default_context()
                body_params: dict = {"client_id": client_id, "scope": "openid profile"}
                if client_sec:
                    body_params["client_secret"] = client_sec
                body = urllib.parse.urlencode(body_params).encode()
                # Try RFC 8628 device_authorization_endpoint
                dc_url = endpoint.rstrip("/") + "/device_authorization"
                req = urllib.request.Request(dc_url, data=body, method="POST")
                with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                    dc = json.loads(resp.read())
                return (
                    f"OIDC device flow started:\n"
                    f"Open: {dc.get('verification_uri_complete', dc.get('verification_uri', ''))}\n"
                    f"Code: {dc.get('user_code', '')}",
                    "sso",
                )
            except urllib.error.HTTPError as he:
                body = he.read(512).decode(errors="replace")
                return (
                    f"OIDC device code request failed (HTTP {he.code}): {body[:160]}",
                    "error",
                )
            except Exception as exc:
                return (f"OIDC device code error: {exc}", "error")

        # ── AWS ───────────────────────────────────────────────────────────────
        if provider == "AWS":
            access_key = data.get("access_key_id", "")
            secret_key = data.get("secret_access_key", "")
            session_tk = data.get("session_token", "") or None
            username = data.get("username", "")
            password = data.get("password", "")

            if auth_mode == "env":
                try:
                    import boto3

                    profile = (data.get("sso_profile", "") or "").strip() or None
                    session = boto3.Session(
                        profile_name=profile, region_name=region or None
                    )
                    identity = session.client("sts").get_caller_identity()
                    instance_id = data.get("resource_name", "")
                    msg = f"✓ Environment credentials — authenticated as {identity.get('Arn')}"
                    if instance_id:
                        rds = session.client("rds", region_name=region or None)
                        resp = rds.describe_db_instances(
                            DBInstanceIdentifier=instance_id
                        )
                        instances = resp.get("DBInstances", [])
                        if instances:
                            state = instances[0].get("DBInstanceStatus", "unknown")
                            msg += f"\nRDS '{instance_id}' — status: {state}."
                    return (msg, "ok")
                except ImportError:
                    return (
                        "boto3 is required for AWS authentication. Install: pip install boto3",
                        "error",
                    )
                except Exception as exc:
                    return (
                        f"✗ AWS environment credentials failed: {exc}",
                        "auth",
                    )

            if auth_mode == "sso":
                return _aws_sso_device_auth()

            if auth_mode == "pwd":
                # IAM username+password cannot call AWS APIs — explain clearly
                return (
                    "AWS API calls require Access Key ID + Secret Access Key.\n"
                    "IAM username/password is for console login only.\n"
                    "Use the 'Access Keys / Tokens' tab, or use 'IAM Identity Center'\n"
                    "tab to authenticate via SSO device flow.",
                    "error",
                )

            if not access_key or not secret_key:
                return ("Access Key ID and Secret Access Key are required.", "error")

            try:
                import boto3

                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=session_tk,
                    region_name=region,
                )
                # Cheapest authenticated call — verify identity first
                sts = session.client("sts")
                identity = sts.get_caller_identity()

                instance_id = data.get("resource_name", "")
                if instance_id:
                    rds = session.client("rds", region_name=region)
                    resp = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
                    instances = resp.get("DBInstances", [])
                    if instances:
                        state = instances[0].get("DBInstanceStatus", "unknown")
                        return (
                            f"✓ Authenticated as {identity.get('Arn')}\n"
                            f"RDS '{instance_id}' — status: {state}.",
                            "ok",
                        )
                return (f"✓ Authenticated as {identity.get('Arn')}", "ok")

            except ImportError:
                pass  # fall through to SigV4 fallback
            except Exception as exc:
                msg = str(exc)
                if any(
                    k in msg
                    for k in (
                        "AuthFailure",
                        "InvalidClientTokenId",
                        "InvalidAccessKeyId",
                        "AccessDenied",
                        "SignatureDoesNotMatch",
                    )
                ):
                    return (f"✗ AWS auth rejected: {msg}", "auth")
                if "DBInstanceNotFound" in msg:
                    return (
                        f"✓ AWS credentials valid — RDS instance not found in {region}.",
                        "ok",
                    )
                return (f"✗ AWS error: {msg}", "error")

            # SigV4 fallback (no boto3)
            try:
                import urllib.request, urllib.error, ssl, hmac, hashlib, datetime

                instance_id = data.get("resource_name", "")
                svc = "rds"
                host = f"{svc}.{region}.amazonaws.com"
                params = (
                    "Action=DescribeDBInstances"
                    + (f"&DBInstanceIdentifier={instance_id}" if instance_id else "")
                    + "&Version=2014-10-31"
                )
                now = datetime.datetime.now(datetime.timezone.utc)
                amzdate = now.strftime("%Y%m%dT%H%M%SZ")
                datestamp = now.strftime("%Y%m%d")

                def _sign(key, msg):
                    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

                sig_key = _sign(
                    _sign(
                        _sign(_sign(("AWS4" + secret_key).encode(), datestamp), region),
                        svc,
                    ),
                    "aws4_request",
                )
                extra_hdr = f"x-amz-security-token:{session_tk}\n" if session_tk else ""
                extra_sh = ";x-amz-security-token" if session_tk else ""
                canon_hdrs = f"host:{host}\nx-amz-date:{amzdate}\n{extra_hdr}"
                signed_hdrs = f"host;x-amz-date{extra_sh}"
                ph = hashlib.sha256(b"").hexdigest()
                cr = f"GET\n/\n{params}\n{canon_hdrs}\n{signed_hdrs}\n{ph}"
                scope = f"{datestamp}/{region}/{svc}/aws4_request"
                sts_str = (
                    f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n"
                    f"{hashlib.sha256(cr.encode()).hexdigest()}"
                )
                sig = hmac.new(sig_key, sts_str.encode(), hashlib.sha256).hexdigest()
                auth_hdr = (
                    f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
                    f"SignedHeaders={signed_hdrs}, Signature={sig}"
                )
                hdrs = {"x-amz-date": amzdate, "Authorization": auth_hdr}
                if session_tk:
                    hdrs["x-amz-security-token"] = session_tk
                ctx = ssl.create_default_context()
                req = urllib.request.Request(
                    f"https://{host}/?{params}", headers=hdrs, method="GET"
                )
                with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                    return (f"✓ AWS RDS API responded (HTTP {resp.status}).", "ok")
            except urllib.error.HTTPError as he:
                if he.code in (401, 403):
                    return (f"✗ AWS credentials rejected (HTTP {he.code}).", "auth")
                body = he.read(512).decode(errors="replace")
                if "DBInstanceNotFound" in body:
                    return (
                        f"✓ AWS credentials valid — RDS instance not found in {region}.",
                        "ok",
                    )
                return (f"AWS responded HTTP {he.code}: {body[:120]}", "error")
            except Exception as exc:
                return (f"✗ AWS connection failed: {exc}", "error")

        # ── Azure ─────────────────────────────────────────────────────────────
        elif provider == "Azure":
            tenant_id = data.get("tenant_id", "")
            client_id = data.get("client_id", "")
            client_sec = data.get("client_secret", "")
            bearer = data.get("bearer_token", "") or None
            sub_id = data.get("subscription_id", "")
            rg = data.get("resource_group", "")
            res_name = data.get("resource_name", "")
            username = data.get("username", "")
            password = data.get("password", "")

            if auth_mode == "env":
                try:
                    from azure.identity import DefaultAzureCredential

                    credential = DefaultAzureCredential()
                    credential.get_token("https://management.azure.com/.default")
                    msg = (
                        "✓ Azure environment credentials valid "
                        "(Managed Identity / az login / env vars)."
                    )
                    if sub_id and rg and res_name:
                        from azure.mgmt.monitor import MonitorManagementClient

                        svc_type = (
                            data.get("db_service_type") or "Microsoft.Sql/servers"
                        ).strip()
                        resource_uri = (
                            f"/subscriptions/{sub_id}/resourceGroups/{rg}"
                            f"/providers/{svc_type}/{res_name}"
                        )
                        monitor = MonitorManagementClient(credential, sub_id)
                        defs = list(monitor.metric_definitions.list(resource_uri))
                        msg += f" {len(defs)} metric definition(s) for resource."
                    return (msg, "ok")
                except ImportError as exc:
                    return (f"azure-identity required: {exc}", "error")
                except Exception as exc:
                    return (f"✗ Azure environment credentials failed: {exc}", "auth")

            if auth_mode == "sso":
                return _azure_device_code_auth()

            # Azure CLI public client ID — usable without app registration for ROPC flow
            _AZURE_CLI_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"

            # For pwd mode, fall back to Azure CLI client ID if none provided
            effective_client_id = client_id or (
                _AZURE_CLI_CLIENT_ID if auth_mode == "pwd" else ""
            )

            # Validate required fields up-front with specific messages
            def _azure_missing() -> str | None:
                if not tenant_id:
                    return (
                        "Tenant ID is required (Cloud Resource Identification section)."
                    )
                if not sub_id:
                    return "Subscription ID is required (Cloud Resource Identification section)."
                if auth_mode == "keys":
                    if not effective_client_id:
                        return (
                            "Client ID (App ID) is required (Access Keys / Tokens tab)."
                        )
                    if not client_sec and not bearer:
                        return "Client Secret or Bearer Token is required (Access Keys / Tokens tab)."
                elif auth_mode == "pwd":
                    if not username:
                        return "Username is required (Username / Password tab)."
                    if not password:
                        return "Password is required (Username / Password tab)."
                return None

            missing = _azure_missing()
            if missing:
                return (f"✗ {missing}", "error")

            try:
                from azure.identity import (
                    ClientSecretCredential,
                    UsernamePasswordCredential,
                )
                from azure.mgmt.monitor import MonitorManagementClient

                if auth_mode == "keys" and client_sec:
                    credential = ClientSecretCredential(
                        tenant_id, effective_client_id, client_sec
                    )
                elif auth_mode == "keys" and bearer:
                    from azure.core.credentials import AccessToken
                    import time

                    class _StaticToken:
                        def get_token(self, *a, **kw):
                            return AccessToken(bearer, int(time.time()) + 3600)

                    credential = _StaticToken()
                elif auth_mode == "pwd":
                    try:
                        credential = UsernamePasswordCredential(
                            effective_client_id, username, password, tenant_id=tenant_id
                        )
                        # Eagerly validate so WsTrust / ROPC errors are caught here
                        credential.get_token("https://management.azure.com/.default")
                    except Exception as pwd_exc:
                        pwd_msg = str(pwd_exc)
                        if (
                            "wst:FailedAuthentication" in pwd_msg
                            or "WsTrust" in pwd_msg
                            or "AADSTS7000218" in pwd_msg
                            or "AADSTS50053" in pwd_msg
                        ):
                            return (
                                "✗ Azure Username/Password blocked by tenant.\n\n"
                                "The tenant has disabled the legacy ROPC/WsTrust flow.\n"
                                "Switch to the 'Azure AD Device Code (SSO)' tab instead.",
                                "auth",
                            )
                        if (
                            "AADSTS50076" in pwd_msg
                            or "AADSTS50079" in pwd_msg
                            or "multi-factor" in pwd_msg.lower()
                        ):
                            return (
                                "✗ Azure MFA required — Username/Password cannot satisfy MFA.\n"
                                "Switch to the 'Azure AD Device Code (SSO)' tab instead.",
                                "auth",
                            )
                        return (f"✗ Azure auth failed: {pwd_msg[:200]}", "auth")
                else:
                    return ("✗ Could not determine Azure credential type.", "error")

                monitor = MonitorManagementClient(credential, sub_id)
                # Use the selected namespace; fall back to SQL if somehow empty
                svc_type = (
                    data.get("db_service_type") or "Microsoft.Sql/servers"
                ).strip()
                resource_uri = (
                    f"/subscriptions/{sub_id}/resourceGroups/{rg}"
                    f"/providers/{svc_type}"
                    f"/{res_name}"
                )
                try:
                    defs = list(monitor.metric_definitions.list(resource_uri))
                    return (
                        f"✓ Azure Monitor connected — {len(defs)} metric definitions found for {svc_type}.",
                        "ok",
                    )
                except Exception as metric_exc:
                    metric_msg = str(metric_exc)
                    # Auth was fine; just the resource URI is wrong (bad namespace, name, or RG)
                    if any(
                        k in metric_msg
                        for k in (
                            "InvalidResourceNamespace",
                            "ResourceNotFound",
                            "InvalidResourceType",
                            "does not exist",
                            "invalid namespace",
                            "404",
                        )
                    ):
                        return (
                            f"✓ Azure credentials valid.\n"
                            f"  Resource not found — check Resource Name, Resource Group, and DB Service Type.\n"
                            f"  ({svc_type}/{res_name})",
                            "ok",
                        )
                    raise  # re-raise for the outer except to handle auth errors

            except ImportError:
                pass
            except Exception as exc:
                msg = str(exc)
                if "AADSTS" in msg or "401" in msg or "403" in msg:
                    return (f"✗ Azure auth rejected: {msg[:200]}", "auth")
                return (f"✗ Azure error: {msg[:200]}", "error")

            # Fallback: raw OAuth2 token POST
            try:
                import urllib.request, urllib.error, urllib.parse, ssl, json

                ctx = ssl.create_default_context()
                token = bearer
                if not token:
                    if auth_mode == "keys" and client_sec:
                        grant, extra = "client_credentials", {
                            "client_secret": client_sec
                        }
                    elif auth_mode == "pwd":
                        grant, extra = "password", {
                            "username": username,
                            "password": password,
                        }
                    else:
                        return ("✗ Could not determine Azure credential type.", "error")
                    body_params = {
                        "grant_type": grant,
                        "client_id": effective_client_id,
                        "scope": "https://management.azure.com/.default",
                        **extra,
                    }
                    turl = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
                    req = urllib.request.Request(
                        turl,
                        data=urllib.parse.urlencode(body_params).encode(),
                        method="POST",
                    )
                    try:
                        with urllib.request.urlopen(
                            req, timeout=10, context=ctx
                        ) as resp:
                            token = json.loads(resp.read()).get("access_token")
                    except urllib.error.HTTPError as he:
                        body_txt = he.read(512).decode(errors="replace")
                        if he.code in (400, 401):
                            return (
                                f"✗ Azure token rejected (HTTP {he.code}): {body_txt[:200]}",
                                "auth",
                            )
                        return (
                            f"✗ Azure token request failed (HTTP {he.code}).",
                            "error",
                        )
                if not token:
                    return ("✗ Could not obtain Azure access token.", "error")
                probe = urllib.request.Request(
                    f"https://management.azure.com/subscriptions/{sub_id}?api-version=2022-12-01",
                    headers={"Authorization": f"Bearer {token}"},
                    method="GET",
                )
                with urllib.request.urlopen(probe, timeout=10, context=ctx) as resp:
                    return (
                        f"✓ Azure Management API connected (HTTP {resp.status}).",
                        "ok",
                    )
            except urllib.error.HTTPError as he:
                if he.code in (401, 403):
                    return (f"✗ Azure credentials rejected (HTTP {he.code}).", "auth")
                return (f"Azure responded HTTP {he.code}.", "error")
            except Exception as exc:
                return (f"✗ Azure connection failed: {exc}", "error")

        # ── GCP ───────────────────────────────────────────────────────────────
        elif provider == "GCP":
            project_id = data.get("project_id", "")
            if not project_id and auth_mode != "env":
                return ("✗ GCP Project ID is required.", "error")

            if auth_mode == "env":
                try:
                    import google.auth
                    from googleapiclient import discovery

                    creds, adc_project = google.auth.default(
                        scopes=["https://www.googleapis.com/auth/cloud-platform"]
                    )
                    eff_project = project_id or adc_project or ""
                    if not eff_project:
                        return (
                            "✓ GCP ADC valid but no project ID detected — "
                            "run Auto-detect or enter Project ID.",
                            "ok",
                        )
                    sqladmin = discovery.build(
                        "sqladmin",
                        "v1beta4",
                        credentials=creds,
                        cache_discovery=False,
                    )
                    resource_name = data.get("resource_name", "")
                    if resource_name:
                        sqladmin.instances().get(
                            project=eff_project, instance=resource_name
                        ).execute()
                    else:
                        sqladmin.instances().list(project=eff_project).execute()
                    return (
                        f"✓ GCP environment credentials valid — project "
                        f"'{eff_project}' reachable.",
                        "ok",
                    )
                except ImportError as exc:
                    return (f"google-auth required: {exc}", "error")
                except Exception as exc:
                    return (f"✗ GCP environment credentials failed: {exc}", "auth")

            if auth_mode == "sso":
                # Interactive — fully handled in the helper (browser + code).
                return _gcp_device_auth()

            # Delegate to the provider so test and monitor share one code path.
            try:
                from monitoring.cloud_providers.gcp_provider import build_credentials
            except ImportError as exc:
                return (
                    f"GCP provider unavailable ({exc}).  Install google-auth "
                    "and google-cloud-monitoring.",
                    "error",
                )

            creds, err = build_credentials(data)
            if err:
                # Distinguish "bad input" from "auth rejected".  Library auth
                # errors come back from build_credentials only after a load
                # success, so anything we see here is an input/format issue.
                return (f"✗ {err}", "error")

            try:
                from googleapiclient import discovery

                resource_name = data.get("resource_name", "")
                sqladmin = discovery.build(
                    "sqladmin",
                    "v1beta4",
                    credentials=creds,
                    cache_discovery=False,
                )
                if resource_name:
                    sqladmin.instances().get(
                        project=project_id, instance=resource_name
                    ).execute()
                else:
                    sqladmin.instances().list(project=project_id).execute()
                return (
                    f"✓ GCP Cloud SQL connected — project '{project_id}' reachable.",
                    "ok",
                )

            except ImportError as exc:
                return (
                    f"google-api-python-client is required ({exc}). "
                    "Install: pip install google-api-python-client",
                    "error",
                )
            except Exception as exc:
                msg = str(exc)
                if any(
                    k in msg
                    for k in ("401", "403", "PERMISSION_DENIED", "UNAUTHENTICATED")
                ):
                    return (f"✗ GCP auth rejected: {msg[:200]}", "auth")
                return (f"✗ GCP Cloud SQL Admin API error: {msg[:200]}", "error")

        # ── Other / Custom ────────────────────────────────────────────────────
        else:
            if auth_mode == "sso":
                return _generic_oidc_device_auth()
            api_token = data.get("api_token", "") or data.get("bearer_token", "")
            resource = data.get("resource_name", "")
            if not api_token:
                return ("✗ No API token provided.", "error")
            return (
                f"✓ Credentials present for '{resource}'. "
                "Test not available for custom providers.",
                "ok",
            )

    def _prompt_mfa_code(self, display_name: str, mfa_type: str) -> str | None:
        """
        Show a small dialog to collect the current MFA code from the user.
        Returns the entered code, or None if the user cancelled.
        """
        dlg = tk.Toplevel(self.root)
        dlg.title("MFA / 2FA Required")
        dlg.geometry("380x200")
        dlg.resizable(False, False)
        dlg.transient(self.root)
        dlg.grab_set()

        ttk.Label(
            dlg, text=f"MFA Required — {display_name}", font=("Arial", 11, "bold")
        ).pack(pady=(16, 4), padx=20)
        ttk.Label(
            dlg, text=f"Method: {mfa_type}", foreground="gray", font=("Arial", 9)
        ).pack()
        ttk.Label(
            dlg, text="Enter your current MFA / OTP code:", font=("Arial", 10)
        ).pack(pady=(10, 4), padx=20)

        code_var = tk.StringVar()
        code_entry = ttk.Entry(
            dlg,
            textvariable=code_var,
            width=20,
            font=("Courier", 14),
            justify=tk.CENTER,
        )
        code_entry.pack(padx=20)
        code_entry.focus()

        result: dict[str, str | None] = {"code": None}

        def submit():
            val = code_var.get().strip()
            if not val:
                messagebox.showwarning(
                    "Empty", "Please enter the MFA code.", parent=dlg
                )
                return
            result["code"] = val
            dlg.destroy()

        code_entry.bind("<Return>", lambda _e: submit())
        btn_row = ttk.Frame(dlg)
        btn_row.pack(pady=10)
        ttk.Button(
            btn_row, text="Verify", command=submit, style="Primary.TButton", width=10
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_row, text="Cancel", command=dlg.destroy, width=10).pack(
            side=tk.LEFT, padx=4
        )

        dlg.update_idletasks()
        cx = (dlg.winfo_screenwidth() - dlg.winfo_width()) // 2
        cy = (dlg.winfo_screenheight() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{cx}+{cy}")
        dlg.wait_window()
        return result["code"]

    # ── Cloud monitor instantiation ───────────────────────────────────────────

    def _azure_sso_callback(self, az_cmd: list) -> "subprocess.CompletedProcess":
        """
        Run ``az login`` with a waiting dialog on the main tkinter thread.
        Passed as ``sso_callback`` to the Azure provider's build_monitor so the
        UI interaction stays here while the business logic lives in the provider.
        """
        _login_done = threading.Event()

        def _show_waiting_dialog():
            dlg = tk.Toplevel(self.root)
            dlg.title("Azure Login")
            dlg.geometry("400x150")
            dlg.resizable(False, False)
            dlg.transient(self.root)
            dlg.attributes("-topmost", True)
            dlg.lift()
            ttk.Label(
                dlg,
                text="Waiting for Azure browser login…",
                font=("Arial", 12, "bold"),
            ).pack(pady=(24, 8))
            ttk.Label(
                dlg,
                text="Complete sign-in in the browser window that opened.\n"
                     "This dialog closes automatically when done.",
                foreground="gray",
                justify=tk.CENTER,
            ).pack()

            def _poll():
                if _login_done.is_set():
                    if dlg.winfo_exists():
                        dlg.destroy()
                else:
                    dlg.after(500, _poll)

            _poll()

        self.root.after(0, _show_waiting_dialog)
        try:
            result = subprocess.run(az_cmd, capture_output=True, text=True, timeout=300)
        finally:
            _login_done.set()
        return result

    def _gcp_sso_callback(self) -> tuple[bool, str]:
        """
        Run ``gcloud auth application-default login --no-launch-browser`` and
        drive the interactive device-code flow from a tk dialog.

        The dialog shows the verification URL with an 'Open in Browser' button
        and an entry field for the authorization code that Google returns
        after sign-in.  The code is fed back to gcloud's stdin.

        Returns ``(True, message)`` on success or ``(False, message)`` on
        failure (gcloud missing, user cancelled, code rejected, etc.).

        Designed to be invoked from a background thread (it schedules its
        own dialogs on the main thread via ``self.root.after``).
        """
        import webbrowser
        import re

        try:
            proc = subprocess.Popen(
                ["gcloud", "auth", "application-default", "login",
                 "--no-launch-browser"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            return (
                False,
                "'gcloud' command not found. Install Google Cloud SDK from "
                "https://cloud.google.com/sdk/docs/install and rerun.",
            )
        except Exception as exc:
            return False, f"Failed to launch gcloud: {exc}"

        output_buf: list[str] = []
        url_re = re.compile(r"https?://\S+")

        def _read_until_url(timeout: float = 60.0) -> str | None:
            start = time.time()
            while time.time() - start < timeout:
                line = proc.stdout.readline()
                if not line:
                    if proc.poll() is not None:
                        return None
                    continue
                output_buf.append(line)
                for m in url_re.finditer(line):
                    candidate = m.group(0).rstrip(".,;)>\"'")
                    if "accounts.google.com" in candidate or "auth/" in candidate:
                        return candidate
            return None

        verification_url = _read_until_url()
        if not verification_url:
            try:
                proc.kill()
            except Exception:
                pass
            tail = "".join(output_buf)[-500:]
            return (
                False,
                "Could not parse the verification URL from gcloud output. "
                f"Last output: {tail or '(empty)'}",
            )

        # Open the user's default browser eagerly so they don't have to click.
        try:
            webbrowser.open(verification_url, new=2)
        except Exception:
            pass  # Manual copy/paste still works via the dialog below.

        _dialog_done = threading.Event()
        _dialog_result: dict = {"code": None, "cancelled": False}

        def _show_dialog():
            previous_grab = self.root.grab_current()
            dlg = tk.Toplevel(self.root)
            dlg.title("Google Cloud Sign-in")
            dlg.geometry("620x340")
            dlg.resizable(False, False)
            dlg.transient(self.root)
            dlg.attributes("-topmost", True)
            dlg.lift()
            dlg.grab_set()

            ttk.Label(
                dlg,
                text="Sign in with your Google account",
                font=("Arial", 12, "bold"),
            ).pack(pady=(16, 4))
            ttk.Label(
                dlg,
                text="1. A browser tab should have opened. If not, click "
                     "'Open in Browser' or copy the URL below.\n"
                     "2. Complete sign-in in the browser.\n"
                     "3. Copy the authorization code shown after sign-in.\n"
                     "4. Paste the code below and click 'Submit'.",
                justify=tk.LEFT,
                foreground="gray",
                font=("Arial", 9),
            ).pack(padx=20, pady=(0, 8), anchor=tk.W)

            url_row = ttk.Frame(dlg)
            url_row.pack(fill=tk.X, padx=20, pady=(0, 6))
            ttk.Label(url_row, text="URL:", width=4).pack(side=tk.LEFT)
            url_var = tk.StringVar(value=verification_url)
            url_entry = ttk.Entry(url_row, textvariable=url_var)
            url_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
            url_entry.config(state="readonly")
            ttk.Button(
                url_row, text="Open in Browser", width=18,
                command=lambda: webbrowser.open(verification_url, new=2),
            ).pack(side=tk.LEFT, padx=(6, 0))

            ttk.Label(
                dlg, text="Authorization code:", font=("Arial", 10, "bold"),
            ).pack(padx=20, anchor=tk.W, pady=(10, 2))
            code_var = tk.StringVar()
            code_entry = ttk.Entry(
                dlg, textvariable=code_var, font=("Courier", 11),
            )
            code_entry.pack(padx=20, fill=tk.X)

            def _close_dialog():
                try:
                    dlg.grab_release()
                except Exception:
                    pass
                try:
                    if previous_grab and previous_grab.winfo_exists():
                        previous_grab.grab_set()
                except Exception:
                    pass
                dlg.destroy()

            def _submit():
                code = code_var.get().strip()
                if not code:
                    messagebox.showwarning(
                        "Missing Code",
                        "Paste the authorization code returned by Google "
                        "first.",
                        parent=dlg,
                    )
                    return
                _dialog_result["code"] = code
                _dialog_done.set()
                _close_dialog()

            def _cancel():
                _dialog_result["cancelled"] = True
                _dialog_done.set()
                _close_dialog()

            code_entry.bind("<Return>", lambda _e: _submit())

            btn_row = ttk.Frame(dlg)
            btn_row.pack(pady=14)
            ttk.Button(
                btn_row, text="Submit", command=_submit,
                style="Primary.TButton", width=14,
            ).pack(side=tk.LEFT, padx=4)
            ttk.Button(
                btn_row, text="Cancel", command=_cancel, width=10,
            ).pack(side=tk.LEFT, padx=4)

            dlg.protocol("WM_DELETE_WINDOW", _cancel)
            dlg.update_idletasks()
            cx = (dlg.winfo_screenwidth() - dlg.winfo_width()) // 2
            cy = (dlg.winfo_screenheight() - dlg.winfo_height()) // 2
            dlg.geometry(f"+{cx}+{cy}")
            dlg.after(100, lambda: (dlg.lift(), dlg.focus_force(),
                                    code_entry.focus_force()))

        self.root.after(0, _show_dialog)

        # 10-minute upper bound for the user to authenticate.
        if not _dialog_done.wait(timeout=600):
            try:
                proc.kill()
            except Exception:
                pass
            return False, "Timed out waiting for the authorization code."

        if _dialog_result["cancelled"]:
            try:
                proc.kill()
            except Exception:
                pass
            return False, "Google Cloud sign-in was cancelled."

        code = _dialog_result["code"] or ""
        try:
            assert proc.stdin is not None
            proc.stdin.write(code + "\n")
            proc.stdin.flush()
        except Exception as exc:
            try:
                proc.kill()
            except Exception:
                pass
            return False, f"Failed to send the code to gcloud: {exc}"

        try:
            tail, _ = proc.communicate(timeout=120)
            if tail:
                output_buf.append(tail)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:
                pass
            return False, "gcloud did not finish within 2 minutes after code submission."

        if proc.returncode != 0:
            tail = "".join(output_buf)[-400:]
            return False, f"gcloud sign-in failed (exit {proc.returncode}): {tail}"

        return True, "Application Default Credentials saved by gcloud."

    def _build_cloud_monitor(self, entry: dict):
        """
        Instantiate the appropriate monitor object from a saved cloud entry.
        Returns (monitor_object, None) on success or (None, error_string) on failure.
        Delegates to CloudProviderRegistry; Azure and GCP SSO pass a UI
        callback so the waiting / device-code dialogs are shown on the main
        thread while this runs in a background thread.
        """
        provider = entry.get("provider", "")
        auth_mode = entry.get("auth_mode", "keys")
        if provider == "Azure" and auth_mode == "sso":
            sso_cb = self._azure_sso_callback
        elif provider == "GCP" and auth_mode == "sso":
            sso_cb = self._gcp_sso_callback
        else:
            sso_cb = None
        return CloudProviderRegistry.build_monitor(entry, sso_callback=sso_cb)

    def _fetch_cloud_metrics(
        self, display_name: str, entry: dict, monitor
    ) -> tuple[str, dict]:
        """
        Fetch metrics from a live cloud monitor object.
        Returns (text_block, graph_data) where graph_data is
        {graph_key: float_value} ready to feed into the visualizer.
        Called from the background metrics thread.

        All provider-specific logic is delegated to CloudProviderRegistry;
        adding a new cloud provider requires zero changes here.
        """
        provider   = entry.get("provider", "")
        resource   = entry.get("resource_name", "")
        ts         = time.strftime("%H:%M:%S")
        refresh_s = self.refresh_interval / 1000.0
        sections = []
        graph_data = {}

        try:
            # --- health check (all providers) ---
            if self._should_skip_liveness(
                self._cloud_last_ok_at.get(display_name, 0.0),
                refresh_s,
                self._cloud_health_skip_if_used_within,
            ):
                health_status = "✓ OK"
            else:
                errors = monitor.check_health()
                health_status = "⚠ " + " | ".join(errors) if errors else "✓ OK"
            health_section = [("Health", [("Status", health_status)])]

            # --- provider-specific metric sections via registry ---
            sections, graph_data, alerts = CloudProviderRegistry.fetch_metrics(
                display_name, entry, monitor, self._threshold_checker
            )
            if alerts:
                self._fire_alerts(alerts, origin="cloud")

            all_sections = health_section + sections

            if sections or graph_data:
                self._cloud_last_ok_at[display_name] = time.time()
                self._cloud_consecutive_failures[display_name] = 0
                self._cloud_needs_refresh[display_name] = False
            else:
                self._cloud_last_ok_at.pop(display_name, None)
                fails = self._cloud_consecutive_failures.get(display_name, 0) + 1
                self._cloud_consecutive_failures[display_name] = fails
                self._cloud_needs_refresh[display_name] = True

        except Exception as exc:
            all_sections = [("Health", [("Status", f"⚠ Error: {exc}")])]
            graph_data = {}
            self._cloud_last_ok_at.pop(display_name, None)
            fails = self._cloud_consecutive_failures.get(display_name, 0) + 1
            self._cloud_consecutive_failures[display_name] = fails
            self._cloud_needs_refresh[display_name] = True

        # Determine the provider label (e.g. "AWS / RDS MariaDB")
        svc = entry.get("db_service_type", "") or ""
        db_label = f"{provider} / {svc}" if svc else provider

        text = self._format_metric_block(
            db_name=display_name,
            db_label=db_label,
            timestamp=ts,
            sections=all_sections,
            resource=resource,
        )
        return text, graph_data

    def select_cloud_database(self):
        """
        Open a picker dialog showing all saved cloud connections.
        Supports: Select (add to monitoring), Edit (open the form pre-filled),
        Delete (remove from registry), Cancel.
        Active connections are shown with a ✓ prefix.
        """
        if not self.cloud_databases:
            messagebox.showinfo(
                "No Saved Connections",
                "No cloud connections saved yet.\n"
                "Use 'Add Database' to register a connection first.",
            )
            return

        # ── picker dialog ─────────────────────────────────────────────────────
        picker = tk.Toplevel(self.root)
        picker.title("Cloud Database Connections")
        picker.geometry("460x360")
        picker.resizable(True, True)
        picker.transient(self.root)
        picker.grab_set()

        ttk.Label(
            picker, text="Saved cloud connections:", font=("Arial", 10, "bold")
        ).pack(pady=(14, 2), padx=16, anchor=tk.W)
        ttk.Label(
            picker,
            text="✓ = already in monitoring list",
            foreground="gray",
            font=("Arial", 8),
        ).pack(padx=16, anchor=tk.W, pady=(0, 6))

        list_frame = ttk.Frame(picker)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=(0, 4))

        sb = ttk.Scrollbar(list_frame, orient=tk.VERTICAL)
        lb = tk.Listbox(
            list_frame,
            yscrollcommand=sb.set,
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            selectbackground=self.theme.PRIMARY,
            selectforeground="white",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=self.theme.BORDER,
            font=self.ui_font,
        )
        sb.config(command=lb.yview)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        def _refresh_lb():
            lb.delete(0, tk.END)
            for name in self.cloud_databases:
                entry = self.cloud_databases[name]
                provider = entry.get("provider", "")
                active = "✓ " if name in self.active_cloud_databases else "  "
                lb.insert(tk.END, f"{active}[{provider}] {name}")

        _refresh_lb()

        names = list(self.cloud_databases.keys())
        if names:
            lb.selection_set(0)

        def _selected_name() -> str | None:
            sel = lb.curselection()
            if not sel:
                return None
            # names list may drift if entries deleted; refresh from cloud_databases
            cur_names = list(self.cloud_databases.keys())
            idx = sel[0]
            return cur_names[idx] if idx < len(cur_names) else None

        # ── action: select for monitoring ─────────────────────────────────────
        def on_select():
            name = _selected_name()
            if not name:
                messagebox.showwarning(
                    "No Selection", "Please select a connection.", parent=picker
                )
                return
            if name in self.active_cloud_databases:
                self.update_status(f"'{name}' is already in the monitoring list.", "info")
                picker.destroy()
                return
            picker.destroy()
            entry = self.cloud_databases[name]
            if entry.get("mfa_enabled"):
                mfa_type = entry.get("mfa_type", self._MFA_TYPES[0])
                code = self._prompt_mfa_code(name, mfa_type)
                if code is None:
                    return
                entry["_mfa_code"] = code
            self.active_cloud_databases[name] = entry
            self._on_cloud_db_connected(name)

        # ── action: edit connection ────────────────────────────────────────────
        def on_edit():
            name = _selected_name()
            if not name:
                messagebox.showwarning(
                    "No Selection", "Please select a connection to edit.", parent=picker
                )
                return
            entry = self.cloud_databases[name]
            provider = entry.get("provider", "")
            picker.destroy()
            self._open_cloud_provider_form(provider, edit_name=name)

        # ── action: delete connection ─────────────────────────────────────────
        def on_delete():
            name = _selected_name()
            if not name:
                messagebox.showwarning(
                    "No Selection",
                    "Please select a connection to delete.",
                    parent=picker,
                )
                return
            active_warn = (
                f"\n\nNote: '{name}' is currently in the monitoring list and will also be removed."
                if name in self.active_cloud_databases
                else ""
            )
            if not messagebox.askyesno(
                "Delete Connection",
                f"Delete '{name}' from the saved connections?{active_warn}\n\n"
                "This cannot be undone.",
                parent=picker,
            ):
                return
            self.cloud_databases.pop(name, None)
            self.active_cloud_databases.pop(name, None)
            self.active_cloud_monitors.pop(name, None)
            self._clear_cloud_liveness_state(name)
            self.cloud_connection_manager.save_cloud_databases(self.cloud_databases)
            self.update_cloud_db_listbox()
            self.update_monitor_status_label()
            self.update_status(f"Cloud connection '{name}' deleted.", "info")
            _refresh_lb()
            if self.cloud_databases:
                lb.selection_set(0)

        lb.bind("<Double-Button-1>", lambda _e: on_select())

        btn_r = ttk.Frame(picker)
        btn_r.pack(pady=8)
        ttk.Button(
            btn_r, text="Select", command=on_select, style="Success.TButton", width=10
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_r, text="Edit", command=on_edit, width=10).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(
            btn_r, text="Delete", command=on_delete, style="Warning.TButton", width=10
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_r, text="Cancel", command=picker.destroy, width=10).pack(
            side=tk.LEFT, padx=4
        )

        picker.update_idletasks()
        cx = (picker.winfo_screenwidth() - picker.winfo_width()) // 2
        cy = (picker.winfo_screenheight() - picker.winfo_height()) // 2
        picker.geometry(f"+{cx}+{cy}")

    # ------------------------------------------------------------------
    # Cloud connection keepalive
    # ------------------------------------------------------------------

