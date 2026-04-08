#-------------------------------------------------------------------------------
#description: AI Query UI manager for the tool
#initial version: 08-APR-2026
#Author: Dhananjay Chaturvedi
#Copyright 2026 Dhananjay Chaturvedi
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#-------------------------------------------------------------------------------

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import sys
import os
import threading
import re
from datetime import datetime

from ai_query_agent import AIQueryAgent
from ui.widgets import make_collapsible_section, create_horizontal_scrollable
from config_loader import get_window_size


class AIQueryUI:
    """AI Query Assistant UI Module - Natural language to SQL query generation"""

    def __init__(self, parent_frame, root, ai_agent, active_connections,
                 update_status_callback, send_to_editor_callback, theme, fonts):
        """
        Initialize AI Query UI

        Args:
            parent_frame: tk.Frame to contain the AI query UI
            root: Main window reference for dialogs and after()
            ai_agent: AIQueryAgent instance
            active_connections: Dict of active database connections
            update_status_callback: Callback function(msg, type) for status updates
            send_to_editor_callback: Callback to send SQL to editor
            theme: ColorTheme class for styling
            fonts: Dict with 'ui' and 'mono' font definitions
        """
        self.parent = parent_frame
        self.root = root
        self.ai_agent = ai_agent
        self.active_connections = active_connections
        self.update_status = update_status_callback
        self.send_to_editor = send_to_editor_callback
        self.theme = theme
        self.fonts = fonts

        # Set fonts as separate attributes for compatibility
        self.ui_font = fonts['ui']
        self.ui_font_mono = fonts['mono']

        # Configuration
        self.sql_review_rules = ""

        # Query execution state tracking
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        # UI widgets (initialized in create_ui)
        self.ai_conn_combo = None
        self.ai_question_text = None
        self.ai_sql_text = None
        self.ai_results_text = None
        self.ai_results_notebook = None
        self.ai_explanation_text = None
        self.ai_optimization_text = None
        self.ai_chat_history = None
        self.ai_followup_text = None
        self.ai_review_text = None
        self.review_sql_btn = None
        self.ai_status_label = None
        self.ai_provider_label = None
        self.ai_model_label = None
        self.execute_query_btn = None
        self.stop_query_btn = None

    # Public API methods
    def refresh_connections(self):
        """Public method called when connections change"""
        self.refresh_ai_connections()

    def invalidate_cache(self, conn_name):
        """Public method to invalidate schema cache"""
        self.ai_agent.invalidate_cache(conn_name)

    def _build_monospace_text_grid(self, parent, wrap_mode=tk.NONE):
        """Text + vertical + horizontal scrollbars for wide, readable tabular output."""
        outer = ttk.Frame(parent)
        outer.pack(fill=tk.BOTH, expand=True)
        text = tk.Text(
            outer,
            wrap=wrap_mode,
            font=self.ui_font_mono,
            undo=False,
            padx=8,
            pady=8,
            relief=tk.FLAT,
        )
        vsb = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=text.yview)
        hsb = ttk.Scrollbar(outer, orient=tk.HORIZONTAL, command=text.xview)
        text.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        text.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        outer.rowconfigure(0, weight=1)
        outer.columnconfigure(0, weight=1)
        return text

    def create_ui(self):
        """Create UI for AI Query Assistant: split workspace / results, adjustable sashes."""
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)

        title_font_bold = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        small_italic = (self.ui_font[0], max(9, int(self.ui_font[1]) - 1), "italic")

        ai_info = self.ai_agent.get_api_info()
        status_outer = make_collapsible_section(main_frame, "AI agent status", title_font_bold, expanded=True)
        status_inner = ttk.Frame(status_outer)
        status_inner.pack(fill=tk.X)

        status_color = "green" if ai_info["status"] == "Connected" else "red"
        ttk.Label(status_inner, text="Status:", font=title_font_bold).grid(row=0, column=0, sticky=tk.W, padx=4, pady=2)
        self.ai_status_label = ttk.Label(
            status_inner, text=ai_info["status"], foreground=status_color, font=self.ui_font
        )
        self.ai_status_label.grid(row=0, column=1, sticky=tk.W, padx=4, pady=2)

        if ai_info["provider"]:
            ttk.Label(status_inner, text="Provider:", font=title_font_bold).grid(
                row=0, column=2, sticky=tk.W, padx=(16, 4), pady=2
            )
            self.ai_provider_label = ttk.Label(status_inner, text=ai_info["provider"], font=self.ui_font)
            self.ai_provider_label.grid(row=0, column=3, sticky=tk.W, padx=4, pady=2)

            ttk.Label(status_inner, text="Model:", font=title_font_bold).grid(
                row=0, column=4, sticky=tk.W, padx=(16, 4), pady=2
            )
            self.ai_model_label = ttk.Label(status_inner, text=ai_info["model"], font=self.ui_font)
            self.ai_model_label.grid(row=0, column=5, sticky=tk.W, padx=4, pady=2)

            ttk.Label(
                status_inner,
                text="Using Claude CLI (No API key required).",
                foreground="gray",
                font=small_italic,
            ).grid(row=1, column=0, columnspan=6, sticky=tk.W, padx=4, pady=2)
        else:
            ttk.Label(
                status_inner,
                text=ai_info["instructions"],
                foreground="orange",
                font=self.ui_font,
                wraplength=720,
            ).grid(row=1, column=0, columnspan=6, sticky=tk.W, padx=4, pady=4)

            install_frame = ttk.Frame(status_inner)
            install_frame.grid(row=2, column=0, columnspan=6, sticky=tk.W, padx=4, pady=6)

            ttk.Label(
                install_frame, text="To enable AI assistant:", foreground="gray", font=title_font_bold
            ).pack(anchor=tk.W)
            ttk.Label(
                install_frame,
                text="1. Install Claude Code or Claude CLI from https://claude.ai/download",
                foreground="blue",
                font=self.ui_font,
            ).pack(anchor=tk.W, padx=16, pady=1)
            ttk.Label(
                install_frame,
                text="2. Put the 'claude' command on your PATH",
                foreground="blue",
                font=self.ui_font,
            ).pack(anchor=tk.W, padx=16, pady=1)
            ttk.Label(install_frame, text="3. Restart this app", foreground="blue", font=self.ui_font).pack(
                anchor=tk.W, padx=16, pady=1
            )

        # Workspace (left) | Results notebook (right)
        hpaned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        hpaned.pack(fill=tk.BOTH, expand=True, pady=(6, 0))

        work_column = ttk.Frame(hpaned)
        hpaned.add(work_column, weight=1)

        results_column = ttk.Frame(hpaned)
        hpaned.add(results_column, weight=2)

        # Left column: question / actions (top) | SQL editor (bottom)
        vpaned = ttk.PanedWindow(work_column, orient=tk.VERTICAL)
        vpaned.pack(fill=tk.BOTH, expand=True)

        # Upper section - direct frame without canvas for proper resizing
        upper_left = ttk.Frame(vpaned, padding=(4, 0, 4, 0))
        vpaned.add(upper_left, weight=1)

        lower_left = ttk.Frame(vpaned, padding=(0, 0, 4, 0))
        vpaned.add(lower_left, weight=2)

        conn_row = ttk.Frame(upper_left)
        conn_row.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(conn_row, text="Connection:").pack(side=tk.LEFT, padx=(0, 6))
        self.ai_conn_combo = ttk.Combobox(conn_row, width=36, state="readonly")
        self.ai_conn_combo.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6))

        question_frame = ttk.LabelFrame(upper_left, text="Question (natural language)", padding=6)
        question_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 6))

        ttk.Label(
            question_frame,
            text="Examples: show all users · count orders by status · products with price > 100",
            foreground="gray",
            font=self.ui_font,
        ).pack(anchor=tk.W, pady=(0, 4))

        # Question text box - initial height but can expand with frame
        self.ai_question_text = scrolledtext.ScrolledText(
            question_frame, wrap=tk.WORD, font=self.ui_font, height=4
        )
        self.ai_question_text.pack(fill=tk.BOTH, expand=True)

        # Action buttons with horizontal scrolling (optimized) - single row layout
        action_wrapper = ttk.Frame(upper_left)
        action_wrapper.pack(fill=tk.X, pady=(0, 6))
        action_frame = create_horizontal_scrollable(action_wrapper)

        ttk.Button(action_frame, text="Generate SQL", command=self.generate_sql_from_question).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        self.execute_query_btn = ttk.Button(action_frame, text="Execute query", command=self.execute_ai_query)
        self.execute_query_btn.pack(side=tk.LEFT, padx=4)

        self.stop_query_btn = ttk.Button(action_frame, text="⏹ Stop Query", command=self.stop_ai_query)
        # Stop button is initially hidden

        self.explain_query_btn = ttk.Button(action_frame, text="Explain query", command=self.explain_ai_query)
        self.explain_query_btn.pack(side=tk.LEFT, padx=4)
        ttk.Button(action_frame, text="Optimize", command=self.optimize_ai_query).pack(side=tk.LEFT, padx=4)
        ttk.Button(action_frame, text="Clear all", command=self.clear_ai_query).pack(side=tk.LEFT, padx=4)

        sql_frame = ttk.LabelFrame(lower_left, text="Generated SQL", padding=6)
        sql_frame.pack(fill=tk.BOTH, expand=True)

        # SQL toolbar with horizontal scrolling (optimized)
        sql_toolbar_wrapper = ttk.Frame(sql_frame)
        sql_toolbar_wrapper.pack(fill=tk.X, pady=(0, 4))
        sql_toolbar = create_horizontal_scrollable(sql_toolbar_wrapper)

        ttk.Button(sql_toolbar, text="Copy SQL", command=self.copy_ai_sql).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(sql_toolbar, text="Edit SQL", command=self.edit_ai_sql).pack(side=tk.LEFT, padx=4)
        ttk.Button(sql_toolbar, text="Send to SQL Editor", command=self.send_to_sql_editor).pack(side=tk.LEFT, padx=4)

        # Review SQL button with dropdown menu
        self.review_sql_btn = ttk.Menubutton(sql_toolbar, text="Review SQL ▾")
        self.review_sql_btn.pack(side=tk.LEFT, padx=4)

        review_menu = tk.Menu(self.review_sql_btn, tearoff=0)
        self.review_sql_btn['menu'] = review_menu
        review_menu.add_command(label="📝 Write Review Rules", command=self.write_review_rules)
        review_menu.add_command(label="📁 Import SQL for Review", command=self.import_sql_for_review)
        review_menu.add_separator()
        review_menu.add_command(label="🔍 Run Review", command=self.run_sql_review)

        # Options menu button for connection management and cache control (moved here for better visibility)
        options_menu_btn = ttk.Menubutton(sql_toolbar, text="⟳ Options")
        options_menu_btn.pack(side=tk.LEFT, padx=4)

        options_menu = tk.Menu(options_menu_btn, tearoff=0)
        options_menu_btn.configure(menu=options_menu)

        options_menu.add_command(label="Refresh Connections", command=self.refresh_ai_connections)
        options_menu.add_command(label="Clear Schema Cache", command=self.clear_ai_schema_cache)
        options_menu.add_separator()
        options_menu.add_command(label="Cache Info", command=self.show_cache_info)
        options_menu.add_command(label="Show Schema Sent to AI", command=self.show_schema_sent_to_ai)

        self.ai_sql_text = scrolledtext.ScrolledText(
            sql_frame, wrap=tk.WORD, font=self.ui_font_mono, height=5
        )
        self.ai_sql_text.pack(fill=tk.BOTH, expand=True)
        self.ai_sql_text.config(state=tk.DISABLED)

        results_wrap = ttk.LabelFrame(results_column, text="Results & AI insights", padding=6)
        results_wrap.pack(fill=tk.BOTH, expand=True)

        self.ai_results_notebook = ttk.Notebook(results_wrap)
        self.ai_results_notebook.pack(fill=tk.BOTH, expand=True)

        results_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(results_tab, text="Query results")
        self.ai_results_text = self._build_monospace_text_grid(results_tab, wrap_mode=tk.NONE)

        explanation_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(explanation_tab, text="Explanation")
        self.ai_explanation_text = scrolledtext.ScrolledText(
            explanation_tab, wrap=tk.WORD, font=self.ui_font, height=12
        )
        self.ai_explanation_text.pack(fill=tk.BOTH, expand=True)

        optimization_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(optimization_tab, text="Optimization")
        self.ai_optimization_text = scrolledtext.ScrolledText(
            optimization_tab, wrap=tk.WORD, font=self.ui_font, height=12
        )
        self.ai_optimization_text.pack(fill=tk.BOTH, expand=True)

        # Chat tab for follow-up conversations
        chat_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(chat_tab, text="Chat")

        # Chat history display
        chat_history_frame = ttk.Frame(chat_tab)
        chat_history_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        ttk.Label(chat_history_frame, text="Conversation History:", font=(self.ui_font[0], self.ui_font[1], "bold")).pack(anchor=tk.W, pady=(0, 5))

        self.ai_chat_history = scrolledtext.ScrolledText(
            chat_history_frame, wrap=tk.WORD, font=self.ui_font, height=15, state=tk.DISABLED
        )
        self.ai_chat_history.pack(fill=tk.BOTH, expand=True)

        # Configure tags for different message types
        self.ai_chat_history.tag_config("user", foreground="#1976D2", font=(self.ui_font[0], self.ui_font[1], "bold"))
        self.ai_chat_history.tag_config("assistant", foreground="#2E7D32", font=(self.ui_font[0], self.ui_font[1], "bold"))
        self.ai_chat_history.tag_config("system", foreground="#F57C00", font=(self.ui_font[0], self.ui_font[1], "italic"))

        # Follow-up message input
        followup_frame = ttk.LabelFrame(chat_tab, text="Send Follow-up Message", padding=6)
        followup_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(
            followup_frame,
            text="Examples: 'Add a WHERE clause for active users' · 'Sort by date descending' · 'The query failed with error X'",
            foreground="gray",
            font=(self.ui_font[0], max(9, int(self.ui_font[1]) - 1))
        ).pack(anchor=tk.W, pady=(0, 4))

        self.ai_followup_text = scrolledtext.ScrolledText(
            followup_frame, wrap=tk.WORD, font=self.ui_font, height=3
        )
        self.ai_followup_text.pack(fill=tk.X, pady=(0, 5))

        # Follow-up buttons with horizontal scrolling (optimized)
        followup_btn_outer = ttk.Frame(followup_frame)
        followup_btn_outer.pack(fill=tk.X)
        followup_btn_frame = create_horizontal_scrollable(followup_btn_outer)

        ttk.Button(followup_btn_frame, text="Send Follow-up", command=self.send_ai_followup).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(followup_btn_frame, text="Clear Chat", command=self.clear_ai_chat).pack(side=tk.LEFT, padx=4)

        self.refresh_ai_connections()

        def _position_ai_sashes(attempt=0):
            try:
                hpaned.update_idletasks()
                vpaned.update_idletasks()
                w = hpaned.winfo_width()
                h = vpaned.winfo_height()
                if (w <= 160 or h <= 80) and attempt < 18:
                    self.root.after(45, lambda: _position_ai_sashes(attempt + 1))
                    return
                if w > 160:
                    hpaned.sashpos(0, max(300, min(int(w * 0.36), int(w * 0.52))))
                if h > 80:
                    vpaned.sashpos(0, max(200, int(h * 0.50)))
            except tk.TclError:
                pass

        self.root.after_idle(_position_ai_sashes)


    def refresh_ai_connections(self):
        """Refresh connection dropdown in AI tab"""
        # Check if AI tab UI has been created
        if self.ai_conn_combo is None:
            return

        connection_names = list(self.active_connections.keys())
        self.ai_conn_combo['values'] = connection_names
        if connection_names and not self.ai_conn_combo.get():
            self.ai_conn_combo.current(0)
        elif not connection_names:
            # No connections available - clear dropdown
            self.ai_conn_combo.set('')

    def clear_ai_schema_cache(self):
        """Clear schema cache for AI Query Assistant"""
        if not self.ai_agent:
            return

        # Check if UI is initialized
        if self.ai_conn_combo is None:
            return

        conn_name = self.ai_conn_combo.get()
        if conn_name:
            self.ai_agent.invalidate_cache(conn_name)
            messagebox.showinfo("Cache Cleared", f"Schema cache cleared for {conn_name}")
            self.update_status(f"Schema cache cleared for {conn_name}")
        else:
            self.ai_agent.invalidate_cache()  # Clear all
            messagebox.showinfo("Cache Cleared", "All schema caches cleared")
            self.update_status("All schema caches cleared")

    def show_cache_info(self):
        """Display cache information dialog"""
        if not hasattr(self, 'ai_agent') or not self.ai_agent:
            messagebox.showinfo("Cache Info", "No cache data available")
            return

        cache_info = self.ai_agent.get_cache_info()

        if not cache_info:
            messagebox.showinfo("Cache Info", "Schema cache is empty\n\nCache will be populated when you generate SQL queries.")
            return

        # Build info message
        msg = "Cached Database Schemas:\n\n"
        for info in cache_info:
            timestamp = info['timestamp'].strftime("%H:%M:%S")
            msg += f"• {info['connection']} ({info['db_type']})\n"
            msg += f"  Tables: {info['table_count']} | Cached at: {timestamp}\n\n"

        msg += "Note: Cache is cleared when connections are disconnected.\n"
        msg += "Use 'Clear Schema Cache' to force refresh."

        messagebox.showinfo("Schema Cache Information", msg)

    def show_schema_sent_to_ai(self):
        """Display the schema that was sent to AI for the last query"""
        if not hasattr(self, 'ai_agent') or not self.ai_agent:
            messagebox.showinfo("Schema Info", "AI agent not available")
            return

        schema_text = self.ai_agent.get_last_schema_sent()

        # Create a dialog window with scrollable text
        dialog = tk.Toplevel(self.root)
        dialog.title("Schema Sent to AI")
        width, height = get_window_size('ai_query')
        dialog.geometry(f"{width}x{height}")

        # Add text widget with scrollbar
        text_frame = ttk.Frame(dialog)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        text_widget = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set, font=("Courier", 10))
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=text_widget.yview)

        text_widget.insert(1.0, schema_text)
        text_widget.config(state=tk.DISABLED)

        # Add close button
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)

        # Copy to clipboard button
        def copy_to_clipboard():
            self.root.clipboard_clear()
            self.root.clipboard_append(schema_text)
            messagebox.showinfo("Copied", "Schema information copied to clipboard")

        ttk.Button(btn_frame, text="Copy to Clipboard", command=copy_to_clipboard).pack(side=tk.RIGHT, padx=(0, 5))

    def extract_sql_from_markdown(self, text):
        """
        Extract SQL code from markdown-formatted text.
        If text has no markdown (no ``` fences), returns as-is.
        If text has markdown, removes fences and wraps non-code text in /* */.
        Also cleans up if ALL lines are wrapped in /* */.
        """
        import re

        if not text or not text.strip():
            return ""

        # Check if ALL lines are wrapped in /* ... */ (AI sometimes generates this)
        lines = text.strip().split('\n')
        all_commented = all(
            line.strip().startswith('/*') and line.strip().endswith('*/')
            for line in lines if line.strip()
        )

        if all_commented:
            # Extract SQL from within comments
            result = []
            for line in lines:
                line = line.strip()
                if line.startswith('/*') and line.endswith('*/'):
                    # Remove /* and */ and keep the SQL
                    sql_part = line[2:-2].strip()
                    if sql_part:
                        result.append(sql_part)
            return '\n'.join(result)

        # Check if text contains any markdown code fences
        has_markdown = '```' in text

        if not has_markdown:
            # No markdown detected - treat entire text as SQL, return as-is
            return text.strip()

        # Process markdown: extract code from fences, comment out other text
        result = []
        in_code_block = False
        lines = text.split('\n')

        for line in lines:
            line_stripped = line.strip()

            # Check if this is a code fence (``` or ```sql or ```SQL)
            if line_stripped.startswith('```'):
                if in_code_block:
                    # End of code block
                    in_code_block = False
                else:
                    # Start of code block
                    in_code_block = True
                continue  # Don't include the fence markers

            if in_code_block:
                # Inside code block - ALL lines are SQL, keep exactly as-is
                result.append(line_stripped)
            else:
                # Outside code block - this is explanatory text, wrap in comment
                if line_stripped:
                    result.append(f"/* {line_stripped} */")
                else:
                    # Keep empty lines
                    if result:
                        result.append('')

        # Join all lines
        final_sql = '\n'.join(result)

        # Clean up any "/* sql */" or "/* */" artifacts
        final_sql = re.sub(r'/\*\s*sql\s*\*/', '', final_sql, flags=re.IGNORECASE)
        final_sql = re.sub(r'/\*\s*\*/', '', final_sql)

        # Clean up multiple empty lines
        final_sql = re.sub(r'\n\n\n+', '\n\n', final_sql)

        return final_sql.strip()

    def generate_sql_from_question(self):
        """Generate SQL from natural language question"""
        if not self.ai_agent.is_available():
            messagebox.showerror("AI Not Available",
                               "AI agent is not configured.\n\nSet one of these environment variables:\n"
                               "- OPENAI_API_KEY\n- ANTHROPIC_API_KEY\n- GOOGLE_API_KEY")
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name:
            messagebox.showwarning("No Connection", "Please select a database connection first!")
            return

        question = self.ai_question_text.get(1.0, tk.END).strip()
        if not question:
            messagebox.showwarning("No Question", "Please enter a question!")
            return

        if conn_name not in self.active_connections:
            messagebox.showerror("Error", "Selected connection not found!")
            return

        db_manager = self.active_connections[conn_name]

        # Show processing message
        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.insert(1.0, "Generating SQL query...\n")
        self.ai_sql_text.config(state=tk.DISABLED)
        self.update_status("Generating SQL...")

        # Run in thread
        thread = threading.Thread(target=self._generate_sql_thread,
                                 args=(question, db_manager, conn_name))
        thread.daemon = True
        thread.start()

    def _generate_sql_thread(self, question, db_manager, connection_name):
        """Thread for SQL generation"""
        try:
            # start_new_conversation returns a dict with 'sql', 'explanation', 'error'
            result = self.ai_agent.start_new_conversation(question, db_manager, connection_name)

            if result['error']:
                self.root.after(0, messagebox.showerror, "Error", result['error'])
                self.root.after(0, self._clear_ai_sql)
                self.root.after(0, self.update_status, "SQL generation failed")
                return

            # Display SQL
            self.root.after(0, self._display_ai_sql, result['sql'], result['explanation'])

            # Add initial message to chat history
            self.root.after(0, self._add_chat_message, "user", question)
            self.root.after(0, self._add_chat_message, "assistant", f"Generated SQL:\n```sql\n{result['sql']}\n```\n\n{result['explanation']}")
            self.root.after(0, self._add_chat_message, "system", "💡 You can now send follow-up messages to refine this query in the Chat tab.")
            self.root.after(0, self.update_status, "SQL query generated successfully")

        except Exception as e:
            import traceback
            error_msg = f"Error generating SQL:\n{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n=== ERROR in SQL generation ===", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            print("="*30, file=sys.stderr)
            self.root.after(0, messagebox.showerror, "SQL Generation Error", error_msg)
            self.root.after(0, self._clear_ai_sql)
            self.root.after(0, self.update_status, "SQL generation failed")

    def _display_ai_sql(self, sql_query, explanation):
        """Display generated SQL"""
        # Clean the SQL from markdown formatting before displaying
        clean_sql = self.extract_sql_from_markdown(sql_query)

        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.insert(1.0, clean_sql)
        self.ai_sql_text.config(state=tk.DISABLED)

        # Clear previous results
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_results_text.insert(1.0, f"SQL query generated. Click 'Execute Query' to run it.\n\n{explanation}")

    def _clear_ai_sql(self):
        """Clear AI SQL text"""
        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.config(state=tk.DISABLED)

    def execute_ai_query(self):
        """Execute the generated SQL query"""
        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showwarning("No Connection", "Please select a database connection!")
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "Generate or enter a SQL query first!")
            return

        # Note: SQL has already been cleaned by extract_sql_from_markdown() when displayed
        # in _display_ai_sql(), so we use it directly here without re-processing

        db_manager = self.active_connections[conn_name]

        # Update execution state
        self.query_running = True
        self.current_db_manager = db_manager
        self.cancellation_requested = False

        # Update UI to show stop button in correct position (before Explain button)
        self.execute_query_btn.pack_forget()
        self.stop_query_btn.pack(side=tk.LEFT, padx=4, before=self.explain_query_btn)

        self.ai_results_text.delete(1.0, tk.END)
        self.ai_results_text.insert(1.0, "Executing query...\n")
        self.update_status("Executing AI-generated query...")

        # Run in thread
        thread = threading.Thread(target=self._execute_ai_query_thread,
                                 args=(sql_query, db_manager))
        thread.daemon = True
        self.current_execution_thread = thread
        thread.start()

    def _execute_ai_query_thread(self, sql_query, db_manager):
        """Thread for executing AI query"""
        try:
            # Check if cancellation was requested before starting
            if self.cancellation_requested:
                self.root.after(0, self._handle_query_cancelled)
                return

            result, error = db_manager.execute_query(sql_query)

            # Check if query was cancelled during execution
            if self.cancellation_requested:
                self.root.after(0, self._handle_query_cancelled)
                return

            if error:
                # Error message already contains detailed info from execute_query
                self.root.after(0, self._display_ai_error, error)
                return

            # Format and display results
            self.root.after(0, self._display_ai_results, result)
            self.root.after(0, self.update_status, "Query executed successfully")

        except Exception as e:
            import traceback
            error_msg = f"Error executing query:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, self._display_ai_error, error_msg)
        finally:
            # Always restore UI state when query completes
            self.root.after(0, self._restore_query_ui_state)

    def stop_ai_query(self):
        """Stop the currently executing query"""
        if not self.query_running:
            return

        # Set cancellation flag
        self.cancellation_requested = True

        # Try to cancel at database level
        if self.current_db_manager:
            try:
                self.current_db_manager.cancel_query()
                self.update_status("Query cancellation requested...")
            except Exception as e:
                print(f"Error cancelling query: {e}", file=sys.stderr)
                # Even if cancellation fails, the flag is set and thread will check it

    def _handle_query_cancelled(self):
        """Handle UI updates when query is cancelled"""
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_results_text.insert(1.0, "⏹ Query execution cancelled by user\n")
        self.update_status("Query cancelled")

    def _restore_query_ui_state(self):
        """Restore UI state after query execution completes"""
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        # Hide stop button, show execute button in correct position (before Explain button)
        self.stop_query_btn.pack_forget()
        self.execute_query_btn.pack(side=tk.LEFT, padx=4, before=self.explain_query_btn)

    def _display_ai_results(self, result):
        """Display query execution results"""
        self.ai_results_text.delete(1.0, tk.END)

        # Check if this is multiple results
        if 'multiple_results' in result and result['multiple_results']:
            # Display results from multiple statements
            self.ai_results_text.insert(tk.END, f"Executed {result['count']} statement(s) in {result['time']:.3f} seconds\n")
            self.ai_results_text.insert(tk.END, "=" * 80 + "\n\n")

            for res in result['results']:
                stmt_num = res.get('statement_num', '?')
                stmt = res.get('statement', '')

                self.ai_results_text.insert(tk.END, f"Statement {stmt_num}: {stmt}\n")
                self.ai_results_text.insert(tk.END, "-" * 80 + "\n")

                if 'message' in res:
                    # DML/DDL result
                    self.ai_results_text.insert(tk.END, f"{res['message']}\n")
                elif 'columns' in res:
                    # SELECT result
                    self.ai_results_text.insert(tk.END, f"Returned {res['rowcount']} row(s)\n\n")

                    # Display column headers
                    headers = res['columns']
                    header_line = " | ".join(f"{h:20}" for h in headers)
                    self.ai_results_text.insert(tk.END, header_line + "\n")
                    self.ai_results_text.insert(tk.END, "-" * len(header_line) + "\n")

                    # Display rows (limit to first 100 per statement for performance)
                    rows = res['rows'][:100]
                    for row in rows:
                        row_values = []
                        for val in row:
                            if isinstance(val, (bytearray, bytes)):
                                val = val.decode('utf-8', errors='ignore')
                            elif val is None:
                                val = "NULL"
                            else:
                                val = str(val)
                            row_values.append(f"{val:20}")
                        self.ai_results_text.insert(tk.END, " | ".join(row_values) + "\n")

                    if len(res['rows']) > 100:
                        self.ai_results_text.insert(tk.END, f"\n... and {len(res['rows']) - 100} more rows\n")

                self.ai_results_text.insert(tk.END, "\n" + "=" * 80 + "\n\n")

        elif 'message' in result:
            # Single DML/DDL result
            self.ai_results_text.insert(tk.END, f"{result['message']}\n")
            self.ai_results_text.insert(tk.END, f"Execution time: {result['time']:.3f} seconds\n")
        else:
            # Single SELECT result
            self.ai_results_text.insert(tk.END, f"Query returned {result['rowcount']} row(s)\n")
            self.ai_results_text.insert(tk.END, f"Execution time: {result['time']:.3f} seconds\n\n")

            # Display column headers
            headers = result['columns']
            header_line = " | ".join(f"{h:20}" for h in headers)
            self.ai_results_text.insert(tk.END, header_line + "\n")
            self.ai_results_text.insert(tk.END, "-" * len(header_line) + "\n")

            # Display rows (limit to first 1000 for performance)
            rows = result['rows'][:1000]
            for row in rows:
                row_values = []
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        val = val.decode('utf-8', errors='ignore')
                    elif val is None:
                        val = "NULL"
                    else:
                        val = str(val)
                    row_values.append(f"{val:20}")
                self.ai_results_text.insert(tk.END, " | ".join(row_values) + "\n")

            if len(result['rows']) > 1000:
                self.ai_results_text.insert(tk.END, f"\n... and {len(result['rows']) - 1000} more rows\n")

        # Switch to results tab
        self.ai_results_notebook.select(0)

    def _display_ai_error(self, error):
        """Display error in AI results"""
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_results_text.insert(1.0, f"Error executing query:\n\n{error}")
        self.update_status("Query execution failed")

        # Add error to chat history with a helpful suggestion
        conversation_info = self.ai_agent.get_conversation_summary()
        if conversation_info['has_active_conversation']:
            self._add_chat_message("system",
                                 f"❌ Query failed with error:\n{error[:200]}{'...' if len(error) > 200 else ''}\n\n"
                                 f"💡 You can send a follow-up message in the Chat tab to fix this error.\n"
                                 f"Example: \"The query failed with error: {error[:80]}...\"")
            # Switch to chat tab to make it visible
            self.ai_results_notebook.select(3)  # Index 3 is the Chat tab

    def explain_ai_query(self):
        """Get AI explanation of the generated query"""
        if not self.ai_agent.is_available():
            messagebox.showwarning("AI Not Available", "AI agent is not configured")
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showwarning("No Connection", "Please select a database connection!")
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "Generate a SQL query first!")
            return

        # Note: SQL has already been cleaned when displayed, use it directly

        db_manager = self.active_connections[conn_name]

        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_explanation_text.insert(1.0, "Getting explanation from AI...\n")
        self.ai_results_notebook.select(1)  # Switch to explanation tab

        # Run in thread
        thread = threading.Thread(target=self._explain_query_thread,
                                 args=(sql_query, db_manager.db_type))
        thread.daemon = True
        thread.start()

    def _explain_query_thread(self, sql_query, db_type):
        """Thread for query explanation"""
        try:
            # explain_query returns a string (not a tuple)
            explanation = self.ai_agent.explain_query(sql_query, db_type)

            if not explanation or explanation.startswith("Error") or explanation.startswith("Claude CLI not available"):
                self.root.after(0, self._display_explanation_error, explanation)
                return

            self.root.after(0, self._display_explanation, explanation)

        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n=== ERROR in explain query ===", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            print("="*30, file=sys.stderr)
            self.root.after(0, self._display_explanation_error, error_msg)

    def _display_explanation(self, explanation):
        """Display query explanation"""
        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_explanation_text.insert(1.0, explanation)

    def _display_explanation_error(self, error):
        """Display explanation error"""
        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_explanation_text.insert(1.0, f"Error getting explanation:\n\n{error}")

    def optimize_ai_query(self):
        """Get AI optimization suggestions"""
        if not self.ai_agent.is_available():
            messagebox.showwarning("AI Not Available", "AI agent is not configured")
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showwarning("No Connection", "Please select a database connection!")
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "Generate a SQL query first!")
            return

        # Note: SQL has already been cleaned when displayed, use it directly

        db_manager = self.active_connections[conn_name]

        self.ai_optimization_text.delete(1.0, tk.END)
        self.ai_optimization_text.insert(1.0, "Getting optimization suggestions from AI...\n")
        self.ai_results_notebook.select(2)  # Switch to optimization tab

        # Run in thread
        thread = threading.Thread(target=self._optimize_query_thread,
                                 args=(sql_query, db_manager.db_type))
        thread.daemon = True
        thread.start()

    def _optimize_query_thread(self, sql_query, db_type):
        """Thread for query optimization"""
        try:
            # suggest_optimizations returns a string (not a tuple)
            suggestions = self.ai_agent.suggest_optimizations(sql_query, db_type)

            if not suggestions or suggestions.startswith("Error") or suggestions.startswith("Claude CLI not available"):
                self.root.after(0, self._display_optimization_error, suggestions)
                return

            self.root.after(0, self._display_optimization, suggestions)

        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n=== ERROR in suggest optimizations ===", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            print("="*30, file=sys.stderr)
            self.root.after(0, self._display_optimization_error, error_msg)

    def _display_optimization(self, suggestions):
        """Display optimization suggestions"""
        self.ai_optimization_text.delete(1.0, tk.END)
        self.ai_optimization_text.insert(1.0, suggestions)

    def _display_optimization_error(self, error):
        """Display optimization error"""
        self.ai_optimization_text.delete(1.0, tk.END)
        self.ai_optimization_text.insert(1.0, f"Error getting optimization suggestions:\n\n{error}")

    def copy_ai_sql(self):
        """Copy generated SQL to clipboard"""
        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if sql_query:
            self.root.clipboard_clear()
            self.root.clipboard_append(sql_query)
            self.update_status("SQL copied to clipboard")
            messagebox.showinfo("Copied", "SQL query copied to clipboard!")
        else:
            messagebox.showwarning("No Query", "No SQL query to copy!")

    def edit_ai_sql(self):
        """Enable editing of generated SQL"""
        self.ai_sql_text.config(state=tk.NORMAL)
        messagebox.showinfo("Edit Mode", "SQL query is now editable. You can modify it before execution.")

    def send_to_sql_editor(self):
        """Send generated SQL to SQL Editor tab"""
        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "No SQL query to send!")
            return

        # Use the callback provided during initialization
        self.send_to_editor(sql_query)
        messagebox.showinfo("Success", "SQL query sent to SQL Editor!")

    def write_review_rules(self):
        """Open dialog to write/edit SQL review rules"""
        dialog = tk.Toplevel(self.root)
        dialog.title("SQL Review Rules")
        width, height = get_window_size('settings')
        dialog.geometry(f"{width}x{height}")
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title
        ttk.Label(main_frame, text="SQL Review Rules", font=("Arial", 14, "bold")).pack(pady=(0, 10))

        # Instructions
        instructions = ttk.Label(
            main_frame,
            text="Define rules that the AI will use to review SQL queries.\n"
                 "Examples: Check for missing indexes, identify N+1 queries, ensure proper error handling, etc.",
            foreground="gray"
        )
        instructions.pack(pady=(0, 10))

        # Rules text area
        rules_frame = ttk.LabelFrame(main_frame, text="Review Rules (one rule per line)", padding=5)
        rules_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        rules_text = scrolledtext.ScrolledText(rules_frame, wrap=tk.WORD, font=self.ui_font, height=15)
        rules_text.pack(fill=tk.BOTH, expand=True)

        # Load existing rules if they exist
        if hasattr(self, 'sql_review_rules'):
            rules_text.insert(1.0, self.sql_review_rules)
        else:
            # Default rules
            default_rules = """Check for missing WHERE clauses in UPDATE/DELETE statements
Identify queries without proper indexing hints
Look for N+1 query patterns
Verify proper use of JOINs vs subqueries
Check for SQL injection vulnerabilities
Ensure proper transaction handling
Identify inefficient LIKE patterns (e.g., %value%)
Check for missing LIMIT clauses in large result sets"""
            rules_text.insert(1.0, default_rules)

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        def save_rules():
            self.sql_review_rules = rules_text.get(1.0, tk.END).strip()
            messagebox.showinfo("Success", "Review rules saved successfully!")
            dialog.destroy()

        def cancel():
            dialog.destroy()

        ttk.Button(button_frame, text="Save Rules", command=save_rules, style="Primary.TButton").pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(button_frame, text="Cancel", command=cancel).pack(side=tk.RIGHT)

    def import_sql_for_review(self):
        """Import SQL from file for review"""
        file_path = filedialog.askopenfilename(
            title="Import SQL for Review",
            filetypes=[
                ("SQL Files", "*.sql"),
                ("Text Files", "*.txt"),
                ("All Files", "*.*")
            ]
        )

        if not file_path:
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read().strip()

            if not sql_content:
                messagebox.showwarning("Empty File", "The selected file is empty!")
                return

            # Display SQL in Generated SQL box
            self.ai_sql_text.config(state=tk.NORMAL)
            self.ai_sql_text.delete(1.0, tk.END)
            self.ai_sql_text.insert(1.0, sql_content)
            self.ai_sql_text.config(state=tk.NORMAL)  # Keep editable for review

            # Clear results
            self.ai_results_text.delete(1.0, tk.END)
            self.ai_results_text.insert(1.0, "SQL imported for review. Click 'Review SQL' to analyze.\n\n")

            # Add to explanation tab
            self.ai_explanation_text.delete(1.0, tk.END)
            self.ai_explanation_text.insert(1.0, f"Imported SQL from: {file_path}\n\n")
            self.ai_explanation_text.insert(1.0, "Ready for review. You can:\n")
            self.ai_explanation_text.insert(1.0, "1. Execute the query to test it\n")
            self.ai_explanation_text.insert(1.0, "2. Get AI explanation\n")
            self.ai_explanation_text.insert(1.0, "3. Request optimization suggestions\n")
            self.ai_explanation_text.insert(1.0, "4. Run it through review rules\n\n")

            self.update_status(f"✓ SQL imported from {os.path.basename(file_path)}", "success")
            messagebox.showinfo("Success", f"SQL imported successfully from:\n{os.path.basename(file_path)}")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to import SQL file:\n{str(e)}")
            self.update_status("Failed to import SQL", "error")

    def run_sql_review(self):
        """Run AI-powered review on the SQL using custom rules"""
        if not self.ai_agent.is_available():
            messagebox.showwarning("AI Not Available", "AI agent is not configured")
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "No SQL query to review!\n\nPlease generate or import a SQL query first.")
            return

        # Get review rules
        if not hasattr(self, 'sql_review_rules') or not self.sql_review_rules:
            response = messagebox.askyesno(
                "No Review Rules",
                "No custom review rules defined. Would you like to set them up now?\n\n"
                "Click 'No' to use default review criteria."
            )
            if response:
                self.write_review_rules()
                return
            rules = "Use standard SQL best practices and performance optimization guidelines"
        else:
            rules = self.sql_review_rules

        # Get database type for context
        conn_name = self.ai_conn_combo.get()
        db_type = "SQL"
        if conn_name and conn_name in self.active_connections:
            db_manager = self.active_connections[conn_name]
            db_type = db_manager.db_type

        # Create review tab if it doesn't exist
        if not hasattr(self, 'ai_review_text'):
            review_tab = ttk.Frame(self.ai_results_notebook)
            self.ai_results_notebook.add(review_tab, text="Review")
            self.ai_review_text = scrolledtext.ScrolledText(
                review_tab, wrap=tk.WORD, font=self.ui_font
            )
            self.ai_review_text.pack(fill=tk.BOTH, expand=True)

        # Show loading message
        self.ai_review_text.delete(1.0, tk.END)
        self.ai_review_text.insert(1.0, "🔍 Running SQL review with AI...\n\n")
        self.ai_review_text.insert(tk.END, f"Database Type: {db_type}\n")
        self.ai_review_text.insert(tk.END, f"Review Rules: {len(rules.split(chr(10)))} rules defined\n\n")
        self.ai_review_text.insert(tk.END, "Please wait...\n")

        # Switch to review tab
        for idx in range(self.ai_results_notebook.index("end")):
            if self.ai_results_notebook.tab(idx, "text") == "Review":
                self.ai_results_notebook.select(idx)
                break

        self.update_status("Running SQL review with AI...")

        def review_thread():
            try:
                # Build review prompt
                prompt = f"""You are an expert SQL reviewer. Review the following {db_type} SQL query based on these criteria:

REVIEW CRITERIA:
{rules}

SQL QUERY TO REVIEW:
{sql_query}

Please provide a comprehensive review with the following sections:
1. ✅ STRENGTHS: What's done well in this query
2. ⚠️ ISSUES: Problems, vulnerabilities, or bad practices found
3. 💡 RECOMMENDATIONS: Specific improvements with examples
4. 🎯 PRIORITY: Rank issues by severity (Critical/High/Medium/Low)
5. ✨ OPTIMIZED VERSION: Provide an improved version if significant changes are needed

Format your response clearly with the sections above."""

                # Call AI agent
                result = self.ai_agent._call_claude_cli(prompt, timeout=60)

                if result['response']:
                    review_text = result['response']

                    # Update UI in main thread
                    def update_ui():
                        self.ai_review_text.delete(1.0, tk.END)
                        self.ai_review_text.insert(1.0, "🔍 SQL REVIEW RESULTS\n")
                        self.ai_review_text.insert(tk.END, "=" * 80 + "\n\n")
                        self.ai_review_text.insert(tk.END, review_text)
                        self.ai_review_text.insert(tk.END, "\n\n" + "=" * 80 + "\n")
                        self.ai_review_text.insert(tk.END, f"\nReview completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        self.update_status("✓ SQL review completed", "success")

                    self.root.after(0, update_ui)
                else:
                    error_msg = result['error'] or "Failed to get review from AI"

                    def show_error():
                        self.ai_review_text.delete(1.0, tk.END)
                        self.ai_review_text.insert(1.0, f"❌ Review failed:\n\n{error_msg}")
                        self.update_status("SQL review failed", "error")
                        messagebox.showerror("Review Failed", error_msg)

                    self.root.after(0, show_error)

            except Exception as e:
                error_msg = f"Error during review: {str(e)}"

                def show_error():
                    self.ai_review_text.delete(1.0, tk.END)
                    self.ai_review_text.insert(1.0, f"❌ Review error:\n\n{error_msg}")
                    self.update_status("SQL review error", "error")
                    messagebox.showerror("Error", error_msg)

                self.root.after(0, show_error)

        # Run review in background thread
        thread = threading.Thread(target=review_thread, daemon=True)
        thread.start()

    def clear_ai_query(self):
        """Clear all AI query fields"""
        self.ai_question_text.delete(1.0, tk.END)
        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.config(state=tk.DISABLED)
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_optimization_text.delete(1.0, tk.END)
        self.clear_ai_chat()
        self.update_status("AI query fields cleared")

    def _add_chat_message(self, role, message):
        """Add a message to the chat history display"""
        self.ai_chat_history.config(state=tk.NORMAL)

        # Add timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")

        if role == "user":
            self.ai_chat_history.insert(tk.END, f"[{timestamp}] You: ", "user")
            self.ai_chat_history.insert(tk.END, f"{message}\n\n")
        elif role == "assistant":
            self.ai_chat_history.insert(tk.END, f"[{timestamp}] AI Assistant: ", "assistant")
            self.ai_chat_history.insert(tk.END, f"{message}\n\n")
        elif role == "system":
            self.ai_chat_history.insert(tk.END, f"[{timestamp}] ", "system")
            self.ai_chat_history.insert(tk.END, f"{message}\n\n", "system")

        # Auto-scroll to bottom
        self.ai_chat_history.see(tk.END)
        self.ai_chat_history.config(state=tk.DISABLED)

    def send_ai_followup(self):
        """Send a follow-up message to the AI"""
        if not self.ai_agent.is_available():
            messagebox.showerror("AI Not Available", "AI agent is not configured.")
            return

        # Check if there's an active conversation
        conversation_info = self.ai_agent.get_conversation_summary()
        if not conversation_info['has_active_conversation']:
            messagebox.showwarning("No Active Conversation",
                                 "Please generate an initial SQL query first using 'Generate SQL' button.")
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showerror("Error", "Please select a valid database connection!")
            return

        followup_message = self.ai_followup_text.get(1.0, tk.END).strip()
        if not followup_message:
            messagebox.showwarning("Empty Message", "Please enter a follow-up message!")
            return

        db_manager = self.active_connections[conn_name]

        # Add user message to chat
        self._add_chat_message("user", followup_message)

        # Clear input
        self.ai_followup_text.delete(1.0, tk.END)

        # Show processing message
        self._add_chat_message("system", "Processing your request...")
        self.update_status("Processing follow-up...")

        # Run in thread
        thread = threading.Thread(target=self._send_followup_thread,
                                 args=(followup_message, db_manager, conn_name))
        thread.daemon = True
        thread.start()

    def _send_followup_thread(self, followup_message, db_manager, connection_name):
        """Thread for processing follow-up messages"""
        try:
            result = self.ai_agent.send_follow_up(followup_message, db_manager, connection_name)

            if result['error']:
                self.root.after(0, self._add_chat_message, "system",
                              f"❌ Error: {result['error']}")
                self.root.after(0, self.update_status, "Follow-up failed")
                return

            # Build response message
            if result['is_clarification']:
                response = f"{result['explanation']}"
                self.root.after(0, self._add_chat_message, "assistant", response)
            else:
                # SQL was updated
                response = f"Updated SQL:\n```sql\n{result['sql']}\n```\n\n{result['explanation']}"
                self.root.after(0, self._add_chat_message, "assistant", response)

                # Update the SQL display
                self.root.after(0, self._display_ai_sql, result['sql'], result['explanation'])

            self.root.after(0, self.update_status, "Follow-up processed successfully")

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"Error in _send_followup_thread: {error_detail}", file=sys.stderr)
            self.root.after(0, self._add_chat_message, "system",
                          f"❌ Unexpected error: {str(e)}")
            self.root.after(0, self.update_status, "Follow-up failed")

    def clear_ai_chat(self):
        """Clear the chat history"""
        self.ai_chat_history.config(state=tk.NORMAL)
        self.ai_chat_history.delete(1.0, tk.END)
        self.ai_chat_history.config(state=tk.DISABLED)
        self.ai_followup_text.delete(1.0, tk.END)
        self.ai_agent.clear_conversation()
        self.update_status("Chat history cleared")

    def configure_ai_api_key(self):
        """Show dialog to configure AI API key"""
        # Create configuration dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Configure AI API Key")
        width, height = get_window_size('ai_chat')
        dialog.geometry(f"{width}x{height}")
        dialog.resizable(True, True)

        # Make dialog modal
        dialog.transient(self.root)
        dialog.grab_set()

        # Main frame with scrollbar
        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title
        ttk.Label(main_frame, text="Configure AI Provider", font=("Arial", 14, "bold")).pack(pady=(0, 10))

        # Instructions
        instructions = ttk.Label(main_frame,
                               text="Enter your API key for one of the supported AI providers.\n"
                                    "⚠️ Key is stored in MEMORY ONLY (not saved to disk).\n"
                                    "You will need to re-enter it if you restart the application.",
                               justify=tk.LEFT, foreground="orange", wraplength=600)
        instructions.pack(anchor=tk.W, pady=(0, 15))

        # Provider selection
        provider_frame = ttk.LabelFrame(main_frame, text="Select Provider", padding="10")
        provider_frame.pack(fill=tk.X, pady=(0, 10))

        provider_var = tk.StringVar(value="anthropic")

        ttk.Radiobutton(provider_frame, text="Anthropic Claude (Recommended for Claude Code users)",
                       variable=provider_var, value="anthropic").pack(anchor=tk.W, pady=2)
        ttk.Radiobutton(provider_frame, text="OpenAI (GPT-4, GPT-3.5)",
                       variable=provider_var, value="openai").pack(anchor=tk.W, pady=2)
        ttk.Radiobutton(provider_frame, text="Google Gemini",
                       variable=provider_var, value="google").pack(anchor=tk.W, pady=2)

        # API Key input
        key_frame = ttk.LabelFrame(main_frame, text="API Key", padding="10")
        key_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(key_frame, text="API Key:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        api_key_entry = ttk.Entry(key_frame, width=50, show="*")
        api_key_entry.grid(row=0, column=1, padx=5, pady=5)

        show_key_var = tk.BooleanVar(value=False)

        def toggle_key_visibility():
            api_key_entry.config(show="" if show_key_var.get() else "*")

        ttk.Checkbutton(key_frame, text="Show key", variable=show_key_var,
                       command=toggle_key_visibility).grid(row=0, column=2, padx=5, pady=5)

        # Model (optional)
        ttk.Label(key_frame, text="Model (optional):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        model_entry = ttk.Entry(key_frame, width=50)
        model_entry.grid(row=1, column=1, padx=5, pady=5)

        # Help text
        help_text = ttk.Label(key_frame,
                             text="Leave model empty to use defaults:\n"
                                  "• Anthropic: claude-3-5-sonnet-20241022\n"
                                  "• OpenAI: gpt-4\n"
                                  "• Google: gemini-pro",
                             justify=tk.LEFT, foreground="gray", font=("Arial", 9))
        help_text.grid(row=2, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)

        # Info section (move before buttons)
        info_frame = ttk.LabelFrame(main_frame, text="Getting API Keys", padding="10")
        info_frame.pack(fill=tk.X, pady=(5, 10))

        info_text = ("• Anthropic: https://console.anthropic.com/\n"
                    "• OpenAI: https://platform.openai.com/api-keys\n"
                    "• Google: https://makersuite.google.com/app/apikey")

        ttk.Label(info_frame, text=info_text, justify=tk.LEFT, foreground="blue").pack(anchor=tk.W)

        # Status label
        status_label = ttk.Label(main_frame, text="", foreground="red", wraplength=600)
        status_label.pack(pady=5)

        # Buttons (at the bottom, always visible)
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(side=tk.BOTTOM, pady=15)

        def save_config():
            provider = provider_var.get()
            api_key = api_key_entry.get().strip()
            model = model_entry.get().strip() or None

            if not api_key:
                status_label.config(text="⚠️ Please enter an API key", foreground="red")
                return

            # Show processing
            status_label.config(text="Configuring...", foreground="blue")
            dialog.update()

            # Try to configure
            success, message = self.ai_agent.configure_api_key(provider, api_key, model)

            if success:
                # Update UI
                ai_info = self.ai_agent.get_api_info()
                self.ai_status_label.config(text=ai_info['status'], foreground="green")

                if hasattr(self, 'ai_provider_label'):
                    self.ai_provider_label.config(text=ai_info['provider'])
                else:
                    # Labels don't exist, need to recreate them
                    pass

                if hasattr(self, 'ai_model_label'):
                    self.ai_model_label.config(text=ai_info['model'])

                messagebox.showinfo("Success", f"✅ {message}\n\nKey is stored in memory only.\nIt will be cleared when you close the application.", parent=dialog)
                dialog.destroy()
                self.update_status("AI agent configured successfully")
            else:
                status_label.config(text=f"❌ {message}", foreground="red")

        ttk.Button(button_frame, text="✓ Save & Connect", command=save_config, width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy, width=15).pack(side=tk.LEFT, padx=5)

        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

