#-------------------------------------------------------------------------------
#description: AI manager for the tool
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

"""
AI Query Agent for Natural Language Database Queries
Uses installed Claude CLI (no API keys required)
"""

import subprocess
import json
import re
import shutil
from datetime import datetime
from config_loader import config, console_print

class AIQueryAgent:
    """AI-powered natural language to SQL converter using Claude CLI"""

    def __init__(self):
        self.cli_available = False
        self.cli_path = None
        self.conversation_history = []  # Store conversation context
        self.current_sql = None  # Store current generated SQL
        self.current_db_type = None  # Store current database type
        self.last_context_sent = None  # Store last context sent to AI for debugging

        # Load timeouts from config
        self.cli_test_timeout = config.get_int('ai.claude_cli', 'cli_test_timeout', default=5)
        self.default_timeout = config.get_int('ai.claude_cli', 'default_timeout', default=120)
        self.simple_timeout = config.get_int('ai.claude_cli', 'simple_query_timeout', default=120)
        self.complex_timeout = config.get_int('ai.claude_cli', 'complex_query_timeout', default=180)
        self.followup_timeout = config.get_int('ai.claude_cli', 'followup_timeout', default=180)
        self.max_output_tokens = config.get_int('ai.claude_cli', 'max_output_tokens', default=4000)

        # Load cache limits
        self.max_tables_fetch = config.get_int('ai.cache', 'max_tables_fetch', default=50)
        self.max_tables_detailed = config.get_int('ai.cache', 'max_tables_detailed', default=10)
        self.max_tables_display = config.get_int('ai.cache', 'max_tables_display', default=100)

        # Enhanced caching with query modes
        self.schema_cache = {}  # {connection_name: schema_info_dict}
        self.context_cache = {}  # {connection_name: {mode: context_dict}}
        self.cache_metadata = {}  # {connection_name: {'db_type': str, 'timestamp': datetime}}

        self._check_claude_cli()

    def _check_claude_cli(self):
        """Check if Claude CLI is installed and available"""
        console_print("\n=== AI Agent Initialization ===")
        console_print("Checking for Claude CLI installation...")

        # Check if 'claude' command is available
        self.cli_path = shutil.which('claude')

        if self.cli_path:
            try:
                # Test the CLI by getting version
                result = subprocess.run(
                    ['claude', '--version'],
                    capture_output=True,
                    text=True,
                    timeout=self.cli_test_timeout
                )

                if result.returncode == 0:
                    version = result.stdout.strip() or result.stderr.strip()
                    console_print(f"✓ Claude CLI found: {self.cli_path}")
                    console_print(f"✓ Version: {version}")
                    self.cli_available = True
                    console_print("✓ AI Agent: Using Claude CLI (no API key needed)")
                else:
                    console_print(f"✗ Claude CLI found but not working properly")
                    console_print(f"  Error: {result.stderr}")

            except subprocess.TimeoutExpired:
                console_print("✗ Claude CLI timeout - command not responding")
            except Exception as e:
                console_print(f"✗ Error testing Claude CLI: {e}")
        else:
            console_print("✗ Claude CLI not found in PATH")
            console_print("\nTo install Claude CLI:")
            console_print("  Visit: https://claude.ai/download")
            console_print("  Or if you have Claude Code: the CLI should already be available")

        console_print("="*35 + "\n")

    def is_available(self):
        """Check if AI agent is available"""
        return self.cli_available

    def get_api_info(self):
        """Get AI agent information"""
        if self.cli_available:
            return {
                'status': 'Connected',
                'provider': 'Claude CLI',
                'model': 'claude-sonnet-4.5',
                'instructions': 'Using installed Claude CLI'
            }
        else:
            return {
                'status': 'Not Available',
                'provider': 'N/A',
                'model': 'N/A',
                'instructions': 'Install Claude CLI from https://claude.ai/download or use Claude Code'
            }

    def _call_claude_cli(self, prompt, max_tokens=None, timeout=None):
        """
        Call Claude CLI with a prompt

        Returns:
            dict with 'response' (str or None) and 'error' (str or None)
        """
        # Use configured defaults if not provided
        if max_tokens is None:
            max_tokens = self.max_output_tokens
        if timeout is None:
            timeout = self.default_timeout

        try:
            # Log prompt size for debugging
            prompt_size = len(prompt)
            console_print(f"Calling Claude CLI (prompt size: {prompt_size} chars, timeout: {timeout}s)")

            # Use claude CLI with the prompt
            # Format: echo "prompt" | claude
            result = subprocess.run(
                ['claude'],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                return {'response': result.stdout.strip(), 'error': None}
            else:
                error = result.stderr.strip()
                console_print(f"Claude CLI error (exit code {result.returncode}): {error}")
                # Return error details for better user feedback
                return {'response': None, 'error': f"Claude CLI returned error (exit code {result.returncode}): {error}"}

        except subprocess.TimeoutExpired:
            error_msg = f"Claude CLI timeout - query took longer than {timeout}s"
            console_print(error_msg)
            return {'response': None, 'error': error_msg}
        except Exception as e:
            error_msg = f"Error calling Claude CLI: {e}"
            console_print(error_msg)
            return {'response': None, 'error': error_msg}

    def get_schema_info(self, db_manager, limit=None, include_schemas=True, schema_limit=None):
        """
        Get comprehensive database schema information for context

        Args:
            db_manager: DatabaseManager instance
            limit: Max number of table names to retrieve (uses config default if None)
            include_schemas: Whether to fetch detailed table schemas
            schema_limit: Max number of tables to fetch detailed schemas for

        Returns:
            dict with database_type, tables, table_count, and table_schemas
        """
        # Use configured defaults if not provided
        if limit is None:
            limit = self.max_tables_fetch
        if schema_limit is None:
            schema_limit = self.max_tables_detailed

        from database_registry import DatabaseRegistry

        schema_info = {
            'database_type': db_manager.db_type,
            'tables': [],
            'table_count': 0,
            'table_schemas': {}  # Detailed schema for tables
        }

        try:
            # Get table names using registry
            console_print(f"  Fetching table list from {db_manager.db_type}...")
            tables = DatabaseRegistry.execute_operation(
                db_manager.db_type, 'getTables', db_manager.conn
            ) or []

            schema_info['table_count'] = len(tables)
            schema_info['tables'] = tables[:limit]

            console_print(f"  Found {len(tables)} tables in database")

            # Fetch detailed schemas for the first N tables
            if include_schemas and tables and DatabaseRegistry.supports_operation(db_manager.db_type, 'getTableSchema'):
                tables_to_fetch = tables[:schema_limit]
                console_print(f"  Fetching detailed schema for first {len(tables_to_fetch)} tables...")

                for table_name in tables_to_fetch:
                    try:
                        schema = DatabaseRegistry.execute_operation(
                            db_manager.db_type, 'getTableSchema', db_manager.conn, table_name
                        )
                        if schema:
                            schema_info['table_schemas'][table_name] = schema
                    except Exception as e:
                        console_print(f"    Warning: Could not get schema for {table_name}: {e}")
                        # Continue with other tables

                console_print(f"  Successfully retrieved schema for {len(schema_info['table_schemas'])} table(s)")

        except Exception as e:
            console_print(f"Error getting schema info: {e}")
            import traceback
            traceback.print_exc()
            console_print("  Will generate query with limited context")

        return schema_info

    def _analyze_question_complexity(self, question):
        """
        Analyze question to determine what context is needed

        Returns:
            dict with flags for what context to load
        """
        question_lower = question.lower()

        # Keywords that indicate need for different context types (English + Japanese)
        relationship_keywords = [
            # English
            'join', 'relationship', 'related', 'foreign key', 'reference', 'connect', 'link', 'between',
            # Japanese
            '結合', '関連', '外部キー', '参照', 'リレーション', '紐付', '繋'
        ]
        performance_keywords = [
            # English
            'slow', 'performance', 'optimize', 'index', 'explain', 'bottleneck', 'lock', 'block', 'session', 'process', 'running',
            # Japanese
            '遅い', 'パフォーマンス', '最適化', 'インデックス', 'ボトルネック', 'ロック', 'セッション', 'プロセス', '実行中', '速度', '高速化'
        ]
        analysis_keywords = [
            # English
            'analyze', 'structure', 'schema', 'design', 'model', 'tablespace', 'database', 'report', 'summary', 'overview',
            # Japanese
            '分析', '構造', 'スキーマ', '設計', 'モデル', 'テーブルスペース', 'データベース', 'レポート', '要約', '概要', '一覧'
        ]
        system_keywords = [
            # English
            'user', 'role', 'permission', 'access', 'grant', 'privilege',
            # Japanese
            'ユーザー', 'ロール', '権限', 'アクセス', '許可', '特権'
        ]

        # Detect complexity
        needs_relationships = any(keyword in question_lower for keyword in relationship_keywords)
        needs_performance = any(keyword in question_lower for keyword in performance_keywords)
        needs_analysis = any(keyword in question_lower for keyword in analysis_keywords)
        needs_system = any(keyword in question_lower for keyword in system_keywords)

        # Check if question is simple (just basic SELECT/INSERT/UPDATE/DELETE)
        simple_patterns = [
            # English
            'select', 'show', 'get', 'list', 'find', 'display',
            # Japanese
            '表示', '取得', '検索', '一覧', '見せ', '探', 'リスト'
        ]
        is_simple = any(question_lower.startswith(pattern) for pattern in simple_patterns)

        # Word count heuristic - longer questions tend to be more complex
        word_count = len(question.split())
        is_complex = word_count > 10

        return {
            'needs_relationships': needs_relationships or is_complex,
            'needs_performance': needs_performance,
            'needs_analysis': needs_analysis,
            'needs_system': needs_system,
            'is_simple': is_simple and not (needs_relationships or needs_performance or needs_analysis),
            'complexity_score': sum([needs_relationships, needs_performance, needs_analysis, needs_system])
        }

    def get_comprehensive_db_context(self, db_manager, connection_name, question=''):
        """
        Adaptively collect database context based on question complexity

        Args:
            db_manager: DatabaseManager instance
            connection_name: Connection identifier
            question: User's question to analyze for context needs

        Returns:
            dict with database context information (adaptive based on question)
        """
        from database_registry import DatabaseRegistry

        # Analyze what context is needed
        analysis = self._analyze_question_complexity(question)

        console_print(f"[Context Analysis] Complexity: {'Simple' if analysis['is_simple'] else 'Complex'} "
              f"(score: {analysis['complexity_score']})")

        context = {
            'database_type': db_manager.db_type,
            'question_complexity': analysis['complexity_score'],
            'schema': {},
            'system': {},
            'relationships': {},
            'performance': {},
            'metadata': {}
        }

        # 1. Basic Schema (ALWAYS collect - needed for all queries)
        tables = DatabaseRegistry.execute_operation(db_manager.db_type, 'getTables', db_manager.conn) or []
        context['schema']['tables'] = tables[:self.max_tables_display]
        context['schema']['table_count'] = len(tables)

        # 2. Detailed table schemas
        # Simple queries: use max_tables_detailed
        # Complex queries: double the limit for complex queries
        schema_limit = self.max_tables_detailed if analysis['is_simple'] else (self.max_tables_detailed * 2)
        console_print(f"[Context] Loading detailed schemas for {schema_limit} tables")

        table_schemas = {}
        for table_name in tables[:schema_limit]:
            if DatabaseRegistry.supports_operation(db_manager.db_type, 'getTableSchema'):
                try:
                    schema = DatabaseRegistry.execute_operation(
                        db_manager.db_type, 'getTableSchema', db_manager.conn, table_name
                    )
                    if schema:
                        table_schemas[table_name] = schema
                except Exception as e:
                    console_print(f"    Warning: Could not get schema for {table_name}: {e}")

        context['schema']['table_schemas'] = table_schemas

        # 3. Relationships & Constraints (if needed for complex queries)
        if analysis['needs_relationships'] or not analysis['is_simple']:
            console_print(f"[Context] Loading relationships (constraints, indexes)")
            if DatabaseRegistry.supports_operation(db_manager.db_type, 'getConstraints'):
                try:
                    constraints = DatabaseRegistry.execute_operation(
                        db_manager.db_type, 'getConstraints', db_manager.conn
                    )
                    context['relationships']['constraints'] = constraints[:50] if constraints else []
                except Exception as e:
                    console_print(f"    Note: Could not get constraints: {e}")

            if DatabaseRegistry.supports_operation(db_manager.db_type, 'getIndexes'):
                try:
                    indexes = DatabaseRegistry.execute_operation(
                        db_manager.db_type, 'getIndexes', db_manager.conn
                    )
                    context['relationships']['indexes'] = indexes[:50] if indexes else []
                except Exception as e:
                    console_print(f"    Note: Could not get indexes: {e}")

        # 4. Views, Procedures, Functions (if analysis needed)
        if analysis['needs_analysis']:
            console_print(f"[Context] Loading database objects (views, procedures, functions)")
            for obj_type in ['getViews', 'getProcedures', 'getFunctions']:
                if DatabaseRegistry.supports_operation(db_manager.db_type, obj_type):
                    try:
                        objects = DatabaseRegistry.execute_operation(
                            db_manager.db_type, obj_type, db_manager.conn
                        )
                        context['schema'][obj_type.lower()] = objects[:30] if objects else []
                    except Exception as e:
                        console_print(f"    Note: Could not get {obj_type}: {e}")

        # 5. System Information (if system query or analysis)
        if analysis['needs_system'] or analysis['needs_analysis']:
            console_print(f"[Context] Loading system information (users, roles)")
            for op in ['getUsers', 'getRoles']:
                if DatabaseRegistry.supports_operation(db_manager.db_type, op):
                    try:
                        result = DatabaseRegistry.execute_operation(
                            db_manager.db_type, op, db_manager.conn
                        )
                        context['system'][op.lower()] = result[:20] if result else []
                    except Exception as e:
                        console_print(f"    Note: Could not get {op}: {e}")

        # Always get version (lightweight)
        version = DatabaseRegistry.execute_operation(
            db_manager.db_type, 'getVersion', db_manager.conn
        )
        context['metadata']['version'] = version

        # 6. Performance/Process Information (if troubleshooting query)
        if analysis['needs_performance']:
            console_print(f"[Context] Loading performance data (processes, sessions, activity)")
            # MySQL/MariaDB: Process List
            if DatabaseRegistry.supports_operation(db_manager.db_type, 'getProcessList'):
                try:
                    processes = DatabaseRegistry.execute_operation(
                        db_manager.db_type, 'getProcessList', db_manager.conn
                    )
                    context['performance']['processes'] = processes[:20] if processes else []
                except Exception as e:
                    console_print(f"    Note: Could not get process list: {e}")

            # PostgreSQL: Activity
            if DatabaseRegistry.supports_operation(db_manager.db_type, 'getActivity'):
                try:
                    activity = DatabaseRegistry.execute_operation(
                        db_manager.db_type, 'getActivity', db_manager.conn
                    )
                    context['performance']['activity'] = activity[:20] if activity else []
                except Exception as e:
                    console_print(f"    Note: Could not get activity: {e}")

            # Oracle: Sessions
            if DatabaseRegistry.supports_operation(db_manager.db_type, 'getSessions'):
                try:
                    sessions = DatabaseRegistry.execute_operation(
                        db_manager.db_type, 'getSessions', db_manager.conn
                    )
                    context['performance']['sessions'] = sessions[:20] if sessions else []
                except Exception as e:
                    console_print(f"    Note: Could not get sessions: {e}")

        # 7. Tablespaces/Databases (if database-level analysis)
        if analysis['needs_analysis']:
            console_print(f"[Context] Loading storage metadata (tablespaces, databases, schemas)")
            for op in ['getTablespaces', 'getDatabases', 'getSchemas']:
                if DatabaseRegistry.supports_operation(db_manager.db_type, op):
                    try:
                        result = DatabaseRegistry.execute_operation(
                            db_manager.db_type, op, db_manager.conn
                        )
                        context['metadata'][op.lower()] = result[:30] if result else []
                    except Exception as e:
                        console_print(f"    Note: Could not get {op}: {e}")

        console_print(f"[Context] Context collection complete")
        return context

    def get_cached_schema_info(self, db_manager, connection_name, limit=None, schema_limit=None, include_schemas=True, force_refresh=False):
        """
        Get schema info with caching to avoid redundant database queries.

        Args:
            db_manager: Database manager instance
            connection_name: Unique connection identifier
            limit: Max number of tables to fetch (uses config default if None)
            schema_limit: Max tables to get detailed schemas for (uses config default if None)
            include_schemas: Whether to include detailed schema info
            force_refresh: If True, bypass cache and fetch fresh data

        Returns:
            Cached or fresh schema info dict
        """
        # Use configured defaults if not provided
        if limit is None:
            limit = self.max_tables_fetch
        if schema_limit is None:
            schema_limit = self.max_tables_detailed

        cache_key = connection_name

        # Check cache validity
        if not force_refresh and cache_key in self.schema_cache:
            # Validate cached data matches current connection
            metadata = self.cache_metadata.get(cache_key, {})
            if metadata.get('db_type') == db_manager.db_type:
                console_print(f"[Schema Cache] Cache HIT for {connection_name}")
                return self.schema_cache[cache_key]
            else:
                # DB type mismatch - invalidate cache
                console_print(f"[Schema Cache] Cache INVALID for {connection_name} (type mismatch)")
                self.invalidate_cache(connection_name)

        # Cache miss or force refresh - fetch fresh data
        console_print(f"[Schema Cache] Cache MISS for {connection_name} - fetching from database")
        schema_info = self.get_schema_info(db_manager, limit, include_schemas, schema_limit)

        # Store in cache
        self.schema_cache[cache_key] = schema_info
        self.cache_metadata[cache_key] = {
            'db_type': db_manager.db_type,
            'timestamp': datetime.now()
        }
        console_print(f"[Schema Cache] Cached schema for {connection_name} ({len(schema_info.get('tables', []))} tables)")

        return schema_info

    def get_cached_comprehensive_context(self, db_manager, connection_name, question='', force_refresh=False):
        """
        Get comprehensive context with intelligent caching

        Cache strategy:
        - Cache by complexity level (simple/complex)
        - Reuse cached context if current question needs <= cached complexity
        - Fetch fresh if current question needs > cached complexity
        """
        cache_key = connection_name

        # Analyze current question
        analysis = self._analyze_question_complexity(question)
        complexity_needed = analysis['complexity_score']

        if not force_refresh and cache_key in self.context_cache:
            cached_data = self.context_cache[cache_key]
            metadata = self.cache_metadata.get(cache_key, {})

            # Check if cached data is valid
            if metadata.get('db_type') == db_manager.db_type:
                cached_complexity = cached_data.get('question_complexity', 0)

                # Reuse cache if it has enough complexity for current question
                if cached_complexity >= complexity_needed:
                    console_print(f"[Context Cache] Cache HIT for {connection_name} (cached: {cached_complexity}, needed: {complexity_needed})")
                    return cached_data
                else:
                    console_print(f"[Context Cache] Cache insufficient (cached: {cached_complexity}, needed: {complexity_needed}) - fetching more")

        console_print(f"[Context Cache] Cache MISS for {connection_name} - fetching from database")
        context = self.get_comprehensive_db_context(db_manager, connection_name, question)

        # Store in cache
        self.context_cache[cache_key] = context
        self.cache_metadata[cache_key] = {
            'db_type': db_manager.db_type,
            'timestamp': datetime.now()
        }

        return context

    def invalidate_cache(self, connection_name=None):
        """Invalidate all caches"""
        if connection_name:
            if connection_name in self.schema_cache:
                del self.schema_cache[connection_name]
            if connection_name in self.context_cache:
                del self.context_cache[connection_name]
            if connection_name in self.cache_metadata:
                del self.cache_metadata[connection_name]
            console_print(f"[Cache] Invalidated all caches for {connection_name}")
        else:
            self.schema_cache.clear()
            self.context_cache.clear()
            self.cache_metadata.clear()
            console_print(f"[Cache] Cleared all caches")

    def get_cache_info(self):
        """Get information about cached schemas for debugging/UI display"""
        info = []
        for conn_name, metadata in self.cache_metadata.items():
            schema_info = self.schema_cache.get(conn_name, {})
            info.append({
                'connection': conn_name,
                'db_type': metadata.get('db_type'),
                'timestamp': metadata.get('timestamp'),
                'table_count': len(schema_info.get('tables', []))
            })
        return info

    def get_last_schema_sent(self):
        """Get the schema context that was last sent to AI for debugging"""
        if not self.last_context_sent:
            return "No schema has been sent to AI yet. Generate a query first."

        context = self.last_context_sent
        schema = context.get('schema', {})
        table_schemas = schema.get('table_schemas', {})

        if not table_schemas:
            return "No detailed schema information was available."

        output = "SCHEMA INFORMATION SENT TO AI\n"
        output += "=" * 80 + "\n\n"
        output += f"Database Type: {context.get('database_type', 'Unknown')}\n"
        output += f"Total Tables: {schema.get('table_count', 0)}\n"
        output += f"Detailed Schemas Loaded: {len(table_schemas)}\n\n"

        output += "AVAILABLE COLUMNS BY TABLE:\n"
        output += "-" * 80 + "\n\n"

        for table_name, columns in sorted(table_schemas.items()):
            output += f"TABLE: {table_name}\n"
            if columns:
                output += "  Columns: " + ", ".join([col['name'] for col in columns]) + "\n"
            else:
                output += "  (No columns available)\n"
            output += "\n"

        output += "\n" + "=" * 80 + "\n"
        output += "Use these EXACT column names in your queries.\n"
        output += "If AI used different names, they are incorrect.\n"

        return output

    def _build_intelligent_context(self, context, user_question=''):
        """
        Build comprehensive, intelligent database context for AI prompt

        Args:
            context: Comprehensive context dict from get_comprehensive_db_context()
            user_question: User's question for context-aware filtering

        Returns:
            Formatted context string
        """
        db_type = context['database_type']
        complexity = context.get('question_complexity', 0)

        output = "=" * 100 + "\n"
        output += "COMPREHENSIVE DATABASE CONTEXT\n"
        output += "=" * 100 + "\n\n"

        # Section 1: Database Identity
        output += "🔷 DATABASE INFORMATION:\n"
        output += f"  Type: {db_type}\n"
        if context.get('metadata', {}).get('version'):
            output += f"  Version: {context['metadata']['version']}\n"
        output += f"  Context Level: {'Basic' if complexity == 0 else 'Enhanced'}\n"
        output += "\n"

        # Section 2: Schema Overview
        schema = context.get('schema', {})
        output += "📊 SCHEMA OVERVIEW:\n"
        output += f"  Total Tables: {schema.get('table_count', 0)}\n"

        if schema.get('getviews'):
            output += f"  Views: {len(schema['getviews'])}\n"
        if schema.get('getprocedures'):
            output += f"  Procedures: {len(schema['getprocedures'])}\n"
        if schema.get('getfunctions'):
            output += f"  Functions: {len(schema['getfunctions'])}\n"
        output += "\n"

        # Section 3: Detailed Table Schemas
        table_schemas = schema.get('table_schemas', {})
        if table_schemas:
            output += "📋 DETAILED TABLE SCHEMAS - READ CAREFULLY:\n"
            output += f"⚠️  IMPORTANT: These are the EXACT, ACTUAL column names from the database.\n"
            output += f"⚠️  You MUST use these EXACT names - NO variations, NO guessing!\n"
            output += f"(Showing {len(table_schemas)} tables with full column details)\n\n"

            for table_name, columns in sorted(table_schemas.items())[:15]:  # Show top 15
                output += f"┏━━ TABLE: {table_name} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

                if not columns:
                    output += "┃   (No columns or access denied)\n┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    continue

                # Format columns with very clear header
                output += "┃   COLUMN NAME" + " " * 15 + "DATA TYPE" + " " * 15 + "NULL?     DEFAULT\n"
                output += "┃   " + "─" * 70 + "\n"

                # Format columns
                max_name_len = max(len(col['name']) for col in columns) if columns else 20
                max_type_len = max(len(col['type']) for col in columns) if columns else 20

                for col in columns:
                    name = col['name'].ljust(max_name_len)
                    col_type = col['type'].ljust(max_type_len)
                    nullable = "NULL    " if col['nullable'] else "NOT NULL"
                    default = f" | {col['default']}" if col.get('default') else ""
                    output += f"┃   {name}  {col_type}  {nullable}{default}\n"

                output += "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        # Section 4: Relationships & Constraints (CRITICAL for intelligence)
        relationships = context.get('relationships', {})
        if relationships:
            output += "🔗 RELATIONSHIPS & CONSTRAINTS:\n"
            output += "(Use these to understand table relationships and data integrity rules)\n\n"

            constraints = relationships.get('constraints', [])
            if constraints:
                output += f"  Constraints: {', '.join(constraints[:30])}\n"
                if len(constraints) > 30:
                    output += f"  ... and {len(constraints) - 30} more\n"

            indexes = relationships.get('indexes', [])
            if indexes:
                output += f"  Indexes: {', '.join(indexes[:30])}\n"
                if len(indexes) > 30:
                    output += f"  ... and {len(indexes) - 30} more\n"

            output += "\n"

        # Section 5: Available Database Objects
        all_tables = schema.get('tables', [])
        if all_tables:
            tables_shown = set(table_schemas.keys())
            remaining = [t for t in all_tables if t not in tables_shown]

            if remaining:
                output += "📑 OTHER AVAILABLE TABLES:\n"
                output += f"({len(remaining)} tables - detailed schemas not loaded)\n"
                output += "  " + ", ".join(remaining[:40]) + "\n"
                if len(remaining) > 40:
                    output += f"  ... and {len(remaining) - 40} more\n"
                output += "\n"

        # Section 6: System Information (if available)
        system = context.get('system', {})
        if system:
            output += "👥 SYSTEM INFORMATION:\n"

            users = system.get('getusers', [])
            if users:
                output += f"  Database Users: {', '.join(users[:15])}\n"
                if len(users) > 15:
                    output += f"  ... and {len(users) - 15} more\n"

            roles = system.get('getroles', [])
            if roles:
                output += f"  Roles: {', '.join(roles[:15])}\n"
                if len(roles) > 15:
                    output += f"  ... and {len(roles) - 15} more\n"

            output += "\n"

        # Section 7: Performance/Process Information (if available)
        performance = context.get('performance', {})
        if performance:
            output += "⚡ PERFORMANCE & PROCESSES:\n"

            if 'processes' in performance:
                output += f"  Active Processes: {len(performance['processes'])}\n"
            if 'activity' in performance:
                output += f"  Active Connections: {len(performance['activity'])}\n"
            if 'sessions' in performance:
                output += f"  Active Sessions: {len(performance['sessions'])}\n"

            output += "  (Use this for performance analysis and troubleshooting)\n"
            output += "\n"

        # Section 8: Storage/Organizational Metadata (if available)
        metadata = context.get('metadata', {})
        storage_items = [k for k in ['gettablespaces', 'getdatabases', 'getschemas'] if k in metadata and metadata[k]]
        if storage_items:
            output += "💾 STORAGE & ORGANIZATION:\n"

            if 'gettablespaces' in metadata:
                tablespaces = metadata['gettablespaces']
                if tablespaces:
                    output += f"  Tablespaces: {', '.join(tablespaces[:10])}\n"

            if 'getdatabases' in metadata:
                databases = metadata['getdatabases']
                if databases:
                    output += f"  Databases: {', '.join(databases[:10])}\n"

            if 'getschemas' in metadata:
                schemas = metadata['getschemas']
                if schemas:
                    output += f"  Schemas: {', '.join(schemas[:10])}\n"

            output += "\n"

        output += "=" * 100 + "\n"

        return output

    def _validate_sql_against_schema(self, sql, context):
        """
        Validate that SQL only uses columns from the provided schema

        Args:
            sql: The generated SQL query
            context: The comprehensive context with schema info

        Returns:
            List of warning strings for validation issues
        """
        if not sql or not context:
            return []

        warnings = []

        try:
            # Get all column names from schema
            table_schemas = context.get('schema', {}).get('table_schemas', {})
            if not table_schemas:
                return []  # Can't validate without schema

            # Build a set of all valid columns for quick lookup
            valid_columns = {}  # {column_name_lower: [table_names]}
            for table_name, columns in table_schemas.items():
                for col in columns:
                    col_name_lower = col['name'].lower()
                    if col_name_lower not in valid_columns:
                        valid_columns[col_name_lower] = []
                    valid_columns[col_name_lower].append(table_name)

            # Extract potential column references from SQL
            # Simple regex pattern - matches identifiers after SELECT, WHERE, FROM, JOIN, etc.
            sql_upper = sql.upper()

            # Extract words that look like column references (not keywords)
            sql_keywords = {
                'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER',
                'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
                'NULL', 'AS', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
                'UNION', 'INTERSECT', 'EXCEPT', 'DISTINCT', 'ALL', 'ASC', 'DESC',
                'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
                'CAST', 'EXTRACT', 'SUBSTRING', 'COALESCE', 'NULLIF'
            }

            # Extract words from SQL (simple approach)
            import re
            words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql)

            suspicious_columns = []
            for word in words:
                word_upper = word.upper()
                word_lower = word.lower()

                # Skip SQL keywords
                if word_upper in sql_keywords:
                    continue

                # Skip table names
                if word_lower in [t.lower() for t in table_schemas.keys()]:
                    continue

                # Check if this looks like a column name and isn't in our schema
                # Must be at least 2 chars to avoid false positives
                if len(word) >= 2 and word_lower not in valid_columns:
                    # Check if it's not a common SQL function or operator
                    if not word_upper.startswith('PG_') and not word_upper.startswith('SYS'):
                        if word not in suspicious_columns:
                            suspicious_columns.append(word)

            # Generate warnings for suspicious columns
            if suspicious_columns:
                warnings.append(
                    f"The SQL uses column name(s) not found in schema: {', '.join(suspicious_columns[:10])}"
                )
                warnings.append(
                    "Please verify the query with your database. The AI may have used incorrect column names."
                )
                if len(suspicious_columns) > 10:
                    warnings.append(f"... and {len(suspicious_columns) - 10} more suspicious identifiers")

        except Exception as e:
            # Validation errors shouldn't break query generation
            console_print(f"[Validation Warning] Error during SQL validation: {e}")

        return warnings

    def ask_question(self, question, db_manager, connection_name):
        """
        Convert natural language question to SQL with adaptive intelligence

        Args:
            question: Natural language question
            db_manager: DatabaseManager instance
            connection_name: Connection identifier

        Returns:
            dict with 'sql', 'explanation', 'error'
        """
        if not self.cli_available:
            return {
                'sql': None,
                'explanation': None,
                'error': 'Claude CLI not available. Please install Claude CLI or Claude Code.'
            }

        try:
            console_print(f"\n=== Intelligent Database Agent ===")
            console_print(f"Question: {question}")
            console_print(f"Database: {db_manager.db_type}")
            console_print(f"Connection: {connection_name}")

            # Get adaptive database context based on question complexity
            context = self.get_cached_comprehensive_context(
                db_manager, connection_name, question
            )

            # Build intelligent context for prompt
            db_context = self._build_intelligent_context(context, question)

            # Universal intelligent system instructions
            system_instructions = """You are an INTELLIGENT DATABASE AGENT with adaptive capabilities.

LANGUAGE SUPPORT:
- Accept questions in ANY language (English, Japanese, etc.)
- Respond in the SAME language as the user's question
- Provide explanations in the user's language

You understand:
- Table schemas, columns, data types, constraints
- Relationships between tables (foreign keys, constraints, indexes)
- Database performance and optimization
- System metadata (users, roles, processes, sessions)
- Database-specific syntax and best practices

Your capabilities adapt to the question:
- Simple queries: Generate clean, accurate SQL
- Complex queries: Use relationships, optimize with indexes
- Performance questions: Analyze processes, sessions, system state
- Analysis questions: Provide insights on structure, data modeling
- Troubleshooting: Diagnose issues using performance metrics

Always:
1. Generate ACCURATE SQL using ONLY columns from the schema
2. Understand relationships and use proper JOINs
3. Consider performance and suggest optimizations
4. Provide context-aware, helpful explanations
5. Adapt your approach based on what the user is asking"""

            # Build comprehensive prompt with VERY STRICT instructions
            prompt = f"""{system_instructions}

{db_context}

═══════════════════════════════════════════════════════════════════════════════════
⚠️  CRITICAL RULES - ZERO TOLERANCE FOR VIOLATIONS ⚠️
═══════════════════════════════════════════════════════════════════════════════════

YOU MUST FOLLOW THESE RULES EXACTLY:

1. 🚫 NEVER use column names that are NOT in the schema above
   - If you use a column name not listed → YOUR QUERY IS WRONG
   - If you guess a column name → YOUR QUERY IS WRONG
   - If you assume a column exists → YOUR QUERY IS WRONG

2. ✅ ONLY use the EXACT column names shown in the schema
   - Copy column names EXACTLY as shown (case-sensitive)
   - Do NOT rename, do NOT abbreviate, do NOT modify
   - Use the EXACT spelling, EXACT case, EXACT format

3. 📋 BEFORE writing SQL, verify EACH column exists in the schema
   - Check table name exists
   - Check column name exists in that table
   - Check you're using the exact name from the schema

4. ❌ If the user asks for data you cannot provide:
   - State clearly: "The column [name] does not exist in the schema"
   - Do NOT generate SQL with non-existent columns
   - Do NOT try to guess alternative column names

5. 🔍 For {context['database_type']} database:
   - Column names may be case-sensitive - use EXACT case
   - Table names may be case-sensitive - use EXACT case
   - Schema/owner names matter - use full qualified names if shown

═══════════════════════════════════════════════════════════════════════════════════

USER QUESTION: {question}

STEP-BY-STEP PROCESS:
1. Read the question carefully
2. Identify which tables are needed
3. List EXACT column names from schema for those tables
4. Verify EVERY column you use exists in the schema above
5. Write SQL using ONLY verified columns
6. Double-check: Does every column in your SQL exist in the schema?

RESPONSE FORMAT:
SQL:
[Your SQL query using ONLY columns from the schema]

EXPLANATION:
[Explain:
 - Which tables/columns you used (list them)
 - Why you chose this approach
 - Any limitations based on available schema
 - Performance considerations if applicable]

REMEMBER: Using column names not in the schema is COMPLETELY UNACCEPTABLE.
"""

            # Adaptive timeout based on question complexity
            complexity = context.get('question_complexity', 0)
            # Simple: 120s, Complex: 180s
            timeout = self.complex_timeout if complexity >= 2 else self.simple_timeout
            console_print(f"Calling Intelligent Database Agent (timeout: {timeout}s, complexity: {complexity})...")

            result = self._call_claude_cli(prompt, timeout=timeout)

            if not result['response']:
                error_msg = result['error'] or 'Failed to get response from Claude CLI'
                return {'sql': None, 'explanation': None, 'error': error_msg}

            response = result['response']

            # Parse response
            sql_match = re.search(r'SQL:\s*\n(.+?)(?=\n\s*EXPLANATION:|\Z)', response, re.DOTALL | re.IGNORECASE)
            explanation_match = re.search(r'EXPLANATION:\s*\n(.+)', response, re.DOTALL | re.IGNORECASE)

            sql = sql_match.group(1).strip() if sql_match else None
            explanation = explanation_match.group(1).strip() if explanation_match else None

            # Clean SQL
            if sql:
                sql = re.sub(r'^```sql\s*\n', '', sql)
                sql = re.sub(r'\n```$', '', sql)
                sql = sql.strip()

            if not sql:
                sql = response.strip()

            # Validate SQL against schema
            validation_warnings = self._validate_sql_against_schema(sql, context)
            if validation_warnings:
                warning_text = "\n\n⚠️ SCHEMA VALIDATION WARNINGS:\n" + "\n".join(f"  • {w}" for w in validation_warnings)
                warning_text += "\n\n💡 TIP: Use 'Show Schema Sent to AI' in Options menu to see exact column names available."
                explanation = (explanation or '') + warning_text
                console_print(f"[VALIDATION] Found {len(validation_warnings)} potential issues:")
                for w in validation_warnings:
                    console_print(f"  - {w}")

            # Store in conversation
            self.current_sql = sql
            self.current_db_type = db_manager.db_type
            self.last_context_sent = context  # Store for debugging

            console_print(f"✓ Intelligent query generated successfully")
            console_print(f"  Query length: {len(sql) if sql else 0} characters")
            console_print(f"  Complexity: {context.get('question_complexity', 0)}")
            console_print("="*35 + "\n")

            return {
                'sql': sql,
                'explanation': explanation or 'Query generated successfully',
                'error': None
            }

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            console_print(f"Error in intelligent agent: {error_detail}")
            return {
                'sql': None,
                'explanation': None,
                'error': f'Error: {str(e)}'
            }

    def explain_query(self, sql, db_type):
        """Explain what a SQL query does"""
        if not self.cli_available:
            return "Claude CLI not available"

        try:
            prompt = f"""Explain the following {db_type} SQL query in simple terms:

{sql}

Provide a clear, concise explanation of:
1. What data this query retrieves
2. What conditions/filters are applied
3. How the results are organized

Keep the explanation simple and easy to understand."""

            result = self._call_claude_cli(prompt)
            if result['response']:
                return result['response']
            else:
                return f"Could not generate explanation: {result['error']}"

        except Exception as e:
            return f"Error explaining query: {str(e)}"

    def suggest_optimizations(self, sql, db_type):
        """Suggest optimizations for a SQL query"""
        if not self.cli_available:
            return "Claude CLI not available"

        try:
            prompt = f"""Analyze this {db_type} SQL query and suggest optimizations:

{sql}

Provide specific suggestions for:
1. Index usage
2. Query structure improvements
3. Performance considerations
4. {db_type}-specific optimizations

Format as a numbered list of actionable suggestions."""

            result = self._call_claude_cli(prompt)
            if result['response']:
                return result['response']
            else:
                return f"Could not generate suggestions: {result['error']}"

        except Exception as e:
            return f"Error suggesting optimizations: {str(e)}"

    def start_new_conversation(self, initial_question, db_manager, connection_name):
        """
        Start a new conversation with adaptive intelligence

        Args:
            initial_question: The initial natural language question
            db_manager: DatabaseManager instance
            connection_name: Unique connection identifier for caching

        Returns:
            dict with 'sql', 'explanation', 'error'
        """
        # Clear previous conversation
        self.conversation_history = []
        self.current_sql = None
        self.current_db_type = db_manager.db_type if db_manager else None

        # Get result from ask_question (with adaptive caching)
        result = self.ask_question(initial_question, db_manager, connection_name)

        if result['sql'] and not result['error']:
            # Store the successful query
            self.current_sql = result['sql']

            # Add to conversation history
            self.conversation_history.append({
                'role': 'user',
                'content': initial_question
            })
            self.conversation_history.append({
                'role': 'assistant',
                'content': f"Generated SQL:\n{result['sql']}\n\nExplanation: {result['explanation']}"
            })

        return result

    def send_follow_up(self, follow_up_message, db_manager, connection_name):
        """
        Send a follow-up message with adaptive context awareness

        Args:
            follow_up_message: The follow-up question or correction request
            db_manager: DatabaseManager instance
            connection_name: Unique connection identifier for caching

        Returns:
            dict with 'sql', 'explanation', 'error', 'is_clarification'
        """
        if not self.cli_available:
            return {
                'sql': None,
                'explanation': None,
                'error': 'Claude CLI not available',
                'is_clarification': False
            }

        if not self.conversation_history:
            # No active conversation, treat as new question
            return self.start_new_conversation(follow_up_message, db_manager, connection_name)

        try:
            # Get adaptive context (will use cached data)
            context = self.get_cached_comprehensive_context(db_manager, connection_name, follow_up_message)

            # Build intelligent context for prompt
            db_context = self._build_intelligent_context(context, follow_up_message)

            # Build conversation context - limit to last 10 messages to prevent context overflow
            max_history = 10
            recent_history = self.conversation_history[-max_history:] if len(self.conversation_history) > max_history else self.conversation_history

            conversation_text = ""
            if len(self.conversation_history) > max_history:
                conversation_text += f"[Earlier conversation omitted - showing last {max_history} messages]\n"

            for msg in recent_history:
                role = "User" if msg['role'] == 'user' else "Assistant"
                conversation_text += f"\n{role}: {msg['content']}\n"

            # Build prompt with conversation context
            prompt = f"""You are an INTELLIGENT DATABASE AGENT helping refine queries. You have complete database understanding.

LANGUAGE SUPPORT: Respond in the SAME language as the user's message (English, Japanese, etc.)

{db_context}

Previous Conversation:
{conversation_text}

Current SQL Query:
{self.current_sql or 'None'}

User's Follow-up Message: {follow_up_message}

CRITICAL INSTRUCTIONS:
1. Use ONLY tables, columns, and objects shown above - NO guessing
2. Use EXACT column names (case-sensitive for {context['database_type']})
3. Leverage relationships (constraints, foreign keys) for JOINs
4. Consider indexes for performance
5. Be INTELLIGENT - use your understanding of the data model

The user wants to refine or correct the query. Please:
1. Understand if they're pointing out an error, asking for modifications, or asking a clarification question
2. Generate an updated SQL query if needed, or provide clarification
3. Explain what you changed and why, including performance considerations

Format your response as:
SQL:
[updated SQL query here, or "NO CHANGE" if no SQL update needed]

EXPLANATION:
[explanation of changes made, or answer to their question]

Important:
- If the user mentions an error, fix it in the SQL
- If the user asks for changes (like "add a WHERE clause", "sort by date"), modify the SQL accordingly
- If the user just asks for clarification, explain but don't change the SQL (use "NO CHANGE")
- Use {context['database_type']}-specific syntax and best practices
"""

            console_print(f"\n=== AI Follow-up ===")
            console_print(f"Follow-up: {follow_up_message}")
            console_print(f"Context: {len(self.conversation_history)} previous messages (using last {len(recent_history)})")
            console_print(f"Calling Claude CLI...")

            # Use longer timeout for follow-up messages due to conversation context
            result = self._call_claude_cli(prompt, timeout=self.followup_timeout)

            if not result['response']:
                error_msg = result['error'] or 'Failed to get response from Claude CLI'
                return {
                    'sql': None,
                    'explanation': None,
                    'error': error_msg,
                    'is_clarification': False
                }

            response = result['response']

            # Parse response
            sql_match = re.search(r'SQL:\s*\n(.+?)(?=\n\s*EXPLANATION:|\Z)', response, re.DOTALL | re.IGNORECASE)
            explanation_match = re.search(r'EXPLANATION:\s*\n(.+)', response, re.DOTALL | re.IGNORECASE)

            sql = sql_match.group(1).strip() if sql_match else None
            explanation = explanation_match.group(1).strip() if explanation_match else None

            # Check if this is a clarification (no SQL change)
            is_clarification = False
            if sql and ("NO CHANGE" in sql.upper() or "NO CHANGE" in (explanation or "").upper()):
                is_clarification = True
                sql = self.current_sql  # Keep the current SQL

            # Clean up SQL - remove markdown code blocks if present
            if sql:
                sql = re.sub(r'^```sql\s*\n', '', sql)
                sql = re.sub(r'\n```$', '', sql)
                sql = sql.strip()

            # Update conversation history
            self.conversation_history.append({
                'role': 'user',
                'content': follow_up_message
            })
            self.conversation_history.append({
                'role': 'assistant',
                'content': f"{'Clarification' if is_clarification else 'Updated SQL'}:\n{sql}\n\nExplanation: {explanation}"
            })

            # Update current SQL if it changed
            if not is_clarification and sql:
                self.current_sql = sql

            console_print(f"✓ Follow-up processed: {'Clarification' if is_clarification else 'SQL Updated'}")
            console_print("="*30 + "\n")

            return {
                'sql': sql,
                'explanation': explanation or 'Query updated successfully',
                'error': None,
                'is_clarification': is_clarification
            }

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            console_print(f"Error in send_follow_up: {error_detail}")
            return {
                'sql': None,
                'explanation': None,
                'error': f'Error processing follow-up: {str(e)}',
                'is_clarification': False
            }

    def clear_conversation(self):
        """Clear the current conversation history"""
        self.conversation_history = []
        self.current_sql = None
        self.current_db_type = None

    def get_conversation_summary(self):
        """Get a summary of the conversation history"""
        return {
            'message_count': len(self.conversation_history),
            'has_active_conversation': len(self.conversation_history) > 0,
            'current_sql': self.current_sql
        }
