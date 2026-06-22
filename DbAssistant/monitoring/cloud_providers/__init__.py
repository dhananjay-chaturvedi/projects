"""
cloud_providers package
=======================
One sub-module per cloud provider.  Each module defines a module-level SPEC
(CloudProviderSpec) and calls CloudProviderRegistry.register(SPEC) on import.

Importing cloud_provider_registry triggers all provider registrations
automatically via _bootstrap() at module load time.
"""
