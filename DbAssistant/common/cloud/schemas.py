"""Cloud provider form schemas — shared by Connections tab and Monitoring."""

from __future__ import annotations

MFA_TYPES = [
    "TOTP (Authenticator App)",
    "SMS / Voice Call",
    "Hardware Token (RSA / YubiKey)",
    "Email OTP",
    "Push Notification (Duo / Okta)",
]

# SQL-login fields shown in the Connections-tab cloud DB form, after the
# provider's resource fields. Shared by the Tk panel, the headless cloud
# service, and the Web/Textual cloud sections so all UIs stay identical.
CLOUD_DB_SQL_FIELDS = (
    ("Database name", "sql_database", "", "Optional default schema/database after connect."),
    ("DB username *", "sql_username", "", "Database login user (required)."),
    ("DB password *", "sql_password", "*", "Database login password (required)."),
    ("DB type *", "sql_db_type", "", "Engine type for SQL connect.",
     ["", "MySQL", "MariaDB", "PostgreSQL", "Oracle", "SQLServer"]),
    ("SQL host *", "sql_host", "", "RDS endpoint hostname (use Resolve for AWS)."),
    ("SQL port *", "sql_port", "", "Database port for the selected engine."),
)

CLOUD_PROVIDER_SCHEMAS = {
    "AWS": {
            "label": "Amazon Web Services",
            "api": "Amazon CloudWatch / RDS API",
            "mfa_common": True,
            "mfa_hint": "TOTP (Authenticator App)",
            "resource": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this entry in the list, e.g. 'prod-mysql-tokyo'.",
                ),
                (
                    "AWS Region *",
                    "region",
                    "",
                    "AWS region code where the RDS instance lives, e.g. ap-northeast-1 (Tokyo), us-east-1 (N. Virginia). Find it in the RDS console URL or instance details.",
                ),
                (
                    "DB Instance Identifier *",
                    "resource_name",
                    "",
                    "The RDS instance identifier — the name shown in the RDS console under 'DB identifier', e.g. 'myapp-prod-db'. Not the endpoint hostname.",
                ),
                (
                    "DB Engine",
                    "db_engine",
                    "",
                    "Optional. The database engine, e.g. mysql, postgres, aurora-mysql. Used to label metrics — leave blank if unsure.",
                ),
            ],
            "resource_vm": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this VM in the monitoring list, e.g. 'prod-web-01'.",
                ),
                (
                    "AWS Region *",
                    "region",
                    "",
                    "AWS region where the EC2 instance runs, e.g. ap-northeast-1, us-east-1.",
                ),
                (
                    "EC2 Instance ID *",
                    "resource_name",
                    "",
                    "EC2 instance ID from the EC2 console, e.g. i-0abc123def4567890.",
                ),
                (
                    "Host / IP (optional)",
                    "host",
                    "",
                    "Public or private IP or DNS hostname — optional if metrics are fetched by instance ID only.",
                ),
            ],
            "resource_cloud_service": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this resource in the monitoring list.",
                ),
                (
                    "AWS Region *",
                    "region",
                    "",
                    "AWS region where the resource is deployed.",
                ),
                (
                    "CloudWatch Namespace *",
                    "cloud_service_type",
                    "",
                    "CloudWatch namespace for the service, e.g. AWS/ApplicationELB, AWS/ElastiCache, AWS/ECS.",
                ),
                (
                    "Resource Identifier *",
                    "resource_name",
                    "",
                    "CloudWatch dimension value — load balancer name, cache cluster ID, ARN suffix, etc.",
                ),
            ],
            "keys_auth": [
                (
                    "Access Key ID *",
                    "access_key_id",
                    "",
                    "20-character alphanumeric key starting with AKIA (long-term) or ASIA (short-term/STS). Found in IAM → Users → your user → Security credentials → Access keys.",
                ),
                (
                    "Secret Access Key *",
                    "secret_access_key",
                    "*",
                    "40-character secret paired with the Access Key ID. Only shown once at creation time. If lost, create a new key pair in IAM.",
                ),
                (
                    "Session Token",
                    "session_token",
                    "*",
                    "Temporary token issued by STS (AssumeRole, SSO, or MFA). Required when using ASIA* keys. Leave blank for long-term AKIA* key pairs.",
                ),
            ],
            "pwd_auth": [
                (
                    "IAM Username *",
                    "username",
                    "",
                    "Your IAM console username (not email). NOTE: AWS API calls cannot use username+password — use the Access Keys tab instead. This is stored for reference only.",
                ),
                (
                    "Password *",
                    "password",
                    "*",
                    "Your IAM console password. NOTE: AWS APIs do not accept this — use Access Keys or IAM Identity Center (SSO tab) for programmatic access.",
                ),
            ],
            "env_auth": {
                "tab_label": "Environment / Instance Role",
                "help": (
                    "Use credentials from the environment — EC2 instance profile / IAM role, "
                    "ECS task role, environment variables, or ~/.aws/credentials. "
                    "No access keys required. Click 'Auto-detect & List Resources' to "
                    "populate region, profile, and resource dropdowns."
                ),
                "fields": [
                    (
                        "AWS Profile (optional)",
                        "sso_profile",
                        "",
                        "Named profile from ~/.aws/config. Leave blank to use the default "
                        "credential chain (instance role on EC2, env vars, or default profile).",
                    ),
                ],
            },
            "sso_auth": {
                "tab_label": "IAM Identity Center / aws login",
                "fields": [
                    (
                        "AWS Profile",
                        "sso_profile",
                        "",
                        "Named profile from ~/.aws/config to log in with (leave blank for 'default'). Use this if you authenticate with the AWS CLI `aws login` command — leave the Start URL below empty and just run Test Connection to trigger `aws login`.",
                    ),
                    (
                        "Start URL",
                        "sso_start_url",
                        "",
                        "ONLY for IAM Identity Center (SSO). Your AWS access portal URL, e.g. https://mycompany.awsapps.com/start. Leave blank if you use the `aws login` command instead of Identity Center.",
                    ),
                    (
                        "Account ID",
                        "sso_account_id",
                        "",
                        "Identity Center only: 12-digit AWS account number, e.g. 123456789012. Leave blank when using `aws login`.",
                    ),
                    (
                        "Role Name",
                        "sso_role_name",
                        "",
                        "Identity Center only: the permission set / role name assigned to you, e.g. AWSReadOnlyAccess. Leave blank when using `aws login`.",
                    ),
                    (
                        "SSO Region",
                        "sso_region",
                        "",
                        "Identity Center only: AWS region hosting your Identity Center instance (must match 'sso_region' in ~/.aws/config). Leave blank when using `aws login`.",
                    ),
                ],
            },
        },
        "Azure": {
            "label": "Microsoft Azure",
            "api": "Azure Monitor / Azure Database API",
            "mfa_common": True,
            "mfa_hint": "Push Notification (Duo / Okta)",
            "resource": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this entry, e.g. 'prod-postgres-eastus'.",
                ),
                (
                    "Tenant ID *",
                    "tenant_id",
                    "",
                    "Azure Active Directory tenant (directory) ID — a UUID like xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx. Found in Azure Portal → Azure Active Directory → Overview → Tenant ID.",
                ),
                (
                    "Subscription ID *",
                    "subscription_id",
                    "",
                    "UUID of the Azure subscription containing the database. Found in Azure Portal → Subscriptions.",
                ),
                (
                    "Resource Group *",
                    "resource_group",
                    "",
                    "Name of the resource group containing the database server, e.g. 'rg-prod-eastus'. Found in the database's Overview page in the portal.",
                ),
                (
                    "Resource Name *",
                    "resource_name",
                    "",
                    "Name of the Azure database server resource, e.g. 'myapp-postgres-server'. This is the server name shown in Azure Portal, not the full hostname.",
                ),
                (
                    "Database Name",
                    "database_name",
                    "",
                    "Required for Microsoft.Sql/servers — the specific database name within the server (e.g. 'myapp-db'). Azure SQL Monitor metrics exist at the database level, not the server level. Leave blank for other service types.",
                ),
                (
                    "DB Service Type",
                    "db_service_type",
                    "",
                    "Azure resource provider namespace for the DB type. Select the one matching your database. Used to build the Monitor API resource URI.",
                    [
                        "Microsoft.Sql/servers",
                        "Microsoft.DBforPostgreSQL/servers",
                        "Microsoft.DBforPostgreSQL/flexibleServers",
                        "Microsoft.DBforMySQL/servers",
                        "Microsoft.DBforMySQL/flexibleServers",
                        "Microsoft.DBforMariaDB/servers",
                        "Microsoft.DocumentDB/databaseAccounts",
                        "Microsoft.Cache/Redis",
                    ],
                ),
            ],
            "resource_vm": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this VM in the monitoring list.",
                ),
                (
                    "Tenant ID *",
                    "tenant_id",
                    "",
                    "Azure AD tenant ID (UUID). Azure Portal → Microsoft Entra ID → Overview → Tenant ID.",
                ),
                (
                    "Subscription ID *",
                    "subscription_id",
                    "",
                    "Azure subscription UUID containing the VM.",
                ),
                (
                    "Resource Group *",
                    "resource_group",
                    "",
                    "Resource group containing the virtual machine.",
                ),
                (
                    "VM Name *",
                    "resource_name",
                    "",
                    "Virtual machine name as shown in the Azure Portal (not the full hostname).",
                ),
                (
                    "Host / IP (optional)",
                    "host",
                    "",
                    "Public or private IP — optional if metrics are resolved via Azure Resource Manager.",
                ),
            ],
            "resource_cloud_service": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this resource in the monitoring list.",
                ),
                (
                    "Tenant ID *",
                    "tenant_id",
                    "",
                    "Azure AD tenant ID (UUID).",
                ),
                (
                    "Subscription ID *",
                    "subscription_id",
                    "",
                    "Azure subscription UUID containing the resource.",
                ),
                (
                    "Resource Group *",
                    "resource_group",
                    "",
                    "Resource group containing the monitored service.",
                ),
                (
                    "Azure Resource Type *",
                    "db_service_type",
                    "",
                    "Azure resource provider type used to build the Monitor API URI.",
                    [
                        "Microsoft.Network/loadBalancers",
                        "Microsoft.Cache/Redis",
                        "Microsoft.Web/sites",
                        "Microsoft.ContainerService/managedClusters",
                        "Microsoft.Sql/servers",
                        "Microsoft.DBforPostgreSQL/flexibleServers",
                        "Microsoft.DBforMySQL/flexibleServers",
                        "Microsoft.DocumentDB/databaseAccounts",
                    ],
                ),
                (
                    "Resource Name *",
                    "resource_name",
                    "",
                    "Name of the Azure resource as shown in the portal.",
                ),
            ],
            "keys_auth": [
                (
                    "Client ID (App ID) *",
                    "client_id",
                    "",
                    "Application (client) ID of the Azure AD app registration used as a service principal. Found in Azure Portal → App registrations → your app → Overview.",
                ),
                (
                    "Client Secret *",
                    "client_secret",
                    "*",
                    "A client secret value created under the app registration's Certificates & secrets. Note: this is the VALUE, not the secret ID.",
                ),
                (
                    "Bearer Token (optional)",
                    "bearer_token",
                    "*",
                    "A pre-obtained OAuth2 Bearer token (starts with 'eyJ'). If provided, skips the client_credentials token exchange. Useful for short-lived manual testing.",
                ),
            ],
            "pwd_auth": [
                (
                    "Username *",
                    "username",
                    "",
                    "Azure AD user principal name (UPN), e.g. user@yourcompany.onmicrosoft.com. Used with ROPC (Resource Owner Password Credentials) flow. NOTE: if your tenant enforces MFA via Conditional Access, this will fail with AADSTS50076 — use the 'Azure AD Device Code' tab instead.",
                ),
                (
                    "Password *",
                    "password",
                    "*",
                    "Password for the Azure AD user account above.",
                ),
                (
                    "Client ID (optional)",
                    "client_id",
                    "",
                    "Optional. Azure AD app registration client ID. If left blank, the Azure CLI public client (04b07795-…) is used automatically — no app registration needed.",
                ),
            ],
            "env_auth": {
                "tab_label": "Environment / Instance Role",
                "help": (
                    "Use credentials from the environment — Azure Managed Identity on a VM, "
                    "environment variables (AZURE_CLIENT_ID, etc.), or an existing `az login` "
                    "session. No client secret required. Click 'Auto-detect & List Resources' "
                    "to populate subscription, resource group, region, and resource dropdowns."
                ),
                "fields": [],
            },
            "sso_auth": {
                "tab_label": "Azure AD Device Code",
                "fields": [
                    (
                        "Client ID (optional)",
                        "sso_client_id",
                        "",
                        "Leave BLANK — no app registration needed.\n\n"
                        "Authentication runs via 'az login' (Azure CLI). The browser opens "
                        "automatically and supports MFA and Conditional Access.\n\n"
                        "Only fill this in if your organisation requires a specific app registration "
                        "client ID. Otherwise the Microsoft Azure CLI public client is used automatically.",
                    ),
                    (
                        "Redirect URI (optional)",
                        "sso_redirect_uri",
                        "",
                        "Leave BLANK — not required for Azure CLI-based login.\n\n"
                        "Only relevant if using a custom app registration with a specific redirect URI.",
                    ),
                ],
            },
        },
        "GCP": {
            "label": "Google Cloud Platform",
            "api": "Cloud Monitoring API (Cloud SQL)",
            "mfa_common": True,
            "mfa_hint": "TOTP (Authenticator App)",
            "resource": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this entry, e.g. 'prod-cloudsql-tokyo'.",
                ),
                (
                    "Project ID *",
                    "project_id",
                    "",
                    "GCP project ID (lowercase, with hyphens), e.g. my-company-prod-123. Found in GCP Console top bar or under IAM & Admin → Settings. NOT the project number.",
                ),
                (
                    "Instance Name *",
                    "resource_name",
                    "",
                    "Cloud SQL instance name as shown in the Cloud SQL console, e.g. myapp-mysql-instance. Not the connection name or IP.",
                ),
                (
                    "Database ID",
                    "database_id",
                    "",
                    "Optional. Specific database name inside the Cloud SQL instance, e.g. myapp_db. Leave blank to monitor at the instance level.",
                ),
                (
                    "Location / Region",
                    "region",
                    "",
                    "GCP region where the Cloud SQL instance is deployed, e.g. asia-northeast1 (Tokyo), us-central1. Found in the Cloud SQL instance details page.",
                ),
            ],
            "resource_vm": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this VM in the monitoring list.",
                ),
                (
                    "Project ID *",
                    "project_id",
                    "",
                    "GCP project ID containing the Compute Engine instance.",
                ),
                (
                    "Zone / Region *",
                    "region",
                    "",
                    "GCP zone or region, e.g. asia-northeast1-a or us-central1.",
                ),
                (
                    "Instance Name *",
                    "resource_name",
                    "",
                    "Compute Engine instance name from the GCP console (not the external IP).",
                ),
                (
                    "Host / IP (optional)",
                    "host",
                    "",
                    "External or internal IP — optional if metrics are fetched via instance name.",
                ),
            ],
            "resource_cloud_service": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name you choose to identify this resource in the monitoring list.",
                ),
                (
                    "Project ID *",
                    "project_id",
                    "",
                    "GCP project ID containing the monitored resource.",
                ),
                (
                    "Resource Type",
                    "cloud_service_type",
                    "",
                    "Optional. GCP monitored resource type label, e.g. cloudsql_database, https_lb_rule, redis_instance.",
                ),
                (
                    "Resource ID *",
                    "resource_name",
                    "",
                    "Monitored resource ID as used by Cloud Monitoring (instance name, URL map, etc.).",
                ),
                (
                    "Location / Region",
                    "region",
                    "",
                    "Optional. GCP region or zone for the resource.",
                ),
            ],
            "keys_auth": [
                (
                    "Google Auth JSON File (path)",
                    "sa_key_path",
                    "file",
                    "Absolute path to either a service-account key JSON or a gcloud ADC authorized_user JSON. Click 'Browse…' to pick the file. Required roles: roles/monitoring.viewer and roles/cloudsql.viewer.",
                ),
                (
                    "…or paste Google Auth JSON",
                    "sa_key_json",
                    "multi",
                    "Alternative to the file path: paste a service-account JSON or authorized_user JSON here. Stored encrypted at rest. Takes precedence over the file path if both are filled.",
                ),
                (
                    "…or OAuth Client ID",
                    "oauth_client_id",
                    "",
                    "For authorized_user credentials entered directly. This is the client_id from the authorized_user JSON.",
                ),
                (
                    "…and OAuth Client Secret",
                    "oauth_client_secret",
                    "*",
                    "For authorized_user credentials entered directly. This is the client_secret from the authorized_user JSON.",
                ),
                (
                    "…and OAuth Refresh Token",
                    "oauth_refresh_token",
                    "*",
                    "For authorized_user credentials entered directly. This is the refresh_token from the authorized_user JSON.",
                ),
                (
                    "…or OAuth2 Access Token",
                    "oauth_token",
                    "*",
                    "Alternative to a key: a short-lived OAuth2 access token (starts with 'ya29.'). Obtain via `gcloud auth print-access-token`. Expires in ~1 hour. Only used if both key fields are blank.",
                ),
            ],
            "pwd_auth": [
                (
                    "Service Account Email",
                    "username",
                    "",
                    "Service-account email such as monitor-sa@my-project.iam.gserviceaccount.com. Informational — the actual account is read from the key file. (GCP APIs do not accept a Google user account password directly.)",
                ),
                (
                    "Google Auth File Path or JSON *",
                    "password",
                    "*",
                    "Either an absolute path to a service-account/authorized_user JSON file, or the full JSON content. Stored encrypted at rest.",
                ),
            ],
            "env_auth": {
                "tab_label": "Environment / Instance Role",
                "help": (
                    "Use Application Default Credentials — GCE/GKE metadata service account, "
                    "GOOGLE_APPLICATION_CREDENTIALS, or a prior `gcloud auth application-default "
                    "login`. No key file required. Click 'Auto-detect & List Resources' to "
                    "populate project, region, and instance dropdowns."
                ),
                "fields": [],
            },
            "sso_auth": {
                "tab_label": "Workforce Identity / gcloud",
                "fields": [
                    (
                        "Workforce Pool Provider",
                        "sso_provider",
                        "",
                        "Optional. Full workforce pool provider resource name if using Workforce Identity Federation, e.g. locations/global/workforcePools/my-pool/providers/my-provider. Leave blank to use gcloud ADC device flow instead.",
                    ),
                    (
                        "Client ID (optional)",
                        "sso_client_id",
                        "",
                        "Optional. OAuth2 client ID if triggering Google's device code endpoint directly without gcloud. Leave blank to use gcloud CLI (recommended).",
                    ),
                ],
            },
        },
        "Other": {
            "label": "Other / Custom",
            "api": "Custom monitoring API",
            "mfa_common": False,
            "mfa_hint": "TOTP (Authenticator App)",
            "resource": [
                (
                    "Display Name *",
                    "display_name",
                    "",
                    "A short name to identify this entry in the list.",
                ),
                (
                    "Provider / Service",
                    "provider_name",
                    "",
                    "Name of the cloud or monitoring service, e.g. Alibaba Cloud, Oracle Cloud, Datadog.",
                ),
                (
                    "Resource Name *",
                    "resource_name",
                    "",
                    "Name or ID of the specific database resource to monitor on this platform.",
                ),
                (
                    "Region",
                    "region",
                    "",
                    "Region or availability zone where the resource is hosted, if applicable.",
                ),
            ],
            "keys_auth": [
                (
                    "API Key / Token *",
                    "api_token",
                    "*",
                    "API key or bearer token used to authenticate with the monitoring API. Check the provider's documentation for where to generate this.",
                ),
            ],
            "pwd_auth": [
                (
                    "Username *",
                    "username",
                    "",
                    "Username or account ID for the monitoring API.",
                ),
                ("Password *", "password", "*", "Password for the account above."),
            ],
            "env_auth": {
                "tab_label": "Environment / Instance Role",
                "help": (
                    "Use ambient credentials from environment variables or the host "
                    "platform when supported. Click 'Auto-detect & List Resources' if "
                    "the provider exposes a discovery API."
                ),
                "fields": [],
            },
            "sso_auth": {
                "tab_label": "OIDC / SSO",
                "fields": [
                    (
                        "OIDC Endpoint *",
                        "sso_endpoint",
                        "",
                        "Base URL of your OIDC provider, e.g. https://auth.yourcompany.com. The device_authorization endpoint is appended automatically.",
                    ),
                    (
                        "Client ID *",
                        "sso_client_id",
                        "",
                        "OAuth2 client ID registered on the OIDC provider for this application.",
                    ),
                    (
                        "Client Secret",
                        "sso_client_sec",
                        "*",
                        "Optional client secret if the OIDC provider requires it for public-client device flow.",
                    ),
                ],
            },
        },
    }

MONITOR_TARGET_KINDS = {
    "cloud_db": "Cloud database (RDS, Azure SQL, Cloud SQL, …)",
    "vm": "Virtual machine / host (metrics via SSH or cloud APIs)",
    "cloud_service": "Other cloud service (load balancer, cache, custom API, …)",
}


def resource_fields_for(provider: str, target_kind: str = "cloud_db") -> list:
    """Return identification fields for a provider and monitoring target kind."""
    schema = CLOUD_PROVIDER_SCHEMAS.get(provider, {})
    if target_kind == "vm":
        return schema.get("resource_vm") or schema.get("resource", [])
    if target_kind == "cloud_service":
        return schema.get("resource_cloud_service") or schema.get("resource", [])
    return schema.get("resource", [])


RESOURCE_SECTION_TITLES = {
    "cloud_db": "Cloud Database Identification",
    "vm": "Virtual Machine Identification",
    "cloud_service": "Cloud Resource Identification",
}

TARGET_KIND_FORM_TITLES = {
    "cloud_db": "Cloud Database",
    "vm": "Cloud Virtual Machine",
    "cloud_service": "Cloud Service",
}
