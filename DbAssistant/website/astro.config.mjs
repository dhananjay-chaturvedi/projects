// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// GitHub Pages project site: https://dhananjay-chaturvedi.github.io/dbassistant/
// For a custom domain at the repo root, set site to that domain and remove base.
export default defineConfig({
  site: 'https://dhananjay-chaturvedi.github.io',
  base: '/dbassistant/',
  trailingSlash: 'ignore',
  integrations: [
    starlight({
      title: 'DbAssistant',
      description:
        'Production-grade database management tool with AI query assistance, data migration (schema + data + validation), real-time monitoring, and full UI / CLI / REST API parity.',
      logo: {
        src: './src/assets/logo.svg',
        replacesTitle: false,
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/dhananjay-chaturvedi/dbassistant',
        },
      ],
      customCss: ['./src/styles/custom.css'],
      lastUpdated: true,
      editLink: {
        baseUrl:
          'https://github.com/dhananjay-chaturvedi/dbassistant/edit/main/website/',
      },
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 4 },
      head: [
        {
          tag: 'meta',
          attrs: {
            name: 'theme-color',
            content: '#2196F3',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image',
            content: '/og-image.svg',
          },
        },
      ],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Overview', slug: 'getting-started/overview' },
            { label: 'Installation', slug: 'getting-started/installation' },
            { label: 'Quickstart', slug: 'getting-started/quickstart' },
            { label: 'Configuration', slug: 'getting-started/configuration' },
            { label: 'Uninstall', slug: 'getting-started/uninstall' },
          ],
        },
        {
          label: 'Architecture',
          items: [
            { label: 'Overview', slug: 'architecture/overview' },
            { label: 'File layout (~/.dbassistant)', slug: 'architecture/file-layout' },
            { label: 'Modules & shipping', slug: 'architecture/modules' },
            { label: 'Security model', slug: 'architecture/security' },
          ],
        },
        {
          label: 'Modules',
          items: [
            { label: 'Data Migration', slug: 'modules/data-migration' },
            { label: 'AI Query Assistant', slug: 'modules/ai-query' },
            { label: 'Monitoring', slug: 'modules/monitoring' },
            { label: 'App Builder', slug: 'modules/app-builder' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Settings & notifications', slug: 'guides/settings' },
            { label: 'RAG Manager (retrieval)', slug: 'guides/rag' },
            { label: 'Local LLM training', slug: 'guides/local-llm' },
          ],
        },
        {
          label: 'CLI Reference',
          items: [
            { label: 'Overview', slug: 'cli/overview' },
            { label: 'connections', slug: 'cli/connections' },
            { label: 'query', slug: 'cli/query' },
            { label: 'objects', slug: 'cli/objects' },
            { label: 'migrator', slug: 'cli/migrator' },
            { label: 'ai', slug: 'cli/ai' },
            { label: 'ai rag & llm', slug: 'cli/ai-rag-llm' },
            { label: 'app-builder', slug: 'cli/app-builder' },
            { label: 'monitor', slug: 'cli/monitor' },
            { label: 'daemon', slug: 'cli/daemon' },
            { label: 'thresholds', slug: 'cli/thresholds' },
            { label: 'cloud', slug: 'cli/cloud' },
            { label: 'os', slug: 'cli/os' },
            { label: 'notify', slug: 'cli/notify' },
            { label: 'databases', slug: 'cli/databases' },
            { label: 'config', slug: 'cli/config' },
          ],
        },
        {
          label: 'REST API Reference',
          items: [
            { label: 'Overview', slug: 'api/overview' },
            { label: 'Authentication', slug: 'api/authentication' },
            { label: 'Health & modules', slug: 'api/health-modules' },
            { label: 'Connections', slug: 'api/connections' },
            { label: 'Query & objects', slug: 'api/query-objects' },
            { label: 'Data Migration', slug: 'api/migrator' },
            { label: 'AI', slug: 'api/ai' },
            { label: 'RAG & LLM', slug: 'api/rag-llm' },
            { label: 'App Builder', slug: 'api/app-builder' },
            { label: 'Metrics', slug: 'api/metrics' },
            { label: 'Thresholds', slug: 'api/thresholds' },
            { label: 'OS metrics', slug: 'api/os' },
            { label: 'Cloud', slug: 'api/cloud' },
            { label: 'Notifications', slug: 'api/notify' },
            { label: 'Daemon status', slug: 'api/daemon' },
            { label: 'Dashboard', slug: 'api/dashboard' },
          ],
        },
        {
          label: 'Cloud setup',
          items: [
            { label: 'AWS (RDS / Aurora / PI)', slug: 'cloud/aws' },
            { label: 'Azure (SQL / MySQL / Postgres)', slug: 'cloud/azure' },
            { label: 'GCP (Cloud SQL)', slug: 'cloud/gcp' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Supported databases', slug: 'reference/supported-databases' },
            { label: 'Threshold rules schema', slug: 'reference/threshold-rules' },
            { label: 'config.ini', slug: 'reference/config-ini' },
            { label: 'Environment variables', slug: 'reference/env-vars' },
          ],
        },
        {
          label: 'Operations',
          items: [
            { label: 'Daemon & systemd', slug: 'operations/daemon' },
            { label: 'Programmatic use (Python)', slug: 'operations/programmatic' },
            { label: 'Shipper / packaging', slug: 'operations/shipper' },
          ],
        },
        {
          label: 'Troubleshooting',
          items: [
            { label: 'Common issues', slug: 'troubleshooting/common-issues' },
            { label: 'FAQ', slug: 'troubleshooting/faq' },
          ],
        },
      ],
    }),
  ],
});
