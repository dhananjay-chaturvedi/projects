# DbAssistant — Website & Documentation

Production-grade documentation site for **DbAssistant**, built with
[Astro](https://astro.build) + [Starlight](https://starlight.astro.build).

## Stack

- **Astro 5** — static-first, zero JS by default
- **Starlight** — docs framework (sidebar, search, light/dark mode, syntax-highlighted code)
- **Cloudflare Pages / GitHub Pages compatible** — pure static output

## Local development

```bash
cd website
nvm use            # uses .nvmrc (Node 20)
npm install
npm run dev        # http://localhost:4321
```

## Production build

```bash
npm run build      # outputs to ./dist
npm run preview    # local preview of the built site
```

The build is fully static — drop `dist/` on any static host.

## Project structure

```
website/
├── public/                # static assets (favicon, og-image, screenshots)
├── src/
│   ├── assets/            # processed images, logo
│   ├── content/
│   │   ├── docs/          # all documentation Markdown
│   │   └── config.ts      # content collection definition (Starlight)
│   ├── pages/
│   │   └── index.astro    # landing page (custom hero, modules grid)
│   ├── components/
│   │   ├── Hero.astro
│   │   ├── FeatureGrid.astro
│   │   ├── ModuleCard.astro
│   │   └── QuickInstall.astro
│   └── styles/
│       └── custom.css     # brand colors override Starlight defaults
├── astro.config.mjs       # site config, sidebar, integrations
├── package.json
└── tsconfig.json
```

## Adding a new docs page

1. Drop a Markdown file under `src/content/docs/<section>/<page>.md` with front-matter:
   ```md
   ---
   title: My new page
   description: One-line summary for SEO.
   ---
   ```
2. Add it to the sidebar in `astro.config.mjs`.
3. `npm run dev` — hot-reload picks it up immediately.

## Adding a new section

1. Create the folder under `src/content/docs/<section>/`.
2. Add a sidebar group in `astro.config.mjs`:
   ```js
   {
     label: 'My section',
     items: [
       { label: 'My page', slug: '<section>/<page>' },
     ],
   }
   ```

## Deployment

GitHub Actions builds and deploys to **GitHub Pages** by default — see
`.github/workflows/website.yml`. To deploy on **Cloudflare Pages** instead:

1. Connect this repo to Cloudflare Pages.
2. Set the build command to `npm run build`.
3. Set the build output directory to `dist`.
4. Set the root directory to `website`.
5. Set the Node version to `20`.

Cloudflare Pages will give you a `*.pages.dev` URL immediately and lets you
attach a custom domain when ready.

## Updating the brand / theme

- Brand color: `src/styles/custom.css` (`--sl-color-accent`)
- Logo: `src/assets/logo.svg`
- OG image: `public/og-image.svg`
- Site title / description: `astro.config.mjs`
