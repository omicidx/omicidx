# OmicIDX docs

[Astro](https://astro.build) + [Starlight](https://starlight.astro.build) static documentation site. Deploys to `https://docs.omicidx.cancerdatasci.org`.

## Local development

```bash
cd docs/
npm install        # one-time
npm run dev        # local dev server, hot reload at http://localhost:4321
npm run build      # static build into ./dist
npm run preview    # serve the production build locally
```

## Content layout

```
src/
├── components/                 # Reusable Astro components
│   ├── ScalarReference.astro   # Embeds the live OpenAPI spec via Scalar
│   └── Wip.astro               # 🚧 Construction-zone callout
└── content/
    └── docs/
        ├── index.mdx           # Landing page
        ├── overview/           # User-facing overview
        ├── api/                # API guide + OpenAPI reference
        ├── guides/             # Tutorials (mostly stubs until clients ship)
        └── contributing/       # Contributor's view
```

The sidebar TOC is configured in `astro.config.mjs`.

## Conventions

- **Markdown vs MDX:** plain `.md` for prose pages; `.mdx` only when the page imports a component (e.g., `Wip` or `ScalarReference`).
- **Construction zones:** mark partially-written sections with the `Wip` component so visitors know what's stable. Out-of-date docs that claim authority are worse than no docs.
- **API reference is auto-generated.** The page at `/api/reference/` renders the live OpenAPI spec at `https://api-omicidx.cancerdatasci.org/openapi.json` via Scalar. Don't hand-write endpoint docs — fix the FastAPI source instead.

## Hosting

Tracked in [#75](https://github.com/omicidx/omicidx/issues/75): static container served behind the existing Traefik proxy at `docs.omicidx.cancerdatasci.org`.
