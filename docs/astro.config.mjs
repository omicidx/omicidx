// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://docs.omicidx.cancerdatasci.org",
  integrations: [
    starlight({
      title: "OmicIDX",
      description:
        "Public, queryable index of NCBI SRA, GEO, BioSample, BioProject, PubMed, and EBI BioSamples metadata.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/omicidx/omicidx",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/omicidx/omicidx/edit/main/docs/",
      },
      lastUpdated: true,
      sidebar: [
        {
          label: "Overview",
          items: [
            { label: "What is OmicIDX?", slug: "overview/what-is-omicidx" },
            { label: "Data sources", slug: "overview/data-sources" },
            { label: "Architecture", slug: "overview/architecture" },
          ],
        },
        {
          label: "API",
          items: [
            { label: "Overview", slug: "api/overview" },
            { label: "Pagination", slug: "api/pagination" },
            { label: "Rate limits", slug: "api/rate-limits" },
            { label: "Reference (OpenAPI)", slug: "api/reference" },
          ],
        },
        {
          label: "Guides",
          items: [{ label: "Overview", slug: "guides" }],
        },
        {
          label: "Contributing",
          items: [
            { label: "Architecture", slug: "contributing/architecture" },
            {
              label: "Automation cadence",
              slug: "contributing/automation-cadence",
            },
          ],
        },
      ],
    }),
  ],
});
