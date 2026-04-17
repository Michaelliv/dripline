# Installing plugins

Dripline plugins are **not** on npm. They live in `packages/plugins/` inside this repo and get installed directly from git. This is deliberate — plugins move fast, we don't want a registry release for every small fix, and they're always in lockstep with the dripline core they're built against.

## Install from the monorepo

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/<name>
```

The `#` fragment is a git subpath, pointing to a specific plugin's directory inside the monorepo.

Examples:

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/github
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/docker
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/cloudflare
```

Installed plugins are recorded in `.dripline/plugins.json` and cached under `.dripline/plugins/git/...`. Re-running `install` pulls the latest.

## Pin to a tag or commit

By default you get the default branch. Pin by appending `@<ref>` before the `#subpath`:

```bash
dripline plugin install git:github.com/Michaelliv/dripline@v0.9.0#packages/plugins/github
```

For reproducible deploys, always pin.

## Install a local plugin

When you're developing your own plugin, point at the file directly:

```bash
dripline plugin install ./my-plugin/src/index.ts
```

Dripline will load it from that path every time — edits are picked up on the next run.

## Install from an arbitrary git repo

Anyone can publish plugins in their own repo. The pattern is:

```bash
dripline plugin install git:github.com/<user>/<repo>#<optional-subpath>
```

If the repo *is* the plugin (package.json at the root), drop the subpath:

```bash
dripline plugin install git:github.com/acme/dripline-plugin-acme
```

## Listing and removing

```bash
dripline plugin list
dripline plugin remove <name>
```

## What's bundled in this repo

Browse [`packages/plugins/`](../../packages/plugins/) for the full list. High-level categories:

- **Cloud** — cloudflare, vercel, aws, gcp
- **SaaS APIs** — github, slack, stripe, linear, okta, twilio, zendesk, trello, hubspot, intercom, sendgrid, notion, figma
- **DevOps** — docker, kubectl, terraform, trivy, tailscale
- **Local / system** — ps, git, brew, npm, spotlight, system-profiler
- **Pi ecosystem** — pi (sessions, costs, prompts)

Each plugin's README documents its tables and required env vars.

## Why not npm?

Plugins change shape with the dripline core. A plugin published three weeks ago against `dripline@0.6` may break against `0.7` in ways that are annoying to version-gate. Git-pin means you always build against a specific commit of both sides at once, and there's no publish step on the critical path when you add a new table.

If you *want* to publish a plugin to npm, nothing stops you — it's just a TypeScript file with a default export. See [Writing your own plugin](./write-your-own.md).
