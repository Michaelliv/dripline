# dripline + dripyard docs

Two tools, one monorepo.

- **dripline** — SQL query engine for REST APIs and CLIs, via plugins.
- **dripyard** — dashboard + worker supervisor that runs on top of a dripline workspace.

Start here:

- [Quickstart](./quickstart.md) — empty directory to queries running to UI deployed, end to end.
- Plugins
  - [Installing plugins](./plugins/install.md)
  - [Writing your own plugin](./plugins/write-your-own.md)
- Dripyard
  - [Overview](./dripyard/overview.md)
  - [Deploying dripyard](./dripyard/deploy.md)

If you want the raw architecture tour, read the repo root `CLAUDE.md` and `packages/dripline/CLAUDE.md`. These docs assume you want to *use* the tools, not modify them.
