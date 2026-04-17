# Deploying dripyard

Dripyard is a single Bun HTTP server that serves both the API and the embedded React UI. Deployment is "run one process, give it a persistent volume, put TLS in front of it." Any host that can run a Bun (or Node) container works — Fly.io, Railway, Render, Kubernetes, a VPS, your laptop.

## Ingredients

- A workspace directory (`.dripline/` + `.dripyard/`) — either built into the image or mounted as a volume.
- Env vars for every plugin that needs credentials (`GITHUB_TOKEN`, `STRIPE_API_KEY`, etc.).
- A port exposed (default `3457`).
- A persistent volume for `.dripyard/` if you care about history across restarts.

## Dockerfile

```dockerfile
FROM oven/bun:1.1 AS base
WORKDIR /app

# Install CLIs globally
RUN bun install -g dripline dripyard

# Bring in your workspace
COPY .dripline ./.dripline
COPY .dripyard ./.dripyard
# ^ If your plugins or config have local paths, copy those too.

# (optional) install plugins at build time so the image is self-contained
RUN dripline plugin install git:github.com/Michaelliv/dripline@v0.7.0#packages/plugins/github \
 && dripline plugin install git:github.com/Michaelliv/dripline@v0.7.0#packages/plugins/stripe

EXPOSE 3457
CMD ["dripyard", "serve", "--port", "3457"]
```

Tips:

- **Pin plugin versions** with `@<tag>` so the image is reproducible.
- **Install plugins at build time**, not at first boot, so the first request isn't blocked on a git clone.
- Mount `/app/.dripyard` as a volume in production so DuckDB files and artifacts survive redeploys.

## Running it

```bash
docker build -t my-dripyard .
docker run -d \
  -p 3457:3457 \
  -v dripyard-data:/app/.dripyard \
  -e GITHUB_TOKEN=ghp_xxx \
  -e STRIPE_API_KEY=sk_xxx \
  --name dripyard \
  my-dripyard
```

Hit `http://localhost:3457/` and you're in.

## Fly.io, Railway, Render

All three accept the Dockerfile above unchanged. Each has its own volume + secrets story:

- **Fly**: `fly volumes create dripyard_data --size 1`, then mount `/app/.dripyard` to it. Secrets via `fly secrets set GITHUB_TOKEN=...`.
- **Railway**: add a volume in the service UI, env vars in settings.
- **Render**: add a disk to the service, mount at `/app/.dripyard`, env vars in the dashboard.

## Kubernetes sketch

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: dripyard }
spec:
  replicas: 1    # dripyard is not horizontally scalable — single-writer DuckDB
  template:
    spec:
      containers:
        - name: dripyard
          image: ghcr.io/you/my-dripyard:v0.7.0
          ports: [{ containerPort: 3457 }]
          env:
            - name: GITHUB_TOKEN
              valueFrom: { secretKeyRef: { name: dripyard-secrets, key: github_token } }
          volumeMounts:
            - { name: data, mountPath: /app/.dripyard }
      volumes:
        - name: data
          persistentVolumeClaim: { claimName: dripyard-data }
```

Put a Service + Ingress (nginx, Traefik, whatever) in front for TLS. **Do not** run replicas > 1 — dripyard is single-writer against the DuckDB files in `.dripyard/`.

## TLS / public hostname

Dripyard itself does not terminate TLS. Use:

- Fly/Railway/Render: automatic HTTPS on their managed domain.
- VPS: Caddy or nginx in front, Let's Encrypt via the usual ACME flow.
- K8s: Ingress controller with cert-manager.

## Health, logs, observability

- Server logs go to stdout — pipe them wherever you pipe everything else.
- Worker logs are aggregated in the server UI and also written to `.dripyard/logs/`.
- There's no built-in `/health` yet; `GET /` returning 200 is a serviceable liveness check.

## Backups

Back up the entire `.dripyard/` directory. That's your database (DuckDB files) plus artifacts plus worker state. Standard snapshot/restore of the volume is enough — dripyard reopens its DuckDBs on start.

## Upgrading

1. Bump the base image / `dripline` + `dripyard` versions.
2. Rebuild, roll out.
3. Schema migrations (if any) run on startup.

Lockstep versioning means `dripline` and `dripyard` always match — don't mix a `0.7` server with `0.8` plugins.
