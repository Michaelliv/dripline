const API = "https://api.linear.app/graphql";
function linearQuery(ctx, query, variables = {}) {
    const token = ctx.connection.config.token || "";
    // syncGet doesn't do POST, use curl via syncExec approach
    // Actually, we can use syncGet with a trick: Linear supports GET with query params too
    // But let's just use the curl approach through syncGet's underlying mechanism
    // We need POST for GraphQL - let's use execSync
    const { execSync } = require("node:child_process");
    const body = JSON.stringify({ query, variables });
    // Linear: lin_api_ keys use plain Authorization, OAuth tokens use Bearer
    const authHeader = token.startsWith("lin_api") ? token : `Bearer ${token}`;
    const result = execSync(`curl -s -X POST "${API}" -H "Authorization: ${authHeader}" -H "Content-Type: application/json" -d '${body.replace(/'/g, "'\\''")}'`, { encoding: "utf-8", timeout: 30000 });
    try {
        return JSON.parse(result);
    }
    catch {
        return null;
    }
}
function* linearPaginate(ctx, query, dataPath) {
    let after = null;
    while (true) {
        const vars = { first: 100 };
        if (after)
            vars.after = after;
        const result = linearQuery(ctx, query, vars);
        if (!result?.data)
            return;
        let node = result.data;
        for (const key of dataPath)
            node = node?.[key];
        if (!node?.nodes?.length)
            return;
        yield* node.nodes;
        if (!node.pageInfo?.hasNextPage)
            return;
        after = node.pageInfo.endCursor;
    }
}
export default function linear(dl) {
    dl.setName("linear");
    dl.setVersion("0.1.0");
    dl.setConnectionSchema({
        token: {
            type: "string",
            required: true,
            description: "Linear API key or OAuth token",
            env: "LINEAR_TOKEN",
        },
    });
    dl.registerTable("linear_issues", {
        description: "Linear issues",
        columns: [
            { name: "id", type: "string" },
            { name: "identifier", type: "string" },
            { name: "title", type: "string" },
            { name: "description", type: "string" },
            { name: "priority", type: "number" },
            { name: "priority_label", type: "string" },
            { name: "state_name", type: "string" },
            { name: "assignee_name", type: "string" },
            { name: "team_name", type: "string" },
            { name: "project_name", type: "string" },
            { name: "labels", type: "json" },
            { name: "created_at", type: "datetime" },
            { name: "updated_at", type: "datetime" },
            { name: "completed_at", type: "datetime" },
            { name: "url", type: "string" },
        ],
        *list(ctx) {
            const query = `query($first: Int!, $after: String) {
        issues(first: $first, after: $after, orderBy: updatedAt) {
          nodes {
            id identifier title description priority priorityLabel
            state { name } assignee { name } team { name } project { name }
            labels { nodes { name } }
            createdAt updatedAt completedAt url
          }
          pageInfo { hasNextPage endCursor }
        }
      }`;
            for (const i of linearPaginate(ctx, query, ["issues"])) {
                yield {
                    id: i.id,
                    identifier: i.identifier || "",
                    title: i.title || "",
                    description: i.description || "",
                    priority: i.priority || 0,
                    priority_label: i.priorityLabel || "",
                    state_name: i.state?.name || "",
                    assignee_name: i.assignee?.name || "",
                    team_name: i.team?.name || "",
                    project_name: i.project?.name || "",
                    labels: JSON.stringify(i.labels?.nodes?.map((l) => l.name) || []),
                    created_at: i.createdAt || "",
                    updated_at: i.updatedAt || "",
                    completed_at: i.completedAt || "",
                    url: i.url || "",
                };
            }
        },
    });
    dl.registerTable("linear_projects", {
        description: "Linear projects",
        columns: [
            { name: "id", type: "string" },
            { name: "name", type: "string" },
            { name: "description", type: "string" },
            { name: "state", type: "string" },
            { name: "progress", type: "number" },
            { name: "lead_name", type: "string" },
            { name: "team_names", type: "json" },
            { name: "start_date", type: "string" },
            { name: "target_date", type: "string" },
            { name: "created_at", type: "datetime" },
            { name: "updated_at", type: "datetime" },
            { name: "url", type: "string" },
        ],
        *list(ctx) {
            const query = `query($first: Int!, $after: String) {
        projects(first: $first, after: $after) {
          nodes {
            id name description state progress
            lead { name } teams { nodes { name } }
            startDate targetDate createdAt updatedAt url
          }
          pageInfo { hasNextPage endCursor }
        }
      }`;
            for (const p of linearPaginate(ctx, query, ["projects"])) {
                yield {
                    id: p.id,
                    name: p.name || "",
                    description: p.description || "",
                    state: p.state || "",
                    progress: p.progress || 0,
                    lead_name: p.lead?.name || "",
                    team_names: JSON.stringify(p.teams?.nodes?.map((t) => t.name) || []),
                    start_date: p.startDate || "",
                    target_date: p.targetDate || "",
                    created_at: p.createdAt || "",
                    updated_at: p.updatedAt || "",
                    url: p.url || "",
                };
            }
        },
    });
    dl.registerTable("linear_teams", {
        description: "Linear teams",
        columns: [
            { name: "id", type: "string" },
            { name: "name", type: "string" },
            { name: "key", type: "string" },
            { name: "description", type: "string" },
            { name: "issue_count", type: "number" },
        ],
        *list(ctx) {
            const query = `query($first: Int!, $after: String) {
        teams(first: $first, after: $after) {
          nodes { id name key description issueCount }
          pageInfo { hasNextPage endCursor }
        }
      }`;
            for (const t of linearPaginate(ctx, query, ["teams"])) {
                yield {
                    id: t.id,
                    name: t.name || "",
                    key: t.key || "",
                    description: t.description || "",
                    issue_count: t.issueCount || 0,
                };
            }
        },
    });
    dl.registerTable("linear_users", {
        description: "Linear users",
        columns: [
            { name: "id", type: "string" },
            { name: "name", type: "string" },
            { name: "email", type: "string" },
            { name: "display_name", type: "string" },
            { name: "active", type: "boolean" },
            { name: "admin", type: "boolean" },
            { name: "created_at", type: "datetime" },
        ],
        *list(ctx) {
            const query = `query($first: Int!, $after: String) {
        users(first: $first, after: $after) {
          nodes { id name email displayName active admin createdAt }
          pageInfo { hasNextPage endCursor }
        }
      }`;
            for (const u of linearPaginate(ctx, query, ["users"])) {
                yield {
                    id: u.id,
                    name: u.name || "",
                    email: u.email || "",
                    display_name: u.displayName || "",
                    active: u.active ? 1 : 0,
                    admin: u.admin ? 1 : 0,
                    created_at: u.createdAt || "",
                };
            }
        },
    });
    dl.registerTable("linear_comments", {
        description: "Linear issue comments",
        columns: [
            { name: "id", type: "string" },
            { name: "body", type: "string" },
            { name: "issue_id", type: "string" },
            { name: "issue_identifier", type: "string" },
            { name: "user_name", type: "string" },
            { name: "created_at", type: "datetime" },
            { name: "updated_at", type: "datetime" },
        ],
        *list(ctx) {
            const query = `query($first: Int!, $after: String) {
        comments(first: $first, after: $after) {
          nodes {
            id body
            issue { id identifier }
            user { name }
            createdAt updatedAt
          }
          pageInfo { hasNextPage endCursor }
        }
      }`;
            for (const c of linearPaginate(ctx, query, ["comments"])) {
                yield {
                    id: c.id,
                    body: c.body || "",
                    issue_id: c.issue?.id || "",
                    issue_identifier: c.issue?.identifier || "",
                    user_name: c.user?.name || "",
                    created_at: c.createdAt || "",
                    updated_at: c.updatedAt || "",
                };
            }
        },
    });
    dl.registerTable("linear_labels", {
        description: "Linear issue labels",
        columns: [
            { name: "id", type: "string" },
            { name: "name", type: "string" },
            { name: "color", type: "string" },
            { name: "description", type: "string" },
            { name: "parent_name", type: "string" },
        ],
        *list(ctx) {
            const query = `query($first: Int!, $after: String) {
        issueLabels(first: $first, after: $after) {
          nodes { id name color description parent { name } }
          pageInfo { hasNextPage endCursor }
        }
      }`;
            for (const l of linearPaginate(ctx, query, ["issueLabels"])) {
                yield {
                    id: l.id,
                    name: l.name || "",
                    color: l.color || "",
                    description: l.description || "",
                    parent_name: l.parent?.name || "",
                };
            }
        },
    });
}
