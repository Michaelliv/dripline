# dripline-plugin-pi

[pi](https://github.com/badlogic/pi-mono) coding agent plugin for [dripline](https://github.com/Michaelliv/dripline) — query sessions, messages, tool usage, and costs with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/pi
```

Reads session data from `~/.pi/agent/sessions/`.

## Tables

| Table | Description |
|-------|-------------|
| `pi_sessions` | All pi sessions with token/cost summaries |
| `pi_messages` | Individual messages (user, assistant) |
| `pi_tool_calls` | Every tool call (bash, edit, read, write, etc.) |
| `pi_costs` | Detailed cost breakdown per session |
| `pi_prompt` | Send a prompt to pi, get a response (WHERE prompt = '...') |
| `pi_generate` | Generate structured JSON data with pi |

## Examples

### Cost per project

```sql
SELECT project, COUNT(*) as sessions,
       SUM(total_tokens) as tokens,
       ROUND(SUM(total_cost), 2) as cost
FROM pi_sessions
GROUP BY project
ORDER BY cost DESC
LIMIT 5;
```

```
┌─────────────────────┬──────────┬───────────┬───────┐
│ project             │ sessions │ tokens    │ cost  │
├─────────────────────┼──────────┼───────────┼───────┤
│ /Projects/my-app/   │ 28       │ 204204743 │ 130.5 │
│ /Projects/backend/  │ 15       │ 64940108  │ 41.4  │
│ /Projects/docs/     │ 12       │ 18355029  │ 11.9  │
│ /Projects/cli-tool/ │ 8        │ 30310702  │ 19.9  │
│ /Projects/scripts/  │ 5        │ 7350163   │ 5.9   │
└─────────────────────┴──────────┴───────────┴───────┘
```

### Most used tools

```sql
SELECT tool_name, COUNT(*) as calls
FROM pi_tool_calls
GROUP BY tool_name
ORDER BY calls DESC;
```

```
┌─────────────────┬───────┐
│ tool_name       │ calls │
├─────────────────┼───────┤
│ bash            │ 18081 │
│ edit            │ 5971  │
│ read            │ 5297  │
│ write           │ 1663  │
│ AskUserQuestion │ 104   │
│ show_widget     │ 66    │
└─────────────────┴───────┘
```

### Cost per model

```sql
SELECT model, COUNT(*) as sessions,
       ROUND(SUM(total_cost), 2) as total_cost
FROM pi_sessions
WHERE model != ''
GROUP BY model
ORDER BY total_cost DESC;
```

```
┌─────────────────┬──────────┬────────────┐
│ model           │ sessions │ total_cost │
├─────────────────┼──────────┼────────────┤
│ claude-opus-4   │ 82       │ 991.09     │
│ claude-sonnet-4 │ 32       │ 173.75     │
│ gpt-5           │ 10       │ 124.15     │
└─────────────────┴──────────┴────────────┘
```

### Daily spend

```sql
SELECT CAST(started_at AS DATE) as day,
       COUNT(*) as sessions,
       ROUND(SUM(total_cost), 2) as cost
FROM pi_sessions
GROUP BY day
ORDER BY day DESC
LIMIT 7;
```

### Tool calls per project

```sql
SELECT t.project, t.tool_name, COUNT(*) as calls
FROM pi_tool_calls t
GROUP BY t.project, t.tool_name
ORDER BY calls DESC
LIMIT 10;
```

### Find expensive sessions

```sql
SELECT project, model, total_tokens, total_cost, started_at
FROM pi_sessions
WHERE total_cost > 10
ORDER BY total_cost DESC;
```

### Ask pi a question

```sql
SELECT response FROM pi_prompt WHERE prompt = 'What is the capital of Japan? Reply in one word.';
```

```
┌──────────┐
│ response │
├──────────┤
│ Tokyo    │
└──────────┘
```

### Generate structured data with AI

```sql
SELECT data->>'name' as name,
       CAST(data->>'age' AS INT) as age,
       data->>'city' as city
FROM pi_generate
WHERE prompt = 'Generate 5 fictional software engineers with name, age, and city';
```

```
┌────────────────┬─────┬──────────┐
│ name           │ age │ city     │
├────────────────┼─────┼──────────┤
│ Talia Vasquez  │ 29  │ Portland │
│ Jun Nakamura   │ 34  │ Tokyo    │
│ Elise Fournier │ 41  │ Lyon     │
│ Kofi Mensah    │ 26  │ Accra    │
│ Darya Sokolova │ 37  │ Berlin   │
└────────────────┴─────┴──────────┘
```

### Use a specific model

```sql
SELECT response FROM pi_prompt
WHERE prompt = 'Explain recursion in one sentence'
AND model = 'claude-sonnet-4';
```
