# dripline-plugin-skills-sh

[skills.sh](https://skills.sh) plugin for [dripline](https://github.com/Michaelliv/dripline) — search the AI agent skills registry with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/skills-sh
```

## Tables

| Table | Description |
|-------|-------------|
| `skills_search` | Search skills by keyword (WHERE query = '...') |

## Examples

### Search for skills

```sql
SELECT name, source, installs FROM skills_search WHERE query = 'react' ORDER BY installs DESC LIMIT 5;
```

```
┌─────────────────────────────┬─────────────────────────────────┬──────────┐
│ name                        │ source                          │ installs │
├─────────────────────────────┼─────────────────────────────────┼──────────┤
│ vercel-react-best-practices │ vercel-labs/agent-skills        │ 231411   │
│ vercel-react-native-skills  │ vercel-labs/agent-skills        │ 65689    │
│ react:components            │ google-labs-code/stitch-skills  │ 18740    │
│ react-native-best-practices │ callstackincubator/agent-skills │ 7909     │
│ react-doctor                │ millionco/react-doctor          │ 6193     │
└─────────────────────────────┴─────────────────────────────────┴──────────┘
```

### Find Obsidian skills

```sql
SELECT name, source, installs FROM skills_search WHERE query = 'obsidian' ORDER BY installs DESC LIMIT 5;
```

### Skills by source

```sql
SELECT source, COUNT(*) as skills, SUM(installs) as total_installs
FROM skills_search
WHERE query = 'typescript'
GROUP BY source
ORDER BY total_installs DESC;
```

### Top skills for a topic

```sql
SELECT name, source, installs
FROM skills_search
WHERE query = 'docker' AND search_limit = '20'
ORDER BY installs DESC;
```
