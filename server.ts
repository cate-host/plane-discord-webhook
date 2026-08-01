/**
 * server.ts — Plane → Discord webhook relay
 *
 * Run: bun run server.ts
 * Env: PLANE_SECRET, DISCORD_WEBHOOK_URL, PLANE_BASE_URL, PORT (optional)
 */

import { Database } from "bun:sqlite";
import {
  parsePlaneRequest,
  shouldNotify,
  issueUrl,
  GROUP_COLORS,
  type PlaneWebhookEvent,
  type PlaneChange,
  type PlaneIssueData,
  type PlaneProjectData,
} from "./plane-webhook";

const SECRET = Bun.env.PLANE_SECRET;
const DISCORD_URL = Bun.env.DISCORD_WEBHOOK_URL;
const PLANE_BASE = Bun.env.PLANE_BASE_URL ?? "https://plane.example.com";
const PORT = Number(Bun.env.PORT ?? 3000);
const DEBOUNCE_MS = 2500;
/** Edit the existing embed instead of posting a new one, within this window. */
const EDIT_WINDOW_MS = 10 * 60 * 1000;

if (!SECRET || !DISCORD_URL) {
  console.error("Missing PLANE_SECRET or DISCORD_WEBHOOK_URL");
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────
// Database
// ─────────────────────────────────────────────────────────────

const db = new Database("plane.db", { create: true, strict: true });
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

db.exec(`
CREATE TABLE IF NOT EXISTS deliveries (
  delivery_id TEXT PRIMARY KEY,
  event TEXT NOT NULL, action TEXT NOT NULL,
  entity_id TEXT NOT NULL, project_id TEXT,
  workspace_slug TEXT NOT NULL, field TEXT,
  raw TEXT NOT NULL, received_at INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS batches (
  batch_key TEXT PRIMARY KEY,
  event TEXT NOT NULL, entity_id TEXT NOT NULL, project_id TEXT,
  workspace_slug TEXT NOT NULL,
  actor TEXT NOT NULL, snapshot TEXT NOT NULL,
  opened_at INTEGER NOT NULL, flush_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_changes (
  batch_key TEXT NOT NULL REFERENCES batches(batch_key) ON DELETE CASCADE,
  field TEXT NOT NULL, old_value TEXT, new_value TEXT,
  seq INTEGER NOT NULL,
  PRIMARY KEY (batch_key, field, seq)
);
CREATE TABLE IF NOT EXISTS messages (
  entity_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  last_posted_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY, identifier TEXT NOT NULL,
  name TEXT NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_flush ON batches(flush_at);
`);

const q = {
  insertDelivery: db.query(`
    INSERT OR IGNORE INTO deliveries
      (delivery_id, event, action, entity_id, project_id,
       workspace_slug, field, raw, received_at)
    VALUES ($id, $event, $action, $entityId, $projectId,
            $slug, $field, $raw, $now)`),
  setStatus: db.query(
    `UPDATE deliveries SET status = $status WHERE delivery_id = $id`,
  ),
  upsertBatch: db.query(`
    INSERT INTO batches
      (batch_key, event, entity_id, project_id, workspace_slug,
       actor, snapshot, opened_at, flush_at)
    VALUES ($key, $event, $entityId, $projectId, $slug,
            $actor, $snapshot, $now, $flushAt)
    ON CONFLICT(batch_key) DO UPDATE SET
      snapshot = excluded.snapshot,
      actor    = excluded.actor,
      flush_at = excluded.flush_at`),
  addChange: db.query(`
    INSERT OR REPLACE INTO batch_changes
      (batch_key, field, old_value, new_value, seq)
    VALUES ($key, $field, $old, $new, $seq)`),
  nextSeq: db.query(
    `SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM batch_changes WHERE batch_key = $key`,
  ),
  dueBatches: db.query(`SELECT * FROM batches WHERE flush_at <= $now`),
  changesFor: db.query(
    `SELECT * FROM batch_changes WHERE batch_key = $key ORDER BY seq ASC`,
  ),
  dropBatch: db.query(`DELETE FROM batches WHERE batch_key = $key`),
  upsertProject: db.query(`
    INSERT INTO projects (id, identifier, name, updated_at)
    VALUES ($id, $identifier, $name, $now)
    ON CONFLICT(id) DO UPDATE SET
      identifier = excluded.identifier,
      name = excluded.name,
      updated_at = excluded.updated_at`),
  getProject: db.query(`SELECT * FROM projects WHERE id = $id`),
  getMessage: db.query(`SELECT * FROM messages WHERE entity_id = $id`),
  putMessage: db.query(`
    INSERT INTO messages (entity_id, message_id, last_posted_at)
    VALUES ($id, $msg, $now)
    ON CONFLICT(entity_id) DO UPDATE SET
      message_id = excluded.message_id,
      last_posted_at = excluded.last_posted_at`),
};

// ─────────────────────────────────────────────────────────────
// Discord (plain incoming webhook — no bot token needed)
// ─────────────────────────────────────────────────────────────

type Embed = Record<string, unknown>;

async function discordFetch(url: string, init: RequestInit): Promise<Response> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const res = await fetch(url, init);
    if (res.status !== 429) return res;
    const body = (await res.json().catch(() => ({}))) as { retry_after?: number };
    const waitMs = Math.ceil((body.retry_after ?? 1) * 1000) + 100;
    console.warn(`rate limited, waiting ${waitMs}ms`);
    await Bun.sleep(waitMs);
  }
  throw new Error("Discord rate limit: gave up after 5 attempts");
}

/** Post a new embed. `?wait=true` makes Discord return the message ID. */
async function postEmbed(embed: Embed): Promise<string | null> {
  const res = await discordFetch(`${DISCORD_URL}?wait=true`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ embeds: [embed] }),
  });
  if (!res.ok) {
    console.error("discord post failed", res.status, await res.text());
    return null;
  }
  const msg = (await res.json()) as { id: string };
  return msg.id;
}

/** Edit a message this same webhook created earlier. */
async function editEmbed(messageId: string, embed: Embed): Promise<boolean> {
  const res = await discordFetch(`${DISCORD_URL}/messages/${messageId}`, {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ embeds: [embed] }),
  });
  return res.ok;
}

// ─────────────────────────────────────────────────────────────
// Formatting
// ─────────────────────────────────────────────────────────────

const PRIORITY_ICON: Record<string, string> = {
  urgent: "🔴", high: "🟠", medium: "🟡", low: "🔵", none: "⚪",
};

function issueRef(issue: PlaneIssueData): string {
  const row = q.getProject.get({ id: issue.project }) as
    | { identifier: string }
    | undefined;
  return row ? `${row.identifier}-${issue.sequence_id}` : `#${issue.sequence_id}`;
}

function describeChange(c: PlaneChange, snapshot: PlaneIssueData): string {
  const fmt = (v: unknown) =>
    v === null || v === "" ? "_none_" : `\`${String(v)}\``;

  switch (c.field) {
    case "state":
      return `**State** → ${snapshot.state.name}`;
    case "priority":
      return `**Priority** ${fmt(c.oldValue)} → ${PRIORITY_ICON[String(c.newValue)] ?? ""} ${fmt(c.newValue)}`;
    case "assignee_ids": {
      const names = snapshot.assignees.map((a) => a.display_name);
      return names.length
        ? `**Assignees** → ${names.join(", ")}`
        : `**Assignees** cleared`;
    }
    case "name":
      return `**Renamed** from ${fmt(c.oldValue)}`;
    case "target_date":
    case "start_date":
      return `**${c.field === "target_date" ? "Due" : "Start"}** ${fmt(c.oldValue)} → ${fmt(c.newValue)}`;
    case "description":
      return `**Description** edited`;
    case "parent":
      return `**Parent** changed`;
    default:
      return `**${c.field}** ${fmt(c.oldValue)} → ${fmt(c.newValue)}`;
  }
}

function buildIssueEmbed(
  issue: PlaneIssueData,
  slug: string,
  actorName: string,
  actorAvatar: string,
  changes: PlaneChange[],
  action: string,
): Embed {
  const ref = issueRef(issue);
  const url = issueUrl(PLANE_BASE, slug, issue.project, issue.id);

  const fields: Embed[] = [
    { name: "State", value: issue.state.name, inline: true },
    {
      name: "Priority",
      value: `${PRIORITY_ICON[issue.priority] ?? ""} ${issue.priority}`,
      inline: true,
    },
  ];
  if (issue.assignees.length) {
    fields.push({
      name: "Assignees",
      value: issue.assignees.map((a) => a.display_name).join(", "),
      inline: true,
    });
  }
  if (changes.length) {
    fields.push({
      name: "Changes",
      value: changes.map((c) => `• ${describeChange(c, issue)}`).join("\n"),
      inline: false,
    });
  }

  const verb =
    action === "created" ? "created" : action === "deleted" ? "deleted" : "updated";

  return {
    title: `${ref} · ${issue.name}`,
    url,
    description: issue.description_stripped
      ? issue.description_stripped.slice(0, 200) +
        (issue.description_stripped.length > 200 ? "…" : "")
      : undefined,
    color: GROUP_COLORS[issue.state.group],
    fields,
    author: {
      name: `${actorName} ${verb} an issue`,
      icon_url: actorAvatar ? `${PLANE_BASE}${actorAvatar}` : undefined,
    },
    timestamp: issue.updated_at,
    footer: { text: slug },
  };
}

// ─────────────────────────────────────────────────────────────
// Dispatch
// ─────────────────────────────────────────────────────────────

async function send(entityId: string, embed: Embed) {
  const prior = q.getMessage.get({ id: entityId }) as
    | { message_id: string; last_posted_at: number }
    | undefined;

  const now = Date.now();
  if (prior && now - prior.last_posted_at < EDIT_WINDOW_MS) {
    const ok = await editEmbed(prior.message_id, embed);
    if (ok) {
      q.putMessage.run({ id: entityId, msg: prior.message_id, now });
      return;
    }
    // message was deleted or token rotated — fall through and post fresh
  }

  const msgId = await postEmbed(embed);
  if (msgId) q.putMessage.run({ id: entityId, msg: msgId, now });
}

function cacheProjectFrom(evt: PlaneWebhookEvent) {
  if (evt.event !== "project") return;
  const p = evt.payload.data as PlaneProjectData;
  q.upsertProject.run({
    id: p.id, identifier: p.identifier, name: p.name, now: Date.now(),
  });
}

async function flushDue() {
  const now = Date.now();
  const due = q.dueBatches.all({ now }) as Array<{
    batch_key: string; event: string; entity_id: string;
    workspace_slug: string; actor: string; snapshot: string;
  }>;

  for (const b of due) {
    const rows = q.changesFor.all({ key: b.batch_key }) as Array<{
      field: string; old_value: string | null; new_value: string | null; seq: number;
    }>;

    // Collapse: first old, last new, per field. Drop no-ops.
    const merged = new Map<string, PlaneChange>();
    for (const r of rows) {
      const oldV = r.old_value === null ? null : JSON.parse(r.old_value);
      const newV = r.new_value === null ? null : JSON.parse(r.new_value);
      const existing = merged.get(r.field);
      merged.set(r.field, {
        field: r.field,
        oldValue: existing ? existing.oldValue : oldV,
        newValue: newV,
        isMeaningful: true,
      });
    }
    const changes = [...merged.values()].filter(
      (c) => JSON.stringify(c.oldValue) !== JSON.stringify(c.newValue),
    );

    q.dropBatch.run({ key: b.batch_key });
    if (!changes.length) continue; // net-zero window, e.g. toggle thrash

    const snapshot = JSON.parse(b.snapshot);
    const actor = JSON.parse(b.actor);

    if (b.event === "issue") {
      const embed = buildIssueEmbed(
        snapshot as PlaneIssueData,
        b.workspace_slug,
        actor.display_name,
        actor.avatar_url,
        changes,
        "updated",
      );
      await send(b.entity_id, embed);
    }
    // project updates: add a buildProjectEmbed here when you want them
  }
}

setInterval(() => {
  flushDue().catch((e) => console.error("flush error", e));
}, 1000);

// ─────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────

Bun.serve({
  port: PORT,
  async fetch(req) {
    const result = await parsePlaneRequest(req, SECRET!);
    if (!result.ok) {
      // 4xx tells Plane not to retry a request that will never succeed
      return new Response(result.reason, { status: result.status });
    }

    const evt = result.event;
    const now = Date.now();

    // Dedupe: a Plane retry hits the PK and inserts nothing.
    q.insertDelivery.run({
      id: evt.deliveryId,
      event: evt.event,
      action: evt.action,
      entityId: evt.entityId,
      projectId: evt.projectId,
      slug: evt.workspaceSlug,
      field: evt.changes[0]?.field ?? null,
      raw: evt.raw,
      now,
    });
    if (db.query("SELECT changes() AS c").get<{ c: number }>()!.c === 0) {
      return new Response("duplicate", { status: 200 });
    }

    cacheProjectFrom(evt);

    if (!shouldNotify(evt)) {
      q.setStatus.run({ id: evt.deliveryId, status: "filtered" });
      return new Response("filtered", { status: 200 });
    }

    // Creates and deletes post immediately — no fanout to coalesce.
    if (evt.isCreate || evt.isDelete) {
      q.setStatus.run({ id: evt.deliveryId, status: "sent" });
      if (evt.event === "issue") {
        const embed = buildIssueEmbed(
          evt.payload.data as PlaneIssueData,
          evt.workspaceSlug,
          evt.actor.display_name,
          evt.actor.avatar_url,
          [],
          evt.action,
        );
        // don't block the 200 — Plane times out on slow responses
        queueMicrotask(() => void send(evt.entityId, embed));
      }
      return new Response("ok", { status: 200 });
    }

    // Updates go into a debounce window.
    const key = `${evt.event}:${evt.entityId}`;
    q.upsertBatch.run({
      key,
      event: evt.event,
      entityId: evt.entityId,
      projectId: evt.projectId,
      slug: evt.workspaceSlug,
      actor: JSON.stringify(evt.actor),
      snapshot: JSON.stringify(evt.payload.data),
      now,
      flushAt: now + DEBOUNCE_MS,
    });
    for (const c of evt.changes) {
      const seq = (q.nextSeq.get({ key }) as { n: number }).n;
      q.addChange.run({
        key,
        field: c.field,
        old: JSON.stringify(c.oldValue),
        new: JSON.stringify(c.newValue),
        seq,
      });
    }
    q.setStatus.run({ id: evt.deliveryId, status: "batched" });
    return new Response("batched", { status: 200 });
  },
});

console.log(`listening on :${PORT}`);
