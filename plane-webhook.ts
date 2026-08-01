/**
 * plane-webhook.ts
 *
 * Turns a raw Bun `Request` from Plane into a verified, normalized event object.
 *
 * Design notes:
 *  - The body is read exactly once as text, kept for HMAC verification, then parsed.
 *  - `activity.old_value` / `new_value` are native JSON types (bool, number, array),
 *    NOT strings — they're typed `unknown` and narrowed per field.
 *  - `assignees` and `state` arrive fully hydrated. `parent` and `project` are bare
 *    UUIDs and need resolution if you want human-readable output.
 *  - `old_identifier` / `new_identifier` have been null in every observed delivery.
 */

// ─────────────────────────────────────────────────────────────
// Raw wire types — mirror exactly what Plane sends
// ─────────────────────────────────────────────────────────────

export type PlaneActor = {
  id: string;
  first_name: string;
  last_name: string;
  email: string;
  avatar: string;
  avatar_url: string;
  display_name: string;
};

export type PlaneStateGroup =
  | "backlog"
  | "unstarted"
  | "started"
  | "completed"
  | "cancelled";

export type PlaneState = {
  id: string;
  name: string;
  color: string;
  group: PlaneStateGroup;
};

export type PlanePriority = "none" | "low" | "medium" | "high" | "urgent";

export type PlaneActivity = {
  /** null iff action === "created" */
  field: string | null;
  old_value: unknown;
  new_value: unknown;
  actor: PlaneActor;
  old_identifier: string | null;
  new_identifier: string | null;
};

export type PlaneIssueData = {
  id: string;
  name: string;
  sequence_id: number;
  priority: PlanePriority;
  state: PlaneState;
  assignees: PlaneActor[];
  labels: unknown[]; // shape unconfirmed — always [] in observed traffic
  description: Record<string, unknown>;
  description_json: Record<string, unknown>;
  description_html: string;
  description_stripped: string;
  description_binary: string | null;
  point: number | null;
  start_date: string | null;
  target_date: string | null;
  completed_at: string | null;
  archived_at: string | null;
  deleted_at: string | null;
  is_draft: boolean;
  sort_order: number;
  estimate_point: string | null;
  type: string | null;
  parent: string | null;
  project: string;
  workspace: string;
  created_at: string;
  updated_at: string;
  created_by: string;
  updated_by: string | null;
  external_source: string | null;
  external_id: string | null;
};

export type PlaneProjectData = {
  id: string;
  name: string;
  /** Display prefix, e.g. "BACKEND" → BACKEND-4 */
  identifier: string;
  description: string;
  description_text: string | null;
  description_html: string | null;
  /** 0 = private, 2 = public */
  network: number;
  logo_props: { emoji?: { value: string }; in_use?: string } | null;
  emoji: string | null;
  icon_prop: unknown;
  cover_image: string | null;
  cover_image_url: string | null;
  cover_image_asset: string | null;
  module_view: boolean;
  cycle_view: boolean;
  issue_views_view: boolean;
  page_view: boolean;
  intake_view: boolean;
  is_time_tracking_enabled: boolean;
  is_issue_type_enabled: boolean;
  guest_view_all_features: boolean;
  archive_in: number;
  close_in: number;
  archived_at: string | null;
  deleted_at: string | null;
  timezone: string;
  project_lead: string | null;
  default_assignee: string | null;
  default_state: string | null;
  estimate: string | null;
  workspace: string;
  created_at: string;
  updated_at: string;
  created_by: string;
  updated_by: string | null;
  external_source: string | null;
  external_id: string | null;
};

export type PlaneAction = "created" | "updated" | "deleted";

type PlaneEnvelope<E extends string, D> = {
  event: E;
  action: PlaneAction;
  webhook_id: string;
  workspace_id: string;
  workspace_slug: string;
  data: D;
  activity: PlaneActivity;
};

export type PlaneRawPayload =
  | PlaneEnvelope<"issue", PlaneIssueData>
  | PlaneEnvelope<"project", PlaneProjectData>
  // Not yet captured — treated as opaque until you have samples.
  | PlaneEnvelope<"cycle", Record<string, unknown>>
  | PlaneEnvelope<"module", Record<string, unknown>>
  | PlaneEnvelope<"issue_comment", Record<string, unknown>>;

export type PlaneEventName = PlaneRawPayload["event"];

// ─────────────────────────────────────────────────────────────
// Normalized shape — what the rest of your app should consume
// ─────────────────────────────────────────────────────────────

export type PlaneChange = {
  field: string;
  oldValue: unknown;
  newValue: unknown;
  /** False when old and new are deep-equal (a no-op toggle). */
  isMeaningful: boolean;
};

export type PlaneWebhookEvent = {
  /** x-plane-delivery — use as the idempotency key. */
  deliveryId: string;
  /** Verified against the shared secret. Never trust the body when false. */
  verified: boolean;
  receivedAt: number;

  event: PlaneEventName;
  action: PlaneAction;
  /** Convenience discriminants. */
  isCreate: boolean;
  isUpdate: boolean;
  isDelete: boolean;

  workspaceId: string;
  workspaceSlug: string;
  webhookId: string;

  /** body.data.id — the entity this delivery is about. */
  entityId: string;
  /** Bare project UUID, when the entity belongs to one. */
  projectId: string | null;

  actor: PlaneActor;
  /** Empty on create/delete; exactly one entry on update. */
  changes: PlaneChange[];

  /** Real client IP, from Cloudflare headers rather than the socket. */
  clientIp: string | null;

  /** Exact bytes as received — required for signature checks and replay. */
  raw: string;
  /** Full parsed payload, discriminate on `.event`. */
  payload: PlaneRawPayload;
};

export type ParseResult =
  | { ok: true; event: PlaneWebhookEvent }
  | { ok: false; reason: ParseFailure; status: number; detail?: string };

export type ParseFailure =
  | "wrong_method"
  | "missing_headers"
  | "bad_json"
  | "bad_signature"
  | "unknown_shape";

// ─────────────────────────────────────────────────────────────
// Parsing
// ─────────────────────────────────────────────────────────────

function timingSafeEqualHex(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  const ab = Buffer.from(a, "hex");
  const bb = Buffer.from(b, "hex");
  if (ab.length !== bb.length) return false;
  let diff = 0;
  for (let i = 0; i < ab.length; i++) diff |= ab[i]! ^ bb[i]!;
  return diff === 0;
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return false;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => deepEqual(v, b[i]));
  }
  if (typeof a === "object" && typeof b === "object") {
    const ak = Object.keys(a as object);
    const bk = Object.keys(b as object);
    return (
      ak.length === bk.length &&
      ak.every((k) =>
        deepEqual(
          (a as Record<string, unknown>)[k],
          (b as Record<string, unknown>)[k],
        ),
      )
    );
  }
  return false;
}

function extractProjectId(payload: PlaneRawPayload): string | null {
  if (payload.event === "project") return payload.data.id;
  const data = payload.data as { project?: unknown };
  return typeof data.project === "string" ? data.project : null;
}

/**
 * Read, verify, and normalize a Plane webhook request.
 *
 * Consumes the request body — call this once per request, before anything
 * else touches `req`.
 */
export async function parsePlaneRequest(
  req: Request,
  secret: string,
): Promise<ParseResult> {
  if (req.method !== "POST") {
    return { ok: false, reason: "wrong_method", status: 405 };
  }

  const deliveryId = req.headers.get("x-plane-delivery");
  const signature = req.headers.get("x-plane-signature");
  const eventHeader = req.headers.get("x-plane-event");

  if (!deliveryId || !signature || !eventHeader) {
    return { ok: false, reason: "missing_headers", status: 400 };
  }

  // Read ONCE. Keep the raw string — re-serializing breaks the HMAC.
  const raw = await req.text();

  const expected = new Bun.CryptoHasher("sha256", secret)
    .update(raw)
    .digest("hex");

  if (!timingSafeEqualHex(signature, expected)) {
    return { ok: false, reason: "bad_signature", status: 401 };
  }

  let payload: PlaneRawPayload;
  try {
    payload = JSON.parse(raw) as PlaneRawPayload;
  } catch (err) {
    return {
      ok: false,
      reason: "bad_json",
      status: 400,
      detail: String(err),
    };
  }

  if (
    typeof payload?.event !== "string" ||
    typeof payload?.action !== "string" ||
    typeof payload?.data !== "object" ||
    payload.data === null ||
    typeof payload?.activity !== "object" ||
    payload.activity === null
  ) {
    return { ok: false, reason: "unknown_shape", status: 422 };
  }

  const { activity, action } = payload;

  const changes: PlaneChange[] =
    activity.field === null
      ? []
      : [
          {
            field: activity.field,
            oldValue: activity.old_value,
            newValue: activity.new_value,
            isMeaningful: !deepEqual(activity.old_value, activity.new_value),
          },
        ];

  const cfIp = req.headers.get("cf-connecting-ip");
  const xff = req.headers.get("x-forwarded-for");

  return {
    ok: true,
    event: {
      deliveryId,
      verified: true,
      receivedAt: Date.now(),

      event: payload.event,
      action,
      isCreate: action === "created",
      isUpdate: action === "updated",
      isDelete: action === "deleted",

      workspaceId: payload.workspace_id,
      workspaceSlug: payload.workspace_slug,
      webhookId: payload.webhook_id,

      entityId: (payload.data as { id: string }).id,
      projectId: extractProjectId(payload),

      actor: activity.actor,
      changes,

      clientIp: cfIp ?? xff?.split(",")[0]?.trim() ?? null,

      raw,
      payload,
    },
  };
}

// ─────────────────────────────────────────────────────────────
// Field filtering — the difference between useful and unusable
// ─────────────────────────────────────────────────────────────

/** Issue fields worth announcing. Everything else is UI chrome. */
export const ISSUE_NOTIFY_FIELDS = new Set([
  "state",
  "priority",
  "assignee_ids",
  "labels",
  "name",
  "description",
  "target_date",
  "start_date",
  "parent",
  "estimate_point",
  "archived_at",
]);

/** Project fields worth announcing. */
export const PROJECT_NOTIFY_FIELDS = new Set([
  "name",
  "identifier",
  "description",
  "project_lead",
  "default_assignee",
  "network",
  "archived_at",
]);

/**
 * True when this delivery should reach Discord at all.
 * Filters out sort_order churn, view toggles, and no-op changes.
 */
export function shouldNotify(evt: PlaneWebhookEvent): boolean {
  if (evt.isCreate || evt.isDelete) return true;

  const allow =
    evt.event === "issue"
      ? ISSUE_NOTIFY_FIELDS
      : evt.event === "project"
        ? PROJECT_NOTIFY_FIELDS
        : null;

  if (!allow) return false; // unknown event type — capture, don't post

  return evt.changes.some((c) => c.isMeaningful && allow.has(c.field));
}

// ─────────────────────────────────────────────────────────────
// Narrowing helpers
// ─────────────────────────────────────────────────────────────

export function isIssueEvent(
  evt: PlaneWebhookEvent,
): evt is PlaneWebhookEvent & { payload: PlaneEnvelope<"issue", PlaneIssueData> } {
  return evt.event === "issue";
}

export function isProjectEvent(
  evt: PlaneWebhookEvent,
): evt is PlaneWebhookEvent & {
  payload: PlaneEnvelope<"project", PlaneProjectData>;
} {
  return evt.event === "project";
}

/** Build the web URL for an issue. All three parts come from the payload. */
export function issueUrl(
  baseUrl: string,
  workspaceSlug: string,
  projectId: string,
  issueId: string,
): string {
  return `${baseUrl.replace(/\/$/, "")}/${workspaceSlug}/projects/${projectId}/issues/${issueId}`;
}

/** state.group → embed sidebar colour. Don't use state.color; it collides. */
export const GROUP_COLORS: Record<PlaneStateGroup, number> = {
  backlog: 0x6b7280,
  unstarted: 0x3b82f6,
  started: 0xf59e0b,
  completed: 0x22c55e,
  cancelled: 0xef4444,
};
