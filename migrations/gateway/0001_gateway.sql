-- Bake-off gateway (ADR-0014): single-tenant store for the gateway slice. Separate database from the
-- CRM control plane; one tenant and one shared bearer token by contract, so no RLS scopes here.
CREATE SCHEMA IF NOT EXISTS gw;

-- Platform fixture as last loaded (one row). Every node reads routing rules from here, so a
-- reload on one node reaches all nodes.
CREATE TABLE gw.config (
    id          int PRIMARY KEY CHECK (id = 1),
    fixture     jsonb NOT NULL,
    version     bigint NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Gateway nodes and their heartbeat; a node silent for a few seconds is dead and its agent
-- sessions no longer count as connected.
CREATE TABLE gw.nodes (
    node_id       text PRIMARY KEY,
    started_at    timestamptz NOT NULL DEFAULT now(),
    heartbeat_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE gw.agents (
    id                text PRIMARY KEY,
    skills            text[] NOT NULL DEFAULT '{}',
    in_fixture        boolean NOT NULL DEFAULT false,
    -- the agent's own `status` flag; "available" = this AND a live session
    status_available  boolean NOT NULL DEFAULT false,
    -- status to restore when the same session resumes within the grace period
    resume_available  boolean NOT NULL DEFAULT false,
    idle_since        timestamptz NOT NULL DEFAULT now(),
    -- last live session went away (disconnect, node death); conversations re-route 30 s later
    disconnected_at   timestamptz,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- One row per live agent WebSocket (keyed by connection, not session: a resumed session may hold
-- a new connection on another node while the old one is still being torn down).
CREATE TABLE gw.agent_sessions (
    connection_id text PRIMARY KEY,
    session_id    text NOT NULL,
    agent_id      text NOT NULL REFERENCES gw.agents(id),
    node_id       text NOT NULL,
    connected_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX agent_sessions_agent ON gw.agent_sessions (agent_id);
CREATE INDEX agent_sessions_node ON gw.agent_sessions (node_id);

CREATE TABLE gw.conversations (
    id              text PRIMARY KEY,                    -- ULID
    channel         text NOT NULL CHECK (channel IN ('whatsapp', 'sip')),
    customer        text NOT NULL,
    skill           text NOT NULL,
    status          text NOT NULL CHECK (status IN ('queued', 'assigned', 'closed')),
    assigned_agent  text,
    last_seq        bigint NOT NULL DEFAULT 0,
    queued_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    closed_at       timestamptz,
    -- queued ⇒ no agent; assigned ⇒ an agent; closed keeps the agent that handled it
    CHECK (status = 'closed' OR (status = 'assigned') = (assigned_agent IS NOT NULL))
);
-- One open conversation per customer per channel.
CREATE UNIQUE INDEX conversations_open_customer ON gw.conversations (channel, customer) WHERE status <> 'closed';
CREATE INDEX conversations_customer ON gw.conversations (customer) WHERE status <> 'closed';
CREATE INDEX conversations_queue ON gw.conversations (skill, queued_at, id) WHERE status = 'queued';
CREATE INDEX conversations_agent ON gw.conversations (assigned_agent) WHERE status = 'assigned';

-- Append-only, gap-free per conversation (seq allocated by bumping conversations.last_seq under
-- the row lock in the same transaction).
CREATE TABLE gw.messages (
    conversation_id  text NOT NULL REFERENCES gw.conversations(id),
    seq              bigint NOT NULL CHECK (seq >= 1),
    message_id       text NOT NULL UNIQUE,
    channel          text NOT NULL,
    direction        text NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    actor_kind       text NOT NULL CHECK (actor_kind IN ('customer', 'agent', 'system')),
    actor_id         text NOT NULL,
    kind             text NOT NULL CHECK (kind IN ('text', 'call_event', 'assignment', 'disposition')),
    body             jsonb NOT NULL,
    received_at      timestamptz NOT NULL,
    external_id      text,
    dedup_key        text,
    client_ref       text,
    PRIMARY KEY (conversation_id, seq)
);
CREATE UNIQUE INDEX messages_dedup ON gw.messages (dedup_key) WHERE dedup_key IS NOT NULL;
CREATE UNIQUE INDEX messages_client_ref ON gw.messages (actor_kind, actor_id, client_ref) WHERE client_ref IS NOT NULL;

CREATE FUNCTION gw.forbid_message_rewrite() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'gw.messages is append-only';
END $$;
CREATE TRIGGER messages_append_only BEFORE UPDATE OR DELETE ON gw.messages
    FOR EACH ROW EXECUTE FUNCTION gw.forbid_message_rewrite();
