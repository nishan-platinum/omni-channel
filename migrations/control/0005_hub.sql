-- M10 Omnichannel Conversation Hub — gateway slice (ADR-0011, ADR-0012).
-- Hub owns engagement truth (spec §40.5 principle 4): conversations, messages, delivery status,
-- agent presence and routing. Every table is tenant-scoped with FORCE ROW LEVEL SECURITY.
--
-- Visibility: tenant scope sees only its own rows; system scope (webhook ingest, routing and
-- delivery workers) sees all; platform (Super Admin) scope sees NOTHING — hub data is tenant
-- business data, like tenant_data.* (ADR-0005/0009).

CREATE SCHEMA IF NOT EXISTS hub;
GRANT USAGE ON SCHEMA hub TO crm_app;

CREATE OR REPLACE FUNCTION shared.rls_tenant_or_system(t uuid) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT CASE coalesce(current_setting('app.scope', true), '')
               WHEN 'system' THEN true
               WHEN 'tenant' THEN t IS NOT NULL AND t = shared.ctx_tenant_id()
               ELSE false
           END
$$;
GRANT EXECUTE ON FUNCTION shared.rls_tenant_or_system(uuid) TO crm_app;

-- Agents are tenant users (bootstrap identity, replaceable by M02).
ALTER TABLE identity.users DROP CONSTRAINT ck_users_role;
ALTER TABLE identity.users ADD CONSTRAINT ck_users_role CHECK (role IN ('super_admin', 'tenant_admin', 'agent'));
ALTER TABLE identity.users DROP CONSTRAINT ck_users_role_tenant;
ALTER TABLE identity.users ADD CONSTRAINT ck_users_role_tenant
    CHECK ((role = 'super_admin' AND tenant_id IS NULL) OR (role IN ('tenant_admin', 'agent') AND tenant_id IS NOT NULL));

-- Channel endpoints: the ONLY way inbound traffic is mapped to a tenant (WhatsApp phone_number_id,
-- voice DID, web-chat widget key). Provider payloads never carry a trusted tenant id.
CREATE TABLE hub.channel_endpoints (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    channel         text NOT NULL,
    address         text NOT NULL,
    label           text NOT NULL,
    default_skill   text NOT NULL DEFAULT 'support',
    simulated       boolean NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_channel_endpoints_channel CHECK (channel IN ('whatsapp', 'voice', 'webchat')),
    CONSTRAINT uq_channel_endpoints_address UNIQUE (channel, address)
);
CREATE INDEX ix_channel_endpoints_tenant_id ON hub.channel_endpoints (tenant_id);

CREATE TABLE hub.agents (
    user_id         uuid PRIMARY KEY REFERENCES identity.users (id) ON DELETE CASCADE,
    tenant_id       uuid NOT NULL REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    skills          text[] NOT NULL,
    max_concurrent  integer NOT NULL DEFAULT 3,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_agents_capacity CHECK (max_concurrent BETWEEN 1 AND 50),
    CONSTRAINT ck_agents_skills CHECK (cardinality(skills) BETWEEN 1 AND 20)
);
CREATE INDEX ix_agents_tenant_id ON hub.agents (tenant_id);

-- Authoritative presence (OCC-M10-R034). heartbeat_at is refreshed by live agent sessions; the
-- presence reaper marks agents whose heartbeat stopped (node crash) Offline and re-queues work.
CREATE TABLE hub.agent_presence (
    user_id         uuid PRIMARY KEY REFERENCES hub.agents (user_id) ON DELETE CASCADE,
    tenant_id       uuid NOT NULL,
    status          text NOT NULL DEFAULT 'offline',
    node_id         text,
    heartbeat_at    timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_agent_presence_status CHECK (status IN ('available', 'busy', 'away', 'wrap_up', 'offline'))
);
CREATE INDEX ix_agent_presence_tenant_status ON hub.agent_presence (tenant_id, status);

-- One conversation per interaction thread regardless of channel (OCC-M10-R014).
CREATE TABLE hub.conversations (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    endpoint_id     uuid NOT NULL REFERENCES hub.channel_endpoints (id) ON DELETE CASCADE,
    channel         text NOT NULL,
    customer_address text NOT NULL,
    customer_name   text,
    status          text NOT NULL DEFAULT 'queued',
    required_skill  text NOT NULL,
    assigned_agent  uuid REFERENCES hub.agents (user_id) ON DELETE SET NULL,
    last_seq        bigint NOT NULL DEFAULT 0,
    queued_at       timestamptz,
    assigned_at     timestamptz,
    closed_at       timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_conversations_status CHECK (status IN ('queued', 'assigned', 'closed'))
);
-- No CHECK tying status to assigned_agent: deleting an agent user sets assigned_agent NULL, and the
-- presence reaper re-queues such orphaned 'assigned' conversations.
-- At most one open conversation per customer per endpoint.
CREATE UNIQUE INDEX uq_conversations_open_thread ON hub.conversations (endpoint_id, customer_address) WHERE status <> 'closed';
CREATE INDEX ix_conversations_queue ON hub.conversations (tenant_id, required_skill, queued_at) WHERE status = 'queued';
CREATE INDEX ix_conversations_agent ON hub.conversations (assigned_agent) WHERE status = 'assigned';
CREATE INDEX ix_conversations_tenant_created ON hub.conversations (tenant_id, created_at DESC);

-- Append-only message/event store (OCC-M10-R015) with a gap-free per-conversation sequence
-- (ordering, OCC-M10-R033) and an idempotency key (duplicate webhooks / client retries).
CREATE TABLE hub.messages (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    conversation_id uuid NOT NULL REFERENCES hub.conversations (id) ON DELETE CASCADE,
    seq             bigint NOT NULL,
    direction       text NOT NULL,
    kind            text NOT NULL,
    sender_type     text NOT NULL,
    sender_id       uuid,
    body            text NOT NULL,
    provider_message_id text,
    idempotency_key text NOT NULL,
    delivery_status text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_messages_direction CHECK (direction IN ('inbound', 'outbound', 'event')),
    CONSTRAINT ck_messages_kind CHECK (kind IN ('text', 'call_event', 'system')),
    CONSTRAINT ck_messages_sender CHECK (sender_type IN ('customer', 'agent', 'system')),
    CONSTRAINT ck_messages_delivery CHECK (delivery_status IS NULL OR delivery_status IN ('queued', 'sent', 'delivered', 'read', 'failed')),
    CONSTRAINT uq_messages_seq UNIQUE (conversation_id, seq),
    CONSTRAINT uq_messages_idempotency UNIQUE (tenant_id, idempotency_key)
);
CREATE UNIQUE INDEX uq_messages_provider_id ON hub.messages (provider_message_id) WHERE provider_message_id IS NOT NULL;

-- Delivery status transitions are append-only events (OCC-M10-R030); messages.delivery_status is
-- the derived current value.
CREATE TABLE hub.message_status_events (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    message_id      uuid NOT NULL REFERENCES hub.messages (id) ON DELETE CASCADE,
    status          text NOT NULL,
    detail          text,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_message_status_events_status CHECK (status IN ('queued', 'sent', 'delivered', 'read', 'failed'))
);
CREATE INDEX ix_message_status_events_message ON hub.message_status_events (message_id, occurred_at);

-- Transactional outbound queue: written in the same transaction as the outbound message, drained
-- FIFO per conversation by the delivery worker with a retry ladder (OCC-M10-R032/R033).
CREATE TABLE hub.outbound_queue (
    message_id      uuid PRIMARY KEY REFERENCES hub.messages (id) ON DELETE CASCADE,
    tenant_id       uuid NOT NULL,
    conversation_id uuid NOT NULL,
    seq             bigint NOT NULL,
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_outbound_queue_due ON hub.outbound_queue (next_attempt_at);
CREATE INDEX ix_outbound_queue_conversation ON hub.outbound_queue (conversation_id, seq);

-- Web-chat customer sessions (minimal M08 slice). Only the token hash is stored.
CREATE TABLE hub.customer_sessions (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    endpoint_id     uuid NOT NULL REFERENCES hub.channel_endpoints (id) ON DELETE CASCADE,
    visitor_id      text NOT NULL,
    display_name    text NOT NULL,
    token_hash      bytea NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    CONSTRAINT uq_customer_sessions_token UNIQUE (token_hash)
);
CREATE INDEX ix_customer_sessions_tenant ON hub.customer_sessions (tenant_id);

-- SIMULATED provider log: what the simulated WhatsApp BSP "sent" and received, shown on the
-- simulator console. Not a production table.
CREATE TABLE hub.sim_provider_log (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    channel         text NOT NULL,
    direction       text NOT NULL,
    summary         text NOT NULL,
    payload         jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_sim_provider_log_tenant ON hub.sim_provider_log (tenant_id, created_at DESC);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['channel_endpoints', 'agents', 'agent_presence', 'conversations', 'messages',
                             'message_status_events', 'outbound_queue', 'customer_sessions', 'sim_provider_log']
    LOOP
        EXECUTE format('ALTER TABLE hub.%I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE hub.%I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format('CREATE POLICY %I ON hub.%I USING (shared.rls_tenant_or_system(tenant_id)) WITH CHECK (shared.rls_tenant_or_system(tenant_id))',
                       t || '_isolation', t);
    END LOOP;
END
$$;

GRANT SELECT, INSERT, UPDATE, DELETE ON hub.channel_endpoints, hub.agents, hub.agent_presence, hub.conversations,
    hub.outbound_queue, hub.customer_sessions TO crm_app;
-- Append-only stores: no DELETE for the runtime role (purge goes through tenant cascade as owner).
GRANT SELECT, INSERT, UPDATE ON hub.messages TO crm_app;
GRANT SELECT, INSERT ON hub.message_status_events, hub.sim_provider_log TO crm_app;

-- Message content is immutable (OCC-M10-R015: corrections are new events, never overwrites). Only
-- the derived delivery fields may change.
CREATE OR REPLACE FUNCTION hub.forbid_message_rewrite() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.body IS DISTINCT FROM OLD.body OR NEW.seq <> OLD.seq OR NEW.conversation_id <> OLD.conversation_id
       OR NEW.tenant_id <> OLD.tenant_id OR NEW.direction <> OLD.direction OR NEW.sender_type <> OLD.sender_type
       OR NEW.sender_id IS DISTINCT FROM OLD.sender_id OR NEW.idempotency_key <> OLD.idempotency_key
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'hub.messages is append-only: only delivery fields may change';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER trg_messages_append_only BEFORE UPDATE ON hub.messages
    FOR EACH ROW EXECUTE FUNCTION hub.forbid_message_rewrite();
