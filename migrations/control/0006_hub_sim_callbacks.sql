-- SIMULATED provider callbacks (ADR-0012): delivery receipts the fake WhatsApp BSP will "post" later.
-- Stored in the database so that ANY node processes them when due — like a real provider whose
-- webhooks reach whichever node the load balancer picks — instead of a timer inside the node that
-- happened to deliver the message (lost if that node dies). Not a production table.

CREATE TABLE hub.sim_callbacks (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    channel         text NOT NULL,
    signature       text NOT NULL,
    body            bytea NOT NULL,
    due_at          timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_sim_callbacks_due ON hub.sim_callbacks (due_at);

ALTER TABLE hub.sim_callbacks ENABLE ROW LEVEL SECURITY;
ALTER TABLE hub.sim_callbacks FORCE ROW LEVEL SECURITY;
CREATE POLICY sim_callbacks_isolation ON hub.sim_callbacks
    USING (shared.rls_tenant_or_system(tenant_id)) WITH CHECK (shared.rls_tenant_or_system(tenant_id));
-- UPDATE is needed for SELECT … FOR UPDATE SKIP LOCKED (claiming due rows).
GRANT SELECT, INSERT, UPDATE, DELETE ON hub.sim_callbacks TO crm_app;
