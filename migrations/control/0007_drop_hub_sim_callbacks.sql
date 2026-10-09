-- The in-process WhatsApp simulator (and its scheduled receipts) was replaced by the WhatsApp
-- Cloud API adapter + the external fake-meta server (ADR-0013), which posts receipts as real
-- webhooks. The table from 0006 is no longer used.
DROP TABLE IF EXISTS hub.sim_callbacks;
