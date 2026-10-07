-- REFERENCE FIXTURES (development + test). Prototype plans standing in for M19 and provisioning
-- templates for OCC-M01-R013. These are NOT specification-defined commercial plans. No PII.

INSERT INTO tenantadm.plans (id, code, name, tier, entitlements, quotas, soft_threshold, status) VALUES
('01920000-0000-7000-8000-000000000001', 'ref-standard', 'Reference Standard (shared PostgreSQL, RLS)', 'standard',
 '["module.crm_contacts","module.crm_sales","module.service_ticketing","module.activities","module.customer_portal","module.reporting","channel.voice","channel.sms","channel.email","channel.webchat"]',
 '{"users":50,"numbers":20,"channels":4,"volume_month":100000,"api_requests_per_minute":600,"campaign_sends_per_hour":5000,"report_query_cost_per_hour":2000,"storage_gb":50,"bpm_executions_per_hour":1000,"emails_month":50000,"ai_tokens_month":0}',
 0.8000, 'active'),
('01920000-0000-7000-8000-000000000002', 'ref-premium', 'Reference Premium (schema-per-tenant PostgreSQL)', 'premium',
 '["module.crm_contacts","module.crm_sales","module.service_ticketing","module.activities","module.customer_portal","module.reporting","module.contact_centre","module.flow_builder","module.documents","module.automated_marketing","channel.voice","channel.sms","channel.email","channel.webchat","channel.whatsapp","channel.social","channel.video","ai.copilot","apps.marketplace"]',
 '{"users":250,"numbers":100,"channels":7,"volume_month":1000000,"api_requests_per_minute":3000,"campaign_sends_per_hour":50000,"report_query_cost_per_hour":20000,"storage_gb":500,"bpm_executions_per_hour":10000,"emails_month":500000,"ai_tokens_month":5000000}',
 0.8000, 'active'),
('01920000-0000-7000-8000-000000000003', 'ref-regulated', 'Reference Regulated (dedicated database, residency pinned)', 'regulated',
 '["module.crm_contacts","module.crm_sales","module.service_ticketing","module.activities","module.customer_portal","module.reporting","module.contact_centre","module.flow_builder","module.documents","module.automated_marketing","module.workforce","module.projects","channel.voice","channel.sms","channel.email","channel.webchat","channel.whatsapp","channel.social","channel.video","ai.copilot","ai.conversational","apps.marketplace"]',
 '{"users":1000,"numbers":500,"channels":7,"volume_month":5000000,"api_requests_per_minute":6000,"campaign_sends_per_hour":100000,"report_query_cost_per_hour":50000,"storage_gb":2000,"bpm_executions_per_hour":50000,"emails_month":2000000,"ai_tokens_month":20000000}',
 0.8000, 'active'),
('01920000-0000-7000-8000-000000000009', 'ref-legacy', 'Reference Legacy (retired — cannot be selected)', 'standard',
 '["module.crm_contacts"]',
 '{"users":5,"numbers":0,"channels":1,"volume_month":1000,"api_requests_per_minute":60,"campaign_sends_per_hour":0,"report_query_cost_per_hour":100,"storage_gb":1,"bpm_executions_per_hour":0,"emails_month":100,"ai_tokens_month":0}',
 0.8000, 'retired');

INSERT INTO tenantadm.provisioning_templates (id, code, name, description, features, config_defaults, packs) VALUES
('01920000-0000-7000-8000-000000000101', 'tpl-contact-centre', 'Contact centre programme',
 'Service-led programme: cases, contact centre, voice/SMS/email/web chat/WhatsApp, reporting.',
 '["module.crm_contacts","module.service_ticketing","module.activities","module.contact_centre","module.reporting","channel.voice","channel.sms","channel.email","channel.webchat","channel.whatsapp"]',
 '{"locale.timezone":"Asia/Kuala_Lumpur","locale.currency":"MYR","locale.default_language":"en","locale.allowed_languages":["en","ms"],"locale.date_format":"DD-MMM-YYYY","locale.number_format":"1,234.56"}',
 '["roles_teams:contact-centre","bpm:case-routing","reports:contact-centre-standard","sla:contact-centre-matrix","dropdowns:contact-centre"]'),
('01920000-0000-7000-8000-000000000102', 'tpl-sales-crm', 'Sales & marketing CRM',
 'Sales-led programme: accounts, pipeline, activities, automated marketing, email.',
 '["module.crm_contacts","module.crm_sales","module.activities","module.automated_marketing","module.reporting","channel.email"]',
 '{"locale.timezone":"Asia/Kuala_Lumpur","locale.currency":"MYR","locale.default_language":"en","locale.allowed_languages":["en","ms"]}',
 '["roles_teams:sales","bpm:lead-assignment","reports:sales-pipeline","sla:none","dropdowns:sales"]'),
('01920000-0000-7000-8000-000000000103', 'tpl-minimal', 'Minimal',
 'Contacts only; everything else switched on later by the Tenant Admin within the plan.',
 '["module.crm_contacts"]',
 '{"locale.default_language":"en"}',
 '["roles_teams:default"]');
