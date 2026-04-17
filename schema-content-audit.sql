-- ═══════════════════════════════════════════════════════════
-- NEG RAG — Schema & Content Audit
-- Run: docker exec -i postgres psql -U postgres -d neg_rag < schema-content-audit.sql
-- Purpose: verify actual DB schema + row content vs what CLAUDE.md and migrations claim.
-- Companion to .planning/ontology-audit.sql (entity-graph side).
-- ═══════════════════════════════════════════════════════════

\pset pager off
\pset linestyle unicode
\pset border 2

\echo ═══════════════════════════════════════════════════════════
\echo CHECK 1: Database identity + version
\echo ═══════════════════════════════════════════════════════════
SELECT current_database() AS db, version();

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 2: Installed extensions (expected: pg_trgm; maybe pg_search later)
\echo ═══════════════════════════════════════════════════════════
SELECT extname, extversion FROM pg_extension ORDER BY extname;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 3: All tables in public schema with row estimate
\echo Expected: companies, vendors, projects, documents, ingestion_runs,
\echo           entities, entity_company_links, entity_document_links
\echo ═══════════════════════════════════════════════════════════
SELECT
  c.relname AS table_name,
  n_live_tup AS est_rows,
  pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY n_live_tup DESC NULLS LAST;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 4: Exact row counts (authoritative)
\echo ═══════════════════════════════════════════════════════════
SELECT 'companies'              AS table_name, count(*) FROM companies
UNION ALL SELECT 'vendors',              count(*) FROM vendors
UNION ALL SELECT 'projects',             count(*) FROM projects
UNION ALL SELECT 'documents',            count(*) FROM documents
UNION ALL SELECT 'ingestion_runs',       count(*) FROM ingestion_runs
UNION ALL SELECT 'entities',             count(*) FROM entities
UNION ALL SELECT 'entity_company_links', count(*) FROM entity_company_links
UNION ALL SELECT 'entity_document_links',count(*) FROM entity_document_links
ORDER BY count DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 5: documents columns (verify schema matches 01-base-schema.sql)
\echo Look for: search_tsv GENERATED, content TEXT, metadata JSONB, area, sub_folder
\echo ═══════════════════════════════════════════════════════════
SELECT
  column_name,
  data_type,
  COALESCE(character_maximum_length::text, '') AS max_len,
  is_nullable,
  column_default,
  is_generated
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'documents'
ORDER BY ordinal_position;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 6: companies columns (verify company-tree migration applied)
\echo Look for: parent_id, company_level, address, hrb, status, sold_date, aliases
\echo ═══════════════════════════════════════════════════════════
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'companies'
ORDER BY ordinal_position;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 7: Indexes on documents (look for search_tsv GIN, area, sub_folder)
\echo ═══════════════════════════════════════════════════════════
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'documents'
ORDER BY indexname;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 8: Functions in public schema (expect get_company_subtree, update_updated_at)
\echo ═══════════════════════════════════════════════════════════
SELECT p.proname,
       pg_get_function_arguments(p.oid) AS args,
       pg_get_function_result(p.oid)    AS returns,
       l.lanname                        AS language
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE n.nspname = 'public'
ORDER BY p.proname;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 9: Views (expect v_company_tree, v_company_doc_counts,
\echo                 v_documents_enriched, v_entity_company_overview,
\echo                 v_entity_document_overview)
\echo ═══════════════════════════════════════════════════════════
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'public'
ORDER BY table_name;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 10: Company counts by level + status
\echo Expected totals match CLAUDE.md "36 companies"
\echo ═══════════════════════════════════════════════════════════
SELECT company_level,
       status,
       count(*)                               AS companies,
       string_agg(company_code, ', ' ORDER BY company_code) AS codes
FROM companies
GROUP BY company_level, status
ORDER BY company_level, status;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 11: Company subtree of NEG_AG (should return 35 codes per ontology audit)
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS subtree_size,
       string_agg(company_code, ', ' ORDER BY company_code) AS all_codes
FROM get_company_subtree('NEG_AG');

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 12: Orphan companies (parent_id NULL but not holding level or NEG_AG)
\echo ═══════════════════════════════════════════════════════════
SELECT company_code, company_name, company_level, status
FROM companies
WHERE parent_id IS NULL AND company_code <> 'NEG_AG'
ORDER BY company_level, company_code;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 13: Documents by type (compare to CLAUDE.md claim of 443 docs)
\echo ═══════════════════════════════════════════════════════════
SELECT type, type_german, count(*) AS docs
FROM documents
GROUP BY type, type_german
ORDER BY docs DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 14: Documents per company (top 15 + unassigned)
\echo ═══════════════════════════════════════════════════════════
SELECT
  COALESCE(company_code, '(NULL)')   AS company_code,
  COALESCE(company_name, '(NULL)')   AS company_name,
  count(*)                            AS docs
FROM documents
GROUP BY company_code, company_name
ORDER BY docs DESC
LIMIT 15;

\echo
\echo -- Docs with NO company_code at all:
SELECT count(*) AS docs_with_null_company FROM documents WHERE company_code IS NULL OR company_code = '';

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 15: qdrant_collection values on documents
\echo (S-02 writes NEG_v4. Older rows may have NEG_v3 or NULL — flag drift)
\echo ═══════════════════════════════════════════════════════════
SELECT
  COALESCE(qdrant_collection, '(NULL)') AS qdrant_collection,
  count(*)                              AS docs
FROM documents
GROUP BY qdrant_collection
ORDER BY docs DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 16: Content health on documents
\echo Look for empty/tiny content, extraction_failed, low ocr_confidence
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE content IS NULL OR content = '')  AS empty_content,
  count(*) FILTER (WHERE length(content) < 200)            AS tiny_content_lt200,
  count(*) FILTER (WHERE extraction_failed = true)         AS extraction_failed,
  count(*) FILTER (WHERE needs_review = true)              AS needs_review,
  count(*) FILTER (WHERE ocr_confidence IS NOT NULL AND ocr_confidence < 0.70) AS low_ocr_conf,
  count(*) FILTER (WHERE confidence IS NOT NULL AND confidence < 0.70)         AS low_class_conf,
  count(*)                                                 AS total_docs
FROM documents;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 17: Date coverage on docs
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE doc_date IS NULL) AS null_doc_date,
  min(doc_date) AS earliest,
  max(doc_date) AS latest,
  count(DISTINCT date_trunc('year', doc_date)) AS distinct_years
FROM documents;

SELECT date_trunc('year', doc_date)::date AS year, count(*) AS docs
FROM documents WHERE doc_date IS NOT NULL
GROUP BY 1 ORDER BY 1;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 18: area + sub_folder populated? (added in 04-scale-readiness.sql)
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE area IS NULL OR area = '')             AS null_area,
  count(*) FILTER (WHERE sub_folder IS NULL OR sub_folder = '') AS null_sub_folder,
  count(DISTINCT area)       AS distinct_area,
  count(DISTINCT sub_folder) AS distinct_sub_folder,
  count(*)                   AS total
FROM documents;

SELECT area, count(*) AS docs
FROM documents
GROUP BY area
ORDER BY docs DESC
LIMIT 10;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 19: vendors table — filled out? known vs unknown
\echo ═══════════════════════════════════════════════════════════
SELECT
  vendor_is_known,
  count(*) AS vendors,
  count(*) FILTER (WHERE vendor_account_code IS NOT NULL) AS with_account_code
FROM vendors
GROUP BY vendor_is_known;

SELECT vendor_name, vendor_account_code, vendor_is_known
FROM vendors
ORDER BY vendor_name
LIMIT 30;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 20: projects table
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS total_projects FROM projects;

SELECT p.project_code, p.project_name, c.company_code AS linked_company
FROM projects p
LEFT JOIN companies c ON c.id = p.company_id
ORDER BY p.project_code
LIMIT 30;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 21: ingestion_runs — success/error/partial breakdown per workflow
\echo ═══════════════════════════════════════════════════════════
SELECT
  source_workflow,
  action,
  status,
  count(*) AS runs,
  min(created_at) AS first_run,
  max(created_at) AS last_run
FROM ingestion_runs
GROUP BY source_workflow, action, status
ORDER BY runs DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 22: Any docs with duplicate internal_number? (should be 0 — UNIQUE)
\echo ═══════════════════════════════════════════════════════════
SELECT internal_number, count(*)
FROM documents
GROUP BY internal_number
HAVING count(*) > 1
ORDER BY count(*) DESC
LIMIT 10;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 23: Spot-check — known doc DOC-2015-0404 (Wiechmann loan, slim-25 A1)
\echo Verify content length + metadata fields are populated
\echo ═══════════════════════════════════════════════════════════
SELECT
  internal_number,
  type, type_german,
  company_code, company_name,
  vendor_name,
  amount, currency,
  doc_date,
  length(content) AS content_chars,
  length(search_tsv::text) AS tsv_chars,
  qdrant_collection,
  area, sub_folder,
  needs_review, extraction_failed
FROM documents
WHERE internal_number = 'DOC-2015-0404';

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 24: Amounts — sanity distribution on invoices
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE amount IS NULL) AS null_amount,
  count(*) FILTER (WHERE amount = 0)     AS zero_amount,
  min(amount)  AS min_amt,
  max(amount)  AS max_amt,
  avg(amount)::numeric(14,2) AS avg_amt,
  count(*)     AS invoices
FROM documents
WHERE type IN ('Invoice','OutgoingInvoice');

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 25: table_blocks JSONB — how many docs carry tables?
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE jsonb_array_length(table_blocks) > 0) AS docs_with_tables,
  count(*) FILTER (WHERE jsonb_array_length(table_blocks) = 0) AS docs_without_tables,
  max(jsonb_array_length(table_blocks)) AS max_tables_per_doc,
  sum(jsonb_array_length(table_blocks)) AS total_tables
FROM documents;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 26: FK integrity — docs linked to companies/projects/vendors
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) AS total_docs,
  count(company_id)  AS with_company_fk,
  count(project_id)  AS with_project_fk,
  count(vendor_id)   AS with_vendor_fk,
  count(*) FILTER (WHERE company_id IS NULL AND company_code IS NOT NULL AND company_code <> '') AS missing_company_fk_despite_code
FROM documents;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 27: metadata JSONB — sample of keys (agent relies on camelCase)
\echo Shows the distinct top-level keys present in documents.metadata
\echo ═══════════════════════════════════════════════════════════
SELECT key, count(*) AS docs
FROM documents, jsonb_object_keys(metadata) AS key
GROUP BY key
ORDER BY docs DESC
LIMIT 40;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 28: Recent ingestions (last 20 runs — what's alive)
\echo ═══════════════════════════════════════════════════════════
SELECT id, document_id, internal_number, source_workflow, action, status, created_at
FROM ingestion_runs
ORDER BY created_at DESC
LIMIT 20;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo DONE. Cross-check vs audit report §3 (DB schema), §7.4 (ingestion state),
\echo and §10 (discrepancies with CLAUDE.md). Flags to watch:
\echo   - CHECK 10: total companies = 36 expected
\echo   - CHECK 11: subtree 35 (one extra is NEG_AG itself if included)
\echo   - CHECK 13: total 443 docs expected
\echo   - CHECK 15: any NEG_v3 stragglers
\echo   - CHECK 22: duplicate internal_numbers should be 0
\echo   - CHECK 26: missing_company_fk_despite_code > 0 indicates broken upserts
\echo ═══════════════════════════════════════════════════════════
