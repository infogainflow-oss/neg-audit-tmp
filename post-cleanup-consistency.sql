-- ═══════════════════════════════════════════════════════════
-- NEG RAG — Post-Cleanup Consistency Audit
-- After deleting 126 stale NEG_v2/NEG_v3 docs.
-- Goal: verify everything is internally consistent, no orphans,
--       no broken FKs, no dangling links.
-- ═══════════════════════════════════════════════════════════

\pset pager off
\pset linestyle unicode
\pset border 2

\echo ═══════════════════════════════════════════════════════════
\echo CHECK 1: Top-line counts
\echo ═══════════════════════════════════════════════════════════
SELECT 'documents'              AS t, count(*) FROM documents
UNION ALL SELECT 'entity_document_links',  count(*) FROM entity_document_links
UNION ALL SELECT 'entity_company_links',   count(*) FROM entity_company_links
UNION ALL SELECT 'ingestion_runs',         count(*) FROM ingestion_runs
UNION ALL SELECT 'entities',               count(*) FROM entities
UNION ALL SELECT 'companies',              count(*) FROM companies
UNION ALL SELECT 'vendors',                count(*) FROM vendors
UNION ALL SELECT 'projects',               count(*) FROM projects;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 2: All docs are now NEG_v4 (audit finding #19 resolved?)
\echo ═══════════════════════════════════════════════════════════
SELECT qdrant_collection, count(*)
FROM documents
GROUP BY qdrant_collection
ORDER BY count DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 3: Orphan entities (no doc link AND no company link)
\echo Update from earlier 686 — should be HIGHER after deletion
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS orphan_entities
FROM entities e
WHERE NOT EXISTS (SELECT 1 FROM entity_document_links WHERE entity_id = e.id)
  AND NOT EXISTS (SELECT 1 FROM entity_company_links  WHERE entity_id = e.id);

\echo
\echo Orphan breakdown by type
SELECT entity_type, count(*) AS orphans
FROM entities e
WHERE NOT EXISTS (SELECT 1 FROM entity_document_links WHERE entity_id = e.id)
  AND NOT EXISTS (SELECT 1 FROM entity_company_links  WHERE entity_id = e.id)
GROUP BY entity_type
ORDER BY orphans DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 4: Entities with ONLY company link (no doc link) — semi-orphans
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS entities_company_only
FROM entities e
WHERE NOT EXISTS (SELECT 1 FROM entity_document_links WHERE entity_id = e.id)
  AND     EXISTS (SELECT 1 FROM entity_company_links  WHERE entity_id = e.id);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 5: FK integrity — any dangling entity_document_links?
\echo (cascade-delete should have handled this; this is a paranoia check)
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS dangling_edl
FROM entity_document_links edl
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = edl.document_id);

SELECT count(*) AS dangling_edl_entity
FROM entity_document_links edl
WHERE NOT EXISTS (SELECT 1 FROM entities WHERE id = edl.entity_id);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 6: FK integrity — entity_company_links
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS dangling_ecl_entity
FROM entity_company_links ecl
WHERE NOT EXISTS (SELECT 1 FROM entities WHERE id = ecl.entity_id);

SELECT count(*) AS dangling_ecl_company
FROM entity_company_links ecl
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE id = ecl.company_id);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 7: ingestion_runs orphans (cascade should have caught these)
\echo ═══════════════════════════════════════════════════════════
SELECT count(*) AS dangling_ingestion_runs
FROM ingestion_runs ir
WHERE ir.document_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM documents WHERE id = ir.document_id);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 8: Documents with NULL or wrong FKs (post-cascade)
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE company_id IS NULL AND company_code IS NOT NULL AND company_code <> '') AS null_company_fk,
  count(*) FILTER (WHERE project_id IS NULL AND project_code IS NOT NULL AND project_code <> '') AS null_project_fk,
  count(*) FILTER (WHERE vendor_id IS NULL  AND vendor_name  IS NOT NULL AND vendor_name  <> '') AS null_vendor_fk,
  count(*) AS total_docs
FROM documents;

-- Any FK pointing at deleted parent?
SELECT count(*) AS docs_with_dead_company_fk
FROM documents d
WHERE d.company_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM companies WHERE id = d.company_id);

SELECT count(*) AS docs_with_dead_project_fk
FROM documents d
WHERE d.project_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM projects WHERE id = d.project_id);

SELECT count(*) AS docs_with_dead_vendor_fk
FROM documents d
WHERE d.vendor_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM vendors WHERE id = d.vendor_id);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 9: Companies with ZERO docs after cleanup (lost coverage)
\echo ═══════════════════════════════════════════════════════════
SELECT c.company_code, c.company_name, c.company_level, c.status
FROM companies c
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE company_code = c.company_code)
  AND c.status = 'active'
ORDER BY c.company_level, c.company_code;

\echo
\echo Active company doc counts (lowest first — who's underrepresented?)
SELECT c.company_code, c.company_name, count(d.id) AS docs
FROM companies c
LEFT JOIN documents d ON d.company_code = c.company_code
WHERE c.status = 'active'
GROUP BY c.company_code, c.company_name
ORDER BY docs ASC, c.company_code
LIMIT 15;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 10: Vendors / projects with ZERO referencing docs
\echo (dead rows worth pruning — but not critical)
\echo ═══════════════════════════════════════════════════════════
SELECT 'unused vendors' AS t, count(*) FROM vendors v
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE vendor_name = v.vendor_name)
UNION ALL
SELECT 'unused projects', count(*) FROM projects p
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE project_code = p.project_code);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 11: Doc count by type (post-cleanup)
\echo ═══════════════════════════════════════════════════════════
SELECT type, type_german, count(*) AS docs
FROM documents
GROUP BY type, type_german
ORDER BY docs DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 12: Doc count by company (post-cleanup)
\echo ═══════════════════════════════════════════════════════════
SELECT company_code, company_name, count(*) AS docs
FROM documents
GROUP BY company_code, company_name
ORDER BY docs DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 13: Spot-check — DOC-2015-0404 still intact
\echo ═══════════════════════════════════════════════════════════
SELECT internal_number, type, company_code, vendor_name, amount, doc_date,
       length(content) AS clen, qdrant_collection,
       (SELECT count(*) FROM entity_document_links WHERE document_id = d.id) AS entity_links
FROM documents d
WHERE internal_number = 'DOC-2015-0404';

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 14: ingestion_runs latest activity (sanity)
\echo ═══════════════════════════════════════════════════════════
SELECT source_workflow, status, count(*) AS runs,
       min(created_at)::date AS first, max(created_at)::date AS last
FROM ingestion_runs
GROUP BY source_workflow, status
ORDER BY runs DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 15: Doc → entity link strength (post-cleanup distribution)
\echo ═══════════════════════════════════════════════════════════
WITH dlinks AS (
  SELECT d.id, d.type, count(edl.id) AS n
  FROM documents d
  LEFT JOIN entity_document_links edl ON edl.document_id = d.id
  GROUP BY d.id, d.type
)
SELECT
  CASE
    WHEN n = 0 THEN '0 (miss)'
    WHEN n BETWEEN 1 AND 2 THEN '1-2 (thin)'
    WHEN n BETWEEN 3 AND 8 THEN '3-8 (healthy)'
    WHEN n BETWEEN 9 AND 20 THEN '9-20 (rich)'
    ELSE '21+ (overloaded)'
  END AS band,
  count(*) AS docs,
  round(100.0 * count(*) / (SELECT count(*) FROM documents), 1) AS pct
FROM dlinks
GROUP BY band
ORDER BY MIN(n);

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 16: Doc miss rate by type (post-cleanup, fixed query)
\echo Uses COUNT(DISTINCT) so no double-count from ontology audit bug
\echo ═══════════════════════════════════════════════════════════
SELECT
  d.type,
  count(DISTINCT d.id) AS total,
  count(DISTINCT d.id) FILTER (WHERE NOT EXISTS (
    SELECT 1 FROM entity_document_links WHERE document_id = d.id
  )) AS no_entities,
  round(100.0 * count(DISTINCT d.id) FILTER (WHERE NOT EXISTS (
    SELECT 1 FROM entity_document_links WHERE document_id = d.id
  )) / GREATEST(count(DISTINCT d.id), 1), 1) AS miss_pct
FROM documents d
GROUP BY d.type
ORDER BY no_entities DESC;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 17: search_tsv health — every doc indexed for FTS?
\echo ═══════════════════════════════════════════════════════════
SELECT
  count(*) FILTER (WHERE search_tsv IS NULL) AS null_tsv,
  count(*) FILTER (WHERE search_tsv IS NOT NULL AND length(search_tsv::text) < 50) AS tiny_tsv,
  count(*) AS total
FROM documents;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo CHECK 18: Test S-03-style FTS query end-to-end
\echo Confirms the tsvector is searchable (German tokenizer working)
\echo ═══════════════════════════════════════════════════════════
SELECT internal_number, type, company_code, doc_date,
       ts_rank_cd(search_tsv, plainto_tsquery('german','Darlehen Wiechmann')) AS rank
FROM documents
WHERE search_tsv @@ plainto_tsquery('german','Darlehen Wiechmann')
ORDER BY rank DESC
LIMIT 5;

\echo
\echo ═══════════════════════════════════════════════════════════
\echo DONE. Greenlights:
\echo   CHECK 2: only NEG_v4
\echo   CHECK 3: orphans count (compare to old 686)
\echo   CHECK 5,6,7,8: all dangling counts = 0
\echo   CHECK 13: DOC-2015-0404 intact
\echo   CHECK 18: returns DOC-2015-0404 at top
\echo Red flags:
\echo   CHECK 9: any active company with 0 docs
\echo   CHECK 10: lots of unused vendors (>50)
\echo   CHECK 16: miss_pct >50% on important types
\echo ═══════════════════════════════════════════════════════════
