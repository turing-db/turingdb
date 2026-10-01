// Run on reactome (2,978,202 nodes, 11,537,843 edges) with seed STAT1:
// 126 ms, 27,141 nodes visited, 15 drugs found.
//
// distance 4: heparin [cytosol]
// distance 5: baricitinib [cytosol]
//             delgocitinib [cytosol]
//             luteolin [cytosol]
//             peginterferon alfa-2a [extracellular region]
//             peginterferon alfa-2b [extracellular region]
//             peginterferon beta-1a [extracellular region]
//             recombinant IFNA2a [extracellular region]
//             recombinant IFNA2b [extracellular region]
//             recombinant IFNB1a [extracellular region]
//             recombinant IFNB1b [extracellular region]
//             ripretinib [cytosol]
//             ruxolitinib [cytosol]
//             sunitinib [cytosol]
//             tofacitinib [cytosol]
MATCH (seed)
WHERE seed.speciesName = 'Homo sapiens'
  AND (seed.displayName = 'STAT1' OR seed.displayName STARTS WITH 'STAT1 [')
MATCH p = (seed)((a)-[]-(b)
    WHERE NOT a:Drug
      AND coalesce(b.speciesName, '') IN ['', 'Homo sapiens']
      AND NOT coalesce(b.schemaClass, '') IN ['Summation', 'InstanceEdit', 'Species', 'Disease', 'Compartment', 'ReferenceDatabase', 'DatabaseIdentifier', 'EntityFunctionalStatus', 'LiteratureReference', 'Publication', 'UndirectedInteraction', 'ReferenceGeneProduct', 'SimpleEntity', 'GO_MolecularFunction', 'FailedReaction', 'ReviewStatus', 'Deleted', 'DeletedInstance', 'DeletedControlledVocabulary', 'UpdateTracker', 'Release']
      AND NOT coalesce(b.displayName, '') IN ['ATP [cytosol]', 'ADP [cytosol]', 'AMP [cytosol]', 'H2O [cytosol]', 'AdoMet [cytosol]', 'AdoHcy [cytosol]', 'gain_of_function via non_conservative_missense_variant', 'lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]']
){1,5}(t)
WHERE t:Drug
WITH t, length(p) AS hops, [n IN nodes(p) | n.displayName] AS path
WITH t, min(hops) AS distance, collect([hops, path]) AS candidates
WITH t, distance, [c IN candidates WHERE c[0] = distance] AS shortest
RETURN t.schemaClass, t.displayName, t.stId, distance, size(shortest) AS shortestPaths, head(shortest)[1] AS path
ORDER BY distance, t.displayName
