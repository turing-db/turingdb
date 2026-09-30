func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_properties(%0, "displayName")
  %2 = db.constant("STAT1")
  %3 = db.eq %1, %2
  %4 = db.constant("STAT1 [")
  %5 = db.starts_with %1, %4
  %6 = db.or %3, %5
  %7 = db.filter(%6, {%0})
  %8 = db.get_node_properties(%7, "speciesName")
  %9 = db.constant("Homo sapiens")
  %10 = db.eq %8, %9
  %11 = db.filter(%10, {%7})
  %12, %13, %14 = db.explore_paths(%11, {}) both hops 1 to 5 {
  ^bb0(%arg0, %arg1, %arg2):
    %35 = db.get_node_properties(%arg0, "schemaClass")
    %36 = db.constant("")
    %37 = db.constant("")
    %38 = db.neq %35, %37
    %39 = db.case({%38}, {%35}, %36)
    %40 = db.constant(["ProteinDrug", "ChemicalDrug"])
    %41 = db.in %39, %40
    %42 = db.not %41
    %43 = db.get_node_properties(%arg2, "speciesName")
    %44 = db.constant("")
    %45 = db.constant("")
    %46 = db.neq %43, %45
    %47 = db.case({%46}, {%43}, %44)
    %48 = db.constant(["", "Homo sapiens"])
    %49 = db.in %47, %48
    %50 = db.get_node_properties(%arg2, "schemaClass")
    %51 = db.constant("")
    %52 = db.constant("")
    %53 = db.neq %50, %52
    %54 = db.case({%53}, {%50}, %51)
    %55 = db.constant(["Summation", "InstanceEdit", "Species", "Disease", "Compartment", "ReferenceDatabase", "DatabaseIdentifier", "EntityFunctionalStatus", "LiteratureReference", "Publication", "UndirectedInteraction", "ReferenceGeneProduct", "SimpleEntity", "GO_MolecularFunction", "FailedReaction", "ReviewStatus", "Deleted", "DeletedInstance", "DeletedControlledVocabulary", "UpdateTracker", "Release"])
    %56 = db.in %54, %55
    %57 = db.not %56
    %58 = db.get_node_properties(%arg2, "displayName")
    %59 = db.constant("")
    %60 = db.constant("")
    %61 = db.neq %58, %60
    %62 = db.case({%61}, {%58}, %59)
    %63 = db.constant(["ATP [cytosol]", "ADP [cytosol]", "AMP [cytosol]", "H2O [cytosol]", "AdoMet [cytosol]", "AdoHcy [cytosol]", "gain_of_function via non_conservative_missense_variant", "lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]"])
    %64 = db.in %62, %63
    %65 = db.not %64
    %66 = db.and %42, %49
    %67 = db.and %66, %57
    %68 = db.and %67, %65
    db.yield %68
  }
  %15 = db.get_node_properties(%13, "schemaClass")
  %16 = db.constant(["ProteinDrug", "ChemicalDrug"])
  %17 = db.in %15, %16
  %18:3 = db.filter(%17, {%13, %14, %12})
  %19 = db.path_length(%18#1)
  %20 = db.to_nullable %19
  %21 = db.expand_path(%18#1, %18#2) kind nodes
  %22 = db.to_nullable %21
  %23 = db.list_comprehension(%22, {%18#0, %18#1, %18#2}) {
  ^bb0(%arg0, %arg1, %arg2, %arg3, %arg4):
    %35 = db.get_node_properties(%arg0, "displayName")
    db.comprehension_yield %arg1, %35
  }
  %24 = db.make_list(%20, %23)
  %25:3 = db.collect(%18#0, %24, %20) keys 1 aggregates [min]
  %26 = db.list_comprehension(%25#1, {%25#1, %25#2, %25#0}) {
  ^bb0(%arg0, %arg1, %arg2, %arg3, %arg4):
    %35 = db.constant(0)
    %36 = db.list_index %arg0, %35
    %37 = db.eq %36, %arg3
    %38:2 = db.filter(%37, {%arg0, %arg1})
    db.comprehension_yield %38#1, %38#0
  }
  %27 = db.get_node_properties(%25#0, "schemaClass")
  %28 = db.get_node_properties(%25#0, "displayName")
  %29 = db.get_node_properties(%25#0, "stId")
  %30 = db.size(%26)
  %31 = db.head(%26)
  %32 = db.constant(1)
  %33 = db.list_index %31, %32
  %34:6 = db.sort(%27, %28, %29, %25#2, %30, %33) keys [3, 1] ascending [true, true]
  db.output(%34#0, %34#1, %34#2, %34#3, %34#4, %34#5) names ["t.schemaClass", "t.displayName", "t.stId", "distance", "shortestPaths", "path"]
  return
}
