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
  %12, %13, %14 = db.explore_paths(%11, {}) both hops 1 to 5 end_labels ["Drug"] {
  ^bb0(%arg0, %arg1, %arg2):
    %31 = db.get_node_label_set(%arg0)
    %32 = db.check_label_constraint(%31, ["Drug"])
    %33 = db.not %32
    %34 = db.get_node_properties(%arg2, "speciesName")
    %35 = db.constant("")
    %36 = db.constant("")
    %37 = db.neq %34, %36
    %38 = db.case({%37}, {%34}, %35)
    %39 = db.constant(["", "Homo sapiens"])
    %40 = db.in %38, %39
    %41 = db.get_node_properties(%arg2, "schemaClass")
    %42 = db.constant("")
    %43 = db.constant("")
    %44 = db.neq %41, %43
    %45 = db.case({%44}, {%41}, %42)
    %46 = db.constant(["Summation", "InstanceEdit", "Species", "Disease", "Compartment", "ReferenceDatabase", "DatabaseIdentifier", "EntityFunctionalStatus", "LiteratureReference", "Publication", "UndirectedInteraction", "ReferenceGeneProduct", "SimpleEntity", "GO_MolecularFunction", "FailedReaction", "ReviewStatus", "Deleted", "DeletedInstance", "DeletedControlledVocabulary", "UpdateTracker", "Release"])
    %47 = db.in %45, %46
    %48 = db.not %47
    %49 = db.get_node_properties(%arg2, "displayName")
    %50 = db.constant("")
    %51 = db.constant("")
    %52 = db.neq %49, %51
    %53 = db.case({%52}, {%49}, %50)
    %54 = db.constant(["ATP [cytosol]", "ADP [cytosol]", "AMP [cytosol]", "H2O [cytosol]", "AdoMet [cytosol]", "AdoHcy [cytosol]", "gain_of_function via non_conservative_missense_variant", "lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]"])
    %55 = db.in %53, %54
    %56 = db.not %55
    %57 = db.and %33, %40
    %58 = db.and %57, %48
    %59 = db.and %58, %56
    db.yield %59
  }
  %15 = db.path_length(%14)
  %16 = db.to_nullable %15
  %17 = db.expand_path(%14, %12) kind nodes
  %18 = db.to_nullable %17
  %19 = db.list_comprehension(%18, {%13, %14, %12}) {
  ^bb0(%arg0, %arg1, %arg2, %arg3, %arg4):
    %31 = db.get_node_properties(%arg0, "displayName")
    db.comprehension_yield %arg1, %31
  }
  %20 = db.make_list(%16, %19)
  %21:3 = db.collect(%13, %20, %16) keys 1 aggregates [min]
  %22 = db.list_comprehension(%21#1, {%21#1, %21#2, %21#0}) {
  ^bb0(%arg0, %arg1, %arg2, %arg3, %arg4):
    %31 = db.constant(0)
    %32 = db.list_index %arg0, %31
    %33 = db.eq %32, %arg3
    %34:2 = db.filter(%33, {%arg0, %arg1})
    db.comprehension_yield %34#1, %34#0
  }
  %23 = db.get_node_properties(%21#0, "schemaClass")
  %24 = db.get_node_properties(%21#0, "displayName")
  %25 = db.get_node_properties(%21#0, "stId")
  %26 = db.size(%22)
  %27 = db.head(%22)
  %28 = db.constant(1)
  %29 = db.list_index %27, %28
  %30:6 = db.sort(%23, %24, %25, %21#2, %26, %29) keys [3, 1] ascending [true, true]
  db.output(%30#0, %30#1, %30#2, %30#3, %30#4, %30#5) names ["t.schemaClass", "t.displayName", "t.stId", "distance", "shortestPaths", "path"]
  return
}
