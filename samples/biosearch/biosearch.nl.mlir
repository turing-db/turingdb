func.func @main() {
  %0 = nl.sort_buffer keys [3, 1] ascending [true, true]
  %1 = nl.constant(0)
  %2 = nl.constant(1)
  %3 = nl.get_property_type("stId")
  %4 = nl.collect_buffer keys 1 aggregates [min]
  %5 = nl.constant("")
  %6 = nl.constant("")
  %7 = nl.constant(["Summation", "InstanceEdit", "Species", "Disease", "Compartment", "ReferenceDatabase", "DatabaseIdentifier", "EntityFunctionalStatus", "LiteratureReference", "Publication", "UndirectedInteraction", "ReferenceGeneProduct", "SimpleEntity", "GO_MolecularFunction", "FailedReaction", "ReviewStatus", "Deleted", "DeletedInstance", "DeletedControlledVocabulary", "UpdateTracker", "Release"])
  %8 = nl.constant("")
  %9 = nl.constant("")
  %10 = nl.constant(["ATP [cytosol]", "ADP [cytosol]", "AMP [cytosol]", "H2O [cytosol]", "AdoMet [cytosol]", "AdoHcy [cytosol]", "gain_of_function via non_conservative_missense_variant", "lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]"])
  %11 = nl.constant(0)
  %12 = nl.get_property_type("schemaClass")
  %13 = nl.constant("Homo sapiens")
  %14 = nl.constant("")
  %15 = nl.constant("")
  %16 = nl.constant(["", "Homo sapiens"])
  %17 = nl.get_property_type("speciesName")
  %18 = nl.constant("STAT1")
  %19 = nl.constant("STAT1 [")
  %20 = nl.get_property_type("displayName")
  %21 = nl.scan_nodes()
  nl.for %arg0 in %21 {
    %24 = nl.get_node_properties(%arg0, %20)
    %25 = nl.eq %24, %18
    %26 = nl.starts_with %24, %19
    %27 = nl.or %25, %26
    %28 = nl.filter %27, (%arg0)
    %29 = nl.get_node_properties(%28, %17)
    %30 = nl.eq %29, %13
    %31 = nl.filter %30, (%28)
    %32 = nl.explore_paths(%31, {}) both hops 1 to 5 end_labels ["Drug"] {
    ^bb0(%arg1, %arg2, %arg3):
      %33 = nl.get_node_label_set(%arg1)
      %34 = nl.check_label_constraint(%33, ["Drug"])
      %35 = nl.not %34
      %36 = nl.get_node_properties(%arg3, %17)
      %37 = nl.neq %36, %15
      %38 = nl.broadcast_constant %14, %37
      %39 = nl.case({%37}, {%36}, %38)
      %40 = nl.in %39, %16
      %41 = nl.get_node_properties(%arg3, %12)
      %42 = nl.neq %41, %6
      %43 = nl.broadcast_constant %5, %42
      %44 = nl.case({%42}, {%41}, %43)
      %45 = nl.in %44, %7
      %46 = nl.not %45
      %47 = nl.get_node_properties(%arg3, %20)
      %48 = nl.neq %47, %9
      %49 = nl.broadcast_constant %8, %48
      %50 = nl.case({%48}, {%47}, %49)
      %51 = nl.in %50, %10
      %52 = nl.not %51
      %53 = nl.and %35, %40
      %54 = nl.and %53, %46
      %55 = nl.and %54, %52
      nl.yield %55
    }
    nl.for %arg1, %arg2, %arg3 in %32 {
      %33 = nl.path_length(%arg3)
      %34 = nl.to_nullable %33
      %35 = nl.expand_path(%arg3, %arg1) kind nodes
      %36 = nl.to_nullable %35
      %37 = nl.list_comprehension(%36, {%arg2, %arg3, %arg1}) {
      ^bb0(%arg4, %arg5, %arg6, %arg7, %arg8):
        %39 = nl.get_node_properties(%arg4, %20)
        nl.comprehension_yield %arg5, %39
      }
      %38 = nl.make_list(%34, %37)
      nl.collect_update %4, (%arg2, %38, %34)
    }
  }
  %22 = nl.collect(%4)
  nl.for %arg0, %arg1, %arg2 in %22 {
    %24 = nl.list_comprehension(%arg1, {%arg1, %arg2, %arg0}) {
    ^bb0(%arg3, %arg4, %arg5, %arg6, %arg7):
      %31 = nl.list_index %arg3, %11
      %32 = nl.eq %31, %arg6
      %33:2 = nl.filter %32, (%arg3, %arg4)
      nl.comprehension_yield %33#1, %33#0
    }
    %25 = nl.get_node_properties(%arg0, %12)
    %26 = nl.get_node_properties(%arg0, %20)
    %27 = nl.get_node_properties(%arg0, %3)
    %28 = nl.size %24
    %29 = nl.list_index %24, %1
    %30 = nl.list_index %29, %2
    nl.sort_collect %0, (%25, %26, %27, %arg2, %28, %30)
  }
  %23 = nl.sort(%0)
  nl.for %arg0, %arg1, %arg2, %arg3, %arg4, %arg5 in %23 {
    nl.output(%arg0, %arg1, %arg2, %arg3, %arg4, %arg5) names ["t.schemaClass", "t.displayName", "t.stId", "distance", "shortestPaths", "path"]
  }
  return
}
