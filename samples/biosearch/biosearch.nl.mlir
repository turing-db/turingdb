func.func @main() {
  %0 = nl.sort_buffer keys [3, 1] ascending [true, true]
  %1 = nl.constant(0)
  %2 = nl.constant(1)
  %3 = nl.get_property_type("stId")
  %4 = nl.collect_buffer keys 1 aggregates [min]
  %5 = nl.constant("")
  %6 = nl.constant("")
  %7 = nl.constant(["ProteinDrug", "ChemicalDrug"])
  %8 = nl.constant("")
  %9 = nl.constant("")
  %10 = nl.constant(["", "Homo sapiens"])
  %11 = nl.constant("")
  %12 = nl.constant("")
  %13 = nl.constant(["Summation", "InstanceEdit", "Species", "Disease", "Compartment", "ReferenceDatabase", "DatabaseIdentifier", "EntityFunctionalStatus", "LiteratureReference", "Publication", "UndirectedInteraction", "ReferenceGeneProduct", "SimpleEntity", "GO_MolecularFunction", "FailedReaction", "ReviewStatus", "Deleted", "DeletedInstance", "DeletedControlledVocabulary", "UpdateTracker", "Release"])
  %14 = nl.constant("")
  %15 = nl.constant("")
  %16 = nl.constant(["ATP [cytosol]", "ADP [cytosol]", "AMP [cytosol]", "H2O [cytosol]", "AdoMet [cytosol]", "AdoHcy [cytosol]", "gain_of_function via non_conservative_missense_variant", "lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]"])
  %17 = nl.constant(["ProteinDrug", "ChemicalDrug"])
  %18 = nl.constant(0)
  %19 = nl.get_property_type("schemaClass")
  %20 = nl.constant("Homo sapiens")
  %21 = nl.get_property_type("speciesName")
  %22 = nl.constant("STAT1")
  %23 = nl.constant("STAT1 [")
  %24 = nl.get_property_type("displayName")
  %25 = nl.scan_nodes()
  nl.for %arg0 in %25 {
    %28 = nl.get_node_properties(%arg0, %24)
    %29 = nl.eq %28, %22
    %30 = nl.starts_with %28, %23
    %31 = nl.or %29, %30
    %32 = nl.filter %31, (%arg0)
    %33 = nl.get_node_properties(%32, %21)
    %34 = nl.eq %33, %20
    %35 = nl.filter %34, (%32)
    %36 = nl.explore_paths(%35, {}) both hops 1 to 5 {
    ^bb0(%arg1, %arg2, %arg3):
      %37 = nl.get_node_properties(%arg1, %19)
      %38 = nl.neq %37, %6
      %39 = nl.broadcast_constant %5, %38
      %40 = nl.case({%38}, {%37}, %39)
      %41 = nl.in %40, %7
      %42 = nl.not %41
      %43 = nl.get_node_properties(%arg3, %21)
      %44 = nl.neq %43, %9
      %45 = nl.broadcast_constant %8, %44
      %46 = nl.case({%44}, {%43}, %45)
      %47 = nl.in %46, %10
      %48 = nl.get_node_properties(%arg3, %19)
      %49 = nl.neq %48, %12
      %50 = nl.broadcast_constant %11, %49
      %51 = nl.case({%49}, {%48}, %50)
      %52 = nl.in %51, %13
      %53 = nl.not %52
      %54 = nl.get_node_properties(%arg3, %24)
      %55 = nl.neq %54, %15
      %56 = nl.broadcast_constant %14, %55
      %57 = nl.case({%55}, {%54}, %56)
      %58 = nl.in %57, %16
      %59 = nl.not %58
      %60 = nl.and %42, %47
      %61 = nl.and %60, %53
      %62 = nl.and %61, %59
      nl.yield %62
    }
    nl.for %arg1, %arg2, %arg3 in %36 {
      %37 = nl.get_node_properties(%arg2, %19)
      %38 = nl.in %37, %17
      %39:3 = nl.filter %38, (%arg2, %arg3, %arg1)
      %40 = nl.path_length(%39#1)
      %41 = nl.to_nullable %40
      %42 = nl.expand_path(%39#1, %39#2) kind nodes
      %43 = nl.to_nullable %42
      %44 = nl.list_comprehension(%43, {%39#0, %39#1, %39#2}) {
      ^bb0(%arg4, %arg5, %arg6, %arg7, %arg8):
        %46 = nl.get_node_properties(%arg4, %24)
        nl.comprehension_yield %arg5, %46
      }
      %45 = nl.make_list(%41, %44)
      nl.collect_update %4, (%39#0, %45, %41)
    }
  }
  %26 = nl.collect(%4)
  nl.for %arg0, %arg1, %arg2 in %26 {
    %28 = nl.list_comprehension(%arg1, {%arg1, %arg2, %arg0}) {
    ^bb0(%arg3, %arg4, %arg5, %arg6, %arg7):
      %35 = nl.list_index %arg3, %18
      %36 = nl.eq %35, %arg6
      %37:2 = nl.filter %36, (%arg3, %arg4)
      nl.comprehension_yield %37#1, %37#0
    }
    %29 = nl.get_node_properties(%arg0, %19)
    %30 = nl.get_node_properties(%arg0, %24)
    %31 = nl.get_node_properties(%arg0, %3)
    %32 = nl.size %28
    %33 = nl.list_index %28, %1
    %34 = nl.list_index %33, %2
    nl.sort_collect %0, (%29, %30, %31, %arg2, %32, %34)
  }
  %27 = nl.sort(%0)
  nl.for %arg0, %arg1, %arg2, %arg3, %arg4, %arg5 in %27 {
    nl.output(%arg0, %arg1, %arg2, %arg3, %arg4, %arg5) names ["t.schemaClass", "t.displayName", "t.stId", "distance", "shortestPaths", "path"]
  }
  return
}
