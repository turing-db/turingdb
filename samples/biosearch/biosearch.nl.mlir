func.func @main() {
  %0 = nl.sort_buffer keys [3, 1] ascending [true, true]
  %1 = nl.constant(0 : i64)
  %2 = nl.constant(1 : i64)
  %3 = nl.get_property_type("stId")
  %4 = nl.collect_buffer keys 1 aggregates [min]
  %5 = nl.constant("" : !storage.string)
  %6 = nl.constant("" : !storage.nullable<none>)
  %7 = nl.constant(["ProteinDrug" : !storage.string, "ChemicalDrug" : !storage.string])
  %8 = nl.constant("" : !storage.string)
  %9 = nl.constant("" : !storage.nullable<none>)
  %10 = nl.constant(["" : !storage.string, "Homo sapiens" : !storage.string])
  %11 = nl.constant("" : !storage.string)
  %12 = nl.constant("" : !storage.nullable<none>)
  %13 = nl.constant(["Summation" : !storage.string, "InstanceEdit" : !storage.string, "Species" : !storage.string, "Disease" : !storage.string, "Compartment" : !storage.string, "ReferenceDatabase" : !storage.string, "DatabaseIdentifier" : !storage.string, "EntityFunctionalStatus" : !storage.string, "LiteratureReference" : !storage.string, "Publication" : !storage.string, "UndirectedInteraction" : !storage.string, "ReferenceGeneProduct" : !storage.string, "SimpleEntity" : !storage.string, "GO_MolecularFunction" : !storage.string, "FailedReaction" : !storage.string, "ReviewStatus" : !storage.string, "Deleted" : !storage.string, "DeletedInstance" : !storage.string, "DeletedControlledVocabulary" : !storage.string, "UpdateTracker" : !storage.string, "Release" : !storage.string])
  %14 = nl.constant("" : !storage.string)
  %15 = nl.constant("" : !storage.nullable<none>)
  %16 = nl.constant(["ATP [cytosol]" : !storage.string, "ADP [cytosol]" : !storage.string, "AMP [cytosol]" : !storage.string, "H2O [cytosol]" : !storage.string, "AdoMet [cytosol]" : !storage.string, "AdoHcy [cytosol]" : !storage.string, "gain_of_function via non_conservative_missense_variant" : !storage.string, "lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]" : !storage.string])
  %17 = nl.constant(["ProteinDrug" : !storage.string, "ChemicalDrug" : !storage.string])
  %18 = nl.constant(0 : i64)
  %19 = nl.get_property_type("schemaClass")
  %20 = nl.constant("Homo sapiens" : !storage.string)
  %21 = nl.get_property_type("speciesName")
  %22 = nl.constant("STAT1" : !storage.string)
  %23 = nl.constant("STAT1 [" : !storage.string)
  %24 = nl.get_property_type("displayName")
  %25 = nl.scan_nodes()
  nl.for %arg0 in %25 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %28 = nl.get_node_properties(%arg0, %24) : !nl.chunk<!storage.nullable<!storage.string>>
    %29 = nl.eq %28, %22 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %30 = nl.starts_with %28, %23 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %31 = nl.or %29, %30 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
    %32 = nl.filter %31, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %33 = nl.get_node_properties(%32, %21) : !nl.chunk<!storage.nullable<!storage.string>>
    %34 = nl.eq %33, %20 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %35 = nl.filter %34, (%32) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %36 = nl.explore_paths(%35, {}) both hops 1 to 5 {
    ^bb0(%arg1: !nl.chunk<!storage.node_id>, %arg2: !nl.chunk<!storage.edge_id>, %arg3: !nl.chunk<!storage.node_id>):
      %37 = nl.get_node_properties(%arg1, %19) : !nl.chunk<!storage.nullable<!storage.string>>
      %38 = nl.neq %37, %6 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.nullable<i1>>
      %39 = nl.broadcast_constant %5, %38 : (!nl.chunk<!storage.string>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %40 = nl.case({%38}, {%37}, %39) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %41 = nl.in %40, %7 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>
      %42 = nl.not %41 : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %43 = nl.get_node_properties(%arg3, %21) : !nl.chunk<!storage.nullable<!storage.string>>
      %44 = nl.neq %43, %9 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.nullable<i1>>
      %45 = nl.broadcast_constant %8, %44 : (!nl.chunk<!storage.string>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %46 = nl.case({%44}, {%43}, %45) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %47 = nl.in %46, %10 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>
      %48 = nl.get_node_properties(%arg3, %19) : !nl.chunk<!storage.nullable<!storage.string>>
      %49 = nl.neq %48, %12 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.nullable<i1>>
      %50 = nl.broadcast_constant %11, %49 : (!nl.chunk<!storage.string>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %51 = nl.case({%49}, {%48}, %50) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %52 = nl.in %51, %13 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>
      %53 = nl.not %52 : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %54 = nl.get_node_properties(%arg3, %24) : !nl.chunk<!storage.nullable<!storage.string>>
      %55 = nl.neq %54, %15 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.nullable<i1>>
      %56 = nl.broadcast_constant %14, %55 : (!nl.chunk<!storage.string>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %57 = nl.case({%55}, {%54}, %56) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %58 = nl.in %57, %16 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>
      %59 = nl.not %58 : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %60 = nl.and %42, %47 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %61 = nl.and %60, %53 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %62 = nl.and %61, %59 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      nl.yield %62 : !nl.chunk<!storage.nullable<i1>>
    }
    nl.for %arg1, %arg2, %arg3 in %36 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.path_ref>> {
      %37 = nl.get_node_properties(%arg2, %19) : !nl.chunk<!storage.nullable<!storage.string>>
      %38 = nl.in %37, %17 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>
      %39:3 = nl.filter %38, (%arg2, %arg3, %arg1) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>)
      %40 = nl.path_length(%39#1) : (!nl.chunk<!storage.path_ref>) -> !nl.chunk<ui64>
      %41 = nl.to_nullable %40 : (!nl.chunk<ui64>) -> !nl.chunk<!storage.nullable<ui64>>
      %42 = nl.expand_path(%39#1, %39#2) kind nodes : (!nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<!storage.node_id>>
      %43 = nl.to_nullable %42 : (!nl.chunk<!storage.list<!storage.node_id>>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.node_id>>>
      %44 = nl.list_comprehension(%43, {%39#0, %39#1, %39#2}) {
      ^bb0(%arg4: !nl.chunk<!storage.node_id>, %arg5: !nl.chunk<ui64>, %arg6: !nl.chunk<!storage.node_id>, %arg7: !nl.chunk<!storage.path_ref>, %arg8: !nl.chunk<!storage.node_id>):
        %46 = nl.get_node_properties(%arg4, %24) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.comprehension_yield %arg5, %46 : !nl.chunk<!storage.nullable<!storage.string>>
      } : (!nl.chunk<!storage.nullable<!storage.list<!storage.node_id>>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>
      %45 = nl.make_list(%41, %44) : (!nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.list<!storage.list_element>>
      nl.collect_update %4, (%39#0, %45, %41) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.list<!storage.list_element>>, !nl.chunk<!storage.nullable<ui64>>
    }
  }
  %26 = nl.collect(%4) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.list<!storage.list<!storage.list_element>>>, !nl.chunk<!storage.nullable<ui64>>>
  nl.for %arg0, %arg1, %arg2 in %26 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.list<!storage.list<!storage.list_element>>>, !nl.chunk<!storage.nullable<ui64>>> {
    %28 = nl.list_comprehension(%arg1, {%arg1, %arg2, %arg0}) {
    ^bb0(%arg3: !nl.chunk<!storage.nullable<!storage.list<!storage.list_element>>>, %arg4: !nl.chunk<ui64>, %arg5: !nl.chunk<!storage.list<!storage.list<!storage.list_element>>>, %arg6: !nl.chunk<!storage.nullable<ui64>>, %arg7: !nl.chunk<!storage.node_id>):
      %35 = nl.list_index %arg3, %18 : (!nl.chunk<!storage.nullable<!storage.list<!storage.list_element>>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<!storage.list_element>>
      %36 = nl.eq %35, %arg6 : (!nl.chunk<!storage.nullable<!storage.list_element>>, !nl.chunk<!storage.nullable<ui64>>) -> !nl.chunk<!storage.nullable<i1>>
      %37:2 = nl.filter %36, (%arg3, %arg4) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.list<!storage.list_element>>>, !nl.chunk<ui64>) -> (!nl.chunk<!storage.nullable<!storage.list<!storage.list_element>>>, !nl.chunk<ui64>)
      nl.comprehension_yield %37#1, %37#0 : !nl.chunk<!storage.nullable<!storage.list<!storage.list_element>>>
    } : (!nl.chunk<!storage.list<!storage.list<!storage.list_element>>>, !nl.chunk<!storage.list<!storage.list<!storage.list_element>>>, !nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.list<!storage.list_element>>>>
    %29 = nl.get_node_properties(%arg0, %19) : !nl.chunk<!storage.nullable<!storage.string>>
    %30 = nl.get_node_properties(%arg0, %24) : !nl.chunk<!storage.nullable<!storage.string>>
    %31 = nl.get_node_properties(%arg0, %3) : !nl.chunk<!storage.nullable<!storage.string>>
    %32 = nl.size %28 : (!nl.chunk<!storage.nullable<!storage.list<!storage.list<!storage.list_element>>>>) -> !nl.chunk<!storage.nullable<i64>>
    %33 = nl.list_index %28, %1 : (!nl.chunk<!storage.nullable<!storage.list<!storage.list<!storage.list_element>>>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<!storage.list_element>>
    %34 = nl.list_index %33, %2 : (!nl.chunk<!storage.nullable<!storage.list_element>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<!storage.list_element>>
    nl.sort_collect %0, (%29, %30, %31, %arg2, %32, %34) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.list_element>>
  }
  %27 = nl.sort(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.list_element>>>
  nl.for %arg0, %arg1, %arg2, %arg3, %arg4, %arg5 in %27 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.list_element>>> {
    nl.output(%arg0, %arg1, %arg2, %arg3, %arg4, %arg5) names ["t.schemaClass", "t.displayName", "t.stId", "distance", "shortestPaths", "path"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.list_element>>
  }
  return
}
