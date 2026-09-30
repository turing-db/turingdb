func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "displayName") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.constant("STAT1" : !storage.string)
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %4 = db.constant("STAT1 [" : !storage.string)
  %5 = db.starts_with %1, %4 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %6 = db.or %3, %5 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %7 = db.filter(%6, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %8 = db.get_node_properties(%7, "speciesName") : (!db.column<!storage.node_id>) -> !db.column<none>
  %9 = db.constant("Homo sapiens" : !storage.string)
  %10 = db.eq %8, %9 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %11 = db.filter(%10, {%7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %12, %13, %14 = db.explore_paths(%11, {}) both hops 1 to 5 {
  ^bb0(%arg0: !db.column<!storage.node_id>, %arg1: !db.column<!storage.edge_id>, %arg2: !db.column<!storage.node_id>):
    %35 = db.get_node_properties(%arg0, "schemaClass") : (!db.column<!storage.node_id>) -> !db.column<none>
    %36 = db.constant("" : !storage.string)
    %37 = db.constant("" : !storage.nullable<none>)
    %38 = db.neq %35, %37 : (!db.column<none>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
    %39 = db.case({%38}, {%35}, %36) : (!db.column<!storage.bool>, !db.column<none>, !db.column<!storage.string>) -> !db.column<none>
    %40 = db.constant(["ProteinDrug" : !storage.string, "ChemicalDrug" : !storage.string])
    %41 = db.in %39, %40 : (!db.column<none>, !db.column<!storage.list<!storage.string>>) -> !db.column<!storage.bool>
    %42 = db.not %41 : (!db.column<!storage.bool>) -> !db.column<!storage.bool>
    %43 = db.get_node_properties(%arg2, "speciesName") : (!db.column<!storage.node_id>) -> !db.column<none>
    %44 = db.constant("" : !storage.string)
    %45 = db.constant("" : !storage.nullable<none>)
    %46 = db.neq %43, %45 : (!db.column<none>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
    %47 = db.case({%46}, {%43}, %44) : (!db.column<!storage.bool>, !db.column<none>, !db.column<!storage.string>) -> !db.column<none>
    %48 = db.constant(["" : !storage.string, "Homo sapiens" : !storage.string])
    %49 = db.in %47, %48 : (!db.column<none>, !db.column<!storage.list<!storage.string>>) -> !db.column<!storage.bool>
    %50 = db.get_node_properties(%arg2, "schemaClass") : (!db.column<!storage.node_id>) -> !db.column<none>
    %51 = db.constant("" : !storage.string)
    %52 = db.constant("" : !storage.nullable<none>)
    %53 = db.neq %50, %52 : (!db.column<none>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
    %54 = db.case({%53}, {%50}, %51) : (!db.column<!storage.bool>, !db.column<none>, !db.column<!storage.string>) -> !db.column<none>
    %55 = db.constant(["Summation" : !storage.string, "InstanceEdit" : !storage.string, "Species" : !storage.string, "Disease" : !storage.string, "Compartment" : !storage.string, "ReferenceDatabase" : !storage.string, "DatabaseIdentifier" : !storage.string, "EntityFunctionalStatus" : !storage.string, "LiteratureReference" : !storage.string, "Publication" : !storage.string, "UndirectedInteraction" : !storage.string, "ReferenceGeneProduct" : !storage.string, "SimpleEntity" : !storage.string, "GO_MolecularFunction" : !storage.string, "FailedReaction" : !storage.string, "ReviewStatus" : !storage.string, "Deleted" : !storage.string, "DeletedInstance" : !storage.string, "DeletedControlledVocabulary" : !storage.string, "UpdateTracker" : !storage.string, "Release" : !storage.string])
    %56 = db.in %54, %55 : (!db.column<none>, !db.column<!storage.list<!storage.string>>) -> !db.column<!storage.bool>
    %57 = db.not %56 : (!db.column<!storage.bool>) -> !db.column<!storage.bool>
    %58 = db.get_node_properties(%arg2, "displayName") : (!db.column<!storage.node_id>) -> !db.column<none>
    %59 = db.constant("" : !storage.string)
    %60 = db.constant("" : !storage.nullable<none>)
    %61 = db.neq %58, %60 : (!db.column<none>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
    %62 = db.case({%61}, {%58}, %59) : (!db.column<!storage.bool>, !db.column<none>, !db.column<!storage.string>) -> !db.column<none>
    %63 = db.constant(["ATP [cytosol]" : !storage.string, "ADP [cytosol]" : !storage.string, "AMP [cytosol]" : !storage.string, "H2O [cytosol]" : !storage.string, "AdoMet [cytosol]" : !storage.string, "AdoHcy [cytosol]" : !storage.string, "gain_of_function via non_conservative_missense_variant" : !storage.string, "lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]" : !storage.string])
    %64 = db.in %62, %63 : (!db.column<none>, !db.column<!storage.list<!storage.string>>) -> !db.column<!storage.bool>
    %65 = db.not %64 : (!db.column<!storage.bool>) -> !db.column<!storage.bool>
    %66 = db.and %42, %49 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
    %67 = db.and %66, %57 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
    %68 = db.and %67, %65 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
    db.yield %68 : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %15 = db.get_node_properties(%13, "schemaClass") : (!db.column<!storage.node_id>) -> !db.column<none>
  %16 = db.constant(["ProteinDrug" : !storage.string, "ChemicalDrug" : !storage.string])
  %17 = db.in %15, %16 : (!db.column<none>, !db.column<!storage.list<!storage.string>>) -> !db.column<!storage.bool>
  %18:3 = db.filter(%17, {%13, %14, %12}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %19 = db.path_length(%18#1) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  %20 = db.to_nullable %19 : (!db.column<ui64>) -> !db.column<!storage.nullable<ui64>>
  %21 = db.expand_path(%18#1, %18#2) kind nodes : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.node_id>>
  %22 = db.to_nullable %21 : (!db.column<!storage.list<!storage.node_id>>) -> !db.column<!storage.nullable<!storage.list<!storage.node_id>>>
  %23 = db.list_comprehension(%22, {%18#0, %18#1, %18#2}) {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<ui64>, %arg2: !db.column<!storage.node_id>, %arg3: !db.column<!storage.path_ref>, %arg4: !db.column<!storage.node_id>):
    %35 = db.get_node_properties(%arg0, "displayName") : (!db.column<none>) -> !db.column<none>
    db.comprehension_yield %arg1, %35 : !db.column<none>
  } : (!db.column<!storage.nullable<!storage.list<!storage.node_id>>>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<none>
  %24 = db.make_list(%20, %23) : (!db.column<!storage.nullable<ui64>>, !db.column<none>) -> !db.column<!storage.list<none>>
  %25:3 = db.collect(%18#0, %24, %20) keys 1 aggregates [min] : (!db.column<!storage.node_id>, !db.column<!storage.list<none>>, !db.column<!storage.nullable<ui64>>) -> (!db.column<!storage.node_id>, !db.column<!storage.list<!storage.list<none>>>, !db.column<none>)
  %26 = db.list_comprehension(%25#1, {%25#1, %25#2, %25#0}) {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<ui64>, %arg2: !db.column<!storage.list<!storage.list<none>>>, %arg3: !db.column<none>, %arg4: !db.column<!storage.node_id>):
    %35 = db.constant(0 : i64)
    %36 = db.list_index %arg0, %35 : (!db.column<none>, !db.column<i64>) -> !db.column<none>
    %37 = db.eq %36, %arg3 : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
    %38:2 = db.filter(%37, {%arg0, %arg1}) : (!db.column<!storage.bool>, !db.column<none>, !db.column<ui64>) -> (!db.column<none>, !db.column<ui64>)
    db.comprehension_yield %38#1, %38#0 : !db.column<none>
  } : (!db.column<!storage.list<!storage.list<none>>>, !db.column<!storage.list<!storage.list<none>>>, !db.column<none>, !db.column<!storage.node_id>) -> !db.column<none>
  %27 = db.get_node_properties(%25#0, "schemaClass") : (!db.column<!storage.node_id>) -> !db.column<none>
  %28 = db.get_node_properties(%25#0, "displayName") : (!db.column<!storage.node_id>) -> !db.column<none>
  %29 = db.get_node_properties(%25#0, "stId") : (!db.column<!storage.node_id>) -> !db.column<none>
  %30 = db.size(%26) : (!db.column<none>) -> !db.column<none>
  %31 = db.head(%26) : (!db.column<none>) -> !db.column<none>
  %32 = db.constant(1 : i64)
  %33 = db.list_index %31, %32 : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  %34:6 = db.sort(%27, %28, %29, %25#2, %30, %33) keys [3, 1] ascending [true, true] : (!db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>)
  db.output(%34#0, %34#1, %34#2, %34#3, %34#4, %34#5) names ["t.schemaClass", "t.displayName", "t.stId", "distance", "shortestPaths", "path"] : !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>
  return
}
