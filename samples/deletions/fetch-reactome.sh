# Helper for running tests on reactome locally (we don't have reactome on CI currently)
mkdir -p .turing/graphs
ln -s ~/.turing/graphs/reactome ~/turingdb/samples/deletions/.turing/data/reactome
