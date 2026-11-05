# Turing Cypher parser

`CypherLexer` turns the query text into tokens on demand. `CypherGrammar` parses them by
recursive descent, one function per grammar rule, and builds the AST. `CypherParser::parse`
runs both, then rejects expressions nested deeper than `MAX_EXPRESSION_DEPTH`.

One choice needs more than the next token: whether a `|` after a label in a comprehension,
reduce or FOREACH is a label disjunction or the bar. `CypherGrammar::parseChoosingBar` parses
the construct speculatively with each reading, rewinding the tokens, the parser state and the
AST between attempts, and keeps the reading Neo4j chooses.
