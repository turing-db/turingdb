# Turing Cypher parser

`CypherLexer` turns the query text into tokens on demand. `CypherGrammar` parses them by
recursive descent, one function per grammar rule, and builds the AST. `CypherParser::parse`
runs both, then rejects expressions nested deeper than `MAX_EXPRESSION_DEPTH`.
