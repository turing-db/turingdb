#pragma once

/**
 * Generated Flex class name is yyFlexLexer by default. If we want to use more flex-generated
 * classes we should name them differently. See scanner.l prefix option.
 *
 * Unfortunately the implementation relies on this trick with redefining class name
 * with a preprocessor macro. See GNU Flex manual, "Generating C++ Scanners" section
 */
#if ! defined(yyFlexLexerOnce)
#undef yyFlexLexer
#define yyFlexLexer YScanner_FlexLexer 
#include <FlexLexer.h>
#endif

// Scanner method signature is defined by this macro. Original yylex() returns int.
// Since Bison 3 uses symbol_type, we must change returned type. We also rename it
// to something sane, since you cannot overload return type.
#undef YY_DECL
#define YY_DECL db::YParser::symbol_type db::YScanner::get_next_token()

#include "parser.hpp" // this is needed for symbol_type

namespace db {

class YScanner : public yyFlexLexer {
public:
    YScanner() = default;
    virtual ~YScanner() = default;
    virtual db::YParser::symbol_type get_next_token();
};

}
