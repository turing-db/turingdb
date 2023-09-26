%skeleton "lalr1.cc"
%require "3.0"
%defines
%define api.parser.class { YParser }

%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.namespace { db }

%code requires
{

#include <string>

namespace db {
class YScanner;
class ASTContext;
class QueryCommand;
}

}

%code top
{

#include "YScanner.h"
#include "parser.hpp"

#include "ASTContext.h"
#include "QueryCommand.h"

#include "BioLog.h"

using namespace db;
using namespace Log;

static db::YParser::symbol_type yylex(db::YScanner& scanner) {
    return scanner.get_next_token();
}

}

%lex-param { db::YScanner& scanner }
%parse-param { db::YScanner& scanner }
%parse-param { db::ASTContext* ctxt }

%define api.token.prefix {TOKEN_}

%token UNKNOWN_TOKEN

%token LIST
%token DATABASES
%token OPEN
%token CLOSE

%token <std::string> STRING_CONSTANT

%token END

%type<db::QueryCommand*> list_cmd
%type<db::QueryCommand*> open_cmd

%start query_unit

%%

query_unit: list_cmd { ctxt->setRoot($1); }
          | open_cmd { ctxt->setRoot($1); }
          | error    { ctxt->setError(true); }
          ;

list_cmd: LIST DATABASES {
                            auto cmd = ListCommand::create(ctxt); 
                            cmd->setSubType(ListCommand::LCOM_DATABASES);
                            $$ = cmd;
                         }
        ;

open_cmd : OPEN STRING_CONSTANT {
                                    auto cmd = OpenCommand::create(ctxt, $2);
                                    $$ = cmd;
                                }
         ;

%%

void db::YParser::error(const std::string &message) {
}
