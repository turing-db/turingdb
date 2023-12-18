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
class SelectField;
class FromTarget;
class PathPattern;
class EdgePattern;
class NodePattern;
class EntityPattern;
class TypeConstraint;
class ExprConstraint;
class VarList;
class Expr;
}

}

%code top
{

#include "YScanner.h"
#include "parser.hpp"

#include "ASTContext.h"
#include "QueryCommand.h"
#include "SelectField.h"
#include "FromTarget.h"
#include "PathPattern.h"
#include "Expr.h"
#include "TypeConstraint.h"
#include "ExprConstraint.h"

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

%token<std::string> ID
%token COMMA
%token COLON
%token STAR
%token OBRACK
%token CBRACK
%token OPAR
%token CPAR
%token OSBRACK
%token CSBRACK

// Keywords
%token LIST
%token DATABASES
%token OPEN
%token CLOSE
%token SELECT
%token FROM

// Operators
%token PLUS
%token MINUS
%token GREATER
%token GREATER_EQUAL
%token LOWER
%token LOWER_EQUAL
%token OR
%token AND
%token EQUAL
%token NOT_EQUAL
%token LIKE
%token NOT

// Constants
%token <std::string> STRING_CONSTANT
%token <std::string> INT_CONSTANT
%token <std::string> DECIMAL_CONSTANT

%token END

%type<db::QueryCommand*> list_cmd
%type<db::QueryCommand*> open_cmd
%type<db::QueryCommand*> select_cmd
%type<db::SelectField*> select_fields
%type<db::FromTarget*> from_target
%type<db::PathPattern*> path_pattern
%type<db::EdgePattern*> edge_pattern
%type<db::NodePattern*> node_pattern
%type<db::EntityPattern*> entity_pattern
%type<db::TypeConstraint*> type_constraint
%type<db::ExprConstraint*> expr_constraint
%type<db::Expr*> expr
%type<db::Expr*> or_expr
%type<db::Expr*> and_expr
%type<db::Expr*> equal_expr
%type<db::Expr*> greater_expr
%type<db::Expr*> lower_expr
%type<db::Expr*> add_expr
%type<db::Expr*> mult_expr
%type<db::Expr*> like_expr
%type<db::Expr*> unary_expr
%type<db::Expr*> simple_expr

%start query_unit

%%

query_unit: list_cmd { ctxt->setRoot($1); }
          | open_cmd { ctxt->setRoot($1); }
          | select_cmd { ctxt->setRoot($1); }
          | error    { ctxt->setError(true); }
          ;

select_cmd: SELECT select_fields FROM from_target {
                                                       auto cmd = SelectCommand::create(ctxt); 
                                                       cmd->addSelectField($2);
                                                       cmd->addFromTarget($4);
                                                       $$ = cmd;
                                                  }
          ;

select_fields: STAR {
                        auto field = SelectField::create(ctxt);
                        field->setAll(true);
                        $$ = field;
                    }
             ;

from_target: path_pattern {
                              auto target = FromTarget::create(ctxt, $1);
                              $$ = target;
                          }
           ;

path_pattern: node_pattern                           {
                                                         auto pattern = PathPattern::create(ctxt);
                                                         pattern->setOrigin($1);
                                                         $$ = pattern;
                                                     }
            | path_pattern edge_pattern node_pattern {
                                                         auto elem = PathElement::create(ctxt, $2, $3);
                                                         $1->addElement(elem);
                                                         $$ = $1;
                                                     }
            ;

node_pattern: entity_pattern { $$ = NodePattern::create(ctxt, $1); }
            ;

edge_pattern: MINUS OPAR entity_pattern CPAR MINUS { $$ = EdgePattern::create(ctxt, $3); }
            | MINUS OPAR CPAR MINUS                { $$ = nullptr; }
            ;

entity_pattern: type_constraint COLON ID expr_constraint {
                                                             VarExpr* var = VarExpr::create(ctxt, $3);
                                                             $$ = EntityPattern::create(ctxt, var, $1, $4);
                                                         }
              | COLON ID expr_constraint                 {
                                                             VarExpr* var = VarExpr::create(ctxt, $2);
                                                             $$ = EntityPattern::create(ctxt, var, nullptr, $3);
                                                         }
              | type_constraint expr_constraint    {
                                                       $$ = EntityPattern::create(ctxt, nullptr, $1, $2);
                                                   }
              | type_constraint COLON ID           {
                                                       VarExpr* var = VarExpr::create(ctxt, $3);
                                                       $$ = EntityPattern::create(ctxt, var, $1, nullptr);
                                                   }
              | COLON ID                           {
                                                       VarExpr* var = VarExpr::create(ctxt, $2);
                                                       $$ = EntityPattern::create(ctxt, var, nullptr, nullptr);
                                                   }
              | type_constraint                    { $$ = EntityPattern::create(ctxt, nullptr, $1, nullptr); }
              ;

type_constraint: type_constraint COMMA ID {
                                              $1->addType(VarExpr::create(ctxt, $3));
                                              $$ = $1;
                                          }
               | ID                       {
                                              auto constr = TypeConstraint::create(ctxt);
                                              constr->addType(VarExpr::create(ctxt, $1));
                                              $$ = constr;
                                          }
               ;

expr_constraint: OBRACK expr CBRACK { $$ = ExprConstraint::create(ctxt, $2); }
               ;

// list command
list_cmd: LIST DATABASES {
                            auto cmd = ListCommand::create(ctxt); 
                            cmd->setSubType(ListCommand::LCOM_DATABASES);
                            $$ = cmd;
                         }
        ;

// open command
open_cmd : OPEN STRING_CONSTANT {
                                    auto cmd = OpenCommand::create(ctxt, $2);
                                    $$ = cmd;
                                }
         ;

// Expressions
expr: or_expr { $$ = nullptr; }
    ;

or_expr: and_expr OR or_expr { $$ = nullptr; }
       | and_expr            { $$ = $1; }

and_expr: equal_expr AND and_expr { $$ = nullptr; }
        | equal_expr              { $$ = $1; }
        ;

equal_expr: greater_expr EQUAL equal_expr     { $$ = nullptr; }
          | greater_expr NOT_EQUAL equal_expr { $$ = nullptr; }
          | greater_expr                      { $$ = $1; }
          ;

greater_expr: lower_expr GREATER greater_expr       { $$ = nullptr; }
            | lower_expr GREATER_EQUAL greater_expr { $$ = nullptr; } 
            | lower_expr                            { $$ = $1; }
            ;

lower_expr: add_expr LOWER lower_expr       { $$ = nullptr; }
          | add_expr LOWER_EQUAL lower_expr { $$ = nullptr; }
          | add_expr                        { $$ = $1; }
          ;

add_expr: mult_expr PLUS add_expr { $$ = nullptr; }
        | mult_expr               { $$ = $1; }
        ;

mult_expr: like_expr STAR mult_expr { $$ = nullptr; }
         | like_expr                { $$ = $1; }
         ;

like_expr: ID LIKE STRING_CONSTANT { $$ = nullptr; }
         | unary_expr              { $$ = $1; }
         ;

unary_expr: NOT simple_expr { $$ = nullptr; }
          | simple_expr     { $$ = $1; }
          ;

simple_expr: OPAR expr CPAR   { $$ = $2; }
           | ID               { $$ = nullptr; }
           | STRING_CONSTANT  { $$ = nullptr; }
           | INT_CONSTANT     { $$ = nullptr; }
           | DECIMAL_CONSTANT { $$ = nullptr; }
           ;

%%

void db::YParser::error(const std::string &message) {
}
