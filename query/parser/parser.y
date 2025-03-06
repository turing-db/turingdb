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
class EntityPattern;
class TypeConstraint;
class ExprConstraint;
class NameConstraint;
class VarExpr;
class VarList;
class Expr;
class SelectProjection;
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
#include "NameConstraint.h"
#include "ExprConstraint.h"
#include "SelectProjection.h"

using namespace db;

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
%token ARROW
%token POINT

// Keywords
%token SELECT
%token FROM
%token CREATE
%token LIST
%token GRAPH
%token LOAD
%token EXPLAIN

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

%type<db::QueryCommand*> query_unit
%type<db::QueryCommand*> cmd

%type<db::QueryCommand*> select_cmd
%type<db::SelectProjection*> select_fields
%type<db::SelectField*> select_field
%type<db::FromTarget*> from_target
%type<db::PathPattern*> path_pattern
%type<db::EntityPattern*> node_pattern
%type<db::EntityPattern*> edge_pattern
%type<db::EntityPattern*> entity_pattern
%type<db::NameConstraint*> name_constraint
%type<db::TypeConstraint*> type_constraint
%type<db::ExprConstraint*> expr_constraint
%type<db::VarExpr*> entity_var

%type<db::QueryCommand*> create_graph_cmd

%type<db::QueryCommand*> list_graph_cmd

%type<db::QueryCommand*> load_graph_cmd

%type<db::QueryCommand*> explain_cmd

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

query_unit: cmd { $$ = $1; }
          | error { ctxt->setError(true); }
          ;
          
cmd: select_cmd { ctxt->setRoot($1); }
   | create_graph_cmd { ctxt->setRoot($1); }
   | list_graph_cmd { ctxt->setRoot($1); }
   | load_graph_cmd { ctxt->setRoot($1); }
   | explain_cmd { ctxt->setRoot($1); }
   ;

select_cmd: SELECT select_fields FROM from_target {
                                                       auto cmd = SelectCommand::create(ctxt); 
                                                       cmd->setProjection($2);
                                                       cmd->addFromTarget($4);
                                                       $$ = cmd;
                                                  }
          ;

select_field: STAR {
                        auto field = SelectField::create(ctxt);
                        field->setAll(true);
                        $$ = field;
                    }
             | ID { 
                    auto field = SelectField::create(ctxt);
                    field->setName($1);
                    $$ = field;
                  }
             | ID POINT ID {
                                auto field = SelectField::create(ctxt);
                                field->setName($1);
                                field->setMemberName($3);
                                $$ = field;
                           }
             ;

select_fields: select_fields COMMA select_field {
                                                    $1->addField($3);
                                                    $$ = $1;
                                                }
             | select_field {
                                auto proj = SelectProjection::create(ctxt);
                                proj->addField($1);
                                $$ = proj;
                            }
             ;

from_target: path_pattern {
                              auto target = FromTarget::create(ctxt, $1);
                              $$ = target;
                          }
           ;

path_pattern: node_pattern
            {
                auto pattern = PathPattern::create(ctxt);
                pattern->addElement($1);
                $$ = pattern;
            }
            | path_pattern MINUS MINUS node_pattern
            {
                auto edge = EntityPattern::create(ctxt, nullptr, nullptr, nullptr, nullptr);
                $1->addElement(edge);
                $1->addElement($4);
                $$ = $1;
            }
            | path_pattern MINUS edge_pattern MINUS node_pattern 
            {
                $1->addElement($3);
                $1->addElement($5);
                $$ = $1;
            }
            ;

node_pattern: OPAR entity_pattern CPAR { $$ = $2; }
            | entity_pattern { $$ = $1; }
            ;

edge_pattern: entity_pattern { $$ = $1; }
            ;

entity_pattern: entity_var type_constraint name_constraint expr_constraint
              {
                  $$ = EntityPattern::create(ctxt, $1, $2, $3, $4);
              }
              | entity_var type_constraint name_constraint
              { $$ = EntityPattern::create(ctxt, $1, $2, $3, nullptr); }
              | entity_var type_constraint expr_constraint 
              { $$ = EntityPattern::create(ctxt, $1, $2, nullptr, $3); }
              | entity_var type_constraint 
              { $$ = EntityPattern::create(ctxt, $1, $2, nullptr, nullptr); }
              | entity_var name_constraint
              { $$ = EntityPattern::create(ctxt, $1, nullptr, $2, nullptr); }
              | entity_var expr_constraint
              { $$ = EntityPattern::create(ctxt, $1, nullptr, nullptr, $2); }
              | entity_var
              { $$ = EntityPattern::create(ctxt, $1, nullptr, nullptr, nullptr); }
              | type_constraint name_constraint expr_constraint
              { $$ = EntityPattern::create(ctxt, nullptr, $1, $2, $3); }
              | type_constraint name_constraint
              { $$ = EntityPattern::create(ctxt, nullptr, $1, $2, nullptr); }
              | type_constraint expr_constraint
              { $$ = EntityPattern::create(ctxt, nullptr, $1, nullptr, $2); }
              | type_constraint
              { $$ = EntityPattern::create(ctxt, nullptr, $1, nullptr, nullptr); }
              ;

entity_var: ID COLON { $$ = VarExpr::create(ctxt, $1); }
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

name_constraint: OSBRACK ID CSBRACK { $$ = NameConstraint::create(ctxt, $2); }
               | OSBRACK STRING_CONSTANT CSBRACK { $$ = NameConstraint::create(ctxt, $2); }
               ;

expr_constraint: OBRACK expr CBRACK { $$ = ExprConstraint::create(ctxt, $2); }
               ;

// CREATE GRAPH
create_graph_cmd: CREATE GRAPH ID { $$ = CreateGraphCommand::create(ctxt, $3); }
             ;

// LIST GRAPH
list_graph_cmd: LIST GRAPH { $$ = ListGraphCommand::create(ctxt); }
           ;

// LOAD GRAPH
load_graph_cmd: LOAD GRAPH ID { $$ = LoadGraphCommand::create(ctxt, $3); }
           ;

// EXPLAIN
explain_cmd: EXPLAIN cmd {
                            auto explain = ExplainCommand::create(ctxt, ctxt->getRoot());
                            $$ = explain;
                         }

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

void db::YParser::error(const std::string& message) {
}
