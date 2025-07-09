%skeleton "lalr1.cc"
%require "3.0"
%defines
%define api.parser.class { YParser }

%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.namespace { db }
%define parse.error detailed


%code requires
{

#include <string>
#include <sstream>
#include <memory>

#include "ChangeOpType.h"
#include "CreateTarget.h"

namespace db {
class YScanner;
class ASTContext;
class QueryCommand;
class ReturnField;
class MatchTarget;
class MatchTargets;
class CreateTarget;
class CreateTargets;
class PathPattern;
class EntityPattern;
class TypeConstraint;
class ExprConstraint;
class VarExpr;
class BinExpr;
class ExprConst;
class VarList;
class Expr;
class ReturnProjection;
}

}

%locations

%code top
{

#include "YScanner.h"
#include "parser.hpp"

#include "ASTContext.h"
#include "QueryCommand.h"
#include "ChangeCommand.h"
#include "ReturnField.h"
#include "MatchTarget.h"
#include "MatchTargets.h"
#include "PathPattern.h"
#include "Expr.h"
#include "TypeConstraint.h"
#include "ExprConstraint.h"
#include "ReturnProjection.h"

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
%token YYEOF 0 "end of query" // For pretty error messages

%token<std::string> ID
%token COMMA        "','"
%token COLON        "':'"
%token STAR         "'*'"
%token OBRACK       "'{'"
%token CBRACK       "'}'"
%token OPAR         "'('"
%token CPAR         "')'"
%token OSBRACK      "'['"
%token CSBRACK      "']'"
%token ARROW        "'->'"
%token POINT        "'.'"
// Keywords
%token SELECT       "'SELECT'"
%token FROM         "'FROM'"
%token MATCH        "'MATCH'"
%token RETURN       "'RETURN'"
%token CREATE       "'CREATE'"
%token LIST         "'LIST'"
%token GRAPH        "'GRAPH'"
%token LOAD         "'LOAD'"
%token EXPLAIN      "'EXPLAIN'"
%token HISTORY      "'HISTORY'"
%token CHANGE       "'CHANGE'"
%token COMMIT       "'COMMIT'"
%token NEW          "'NEW'"
%token SUBMIT       "'SUBMIT'"
%token DELETE       "'DELETE'"
// Operators
%token PLUS         "'+'"
%token MINUS        "'-'"
%token GREATER      "'>'"
%token GREATER_EQUAL "'>='"
%token LOWER        "'<'"
%token LOWER_EQUAL  "'<='"
%token OR           "'OR'"
%token AND          "'AND'"
%token EQUAL        "'='"
%token NOT_EQUAL    "'!='"
%token STR_APPROX   "'~='"
%token LIKE         "'LIKE'"
%token NOT          "'NOT'"

// Constants
%token <std::string> STRING_CONSTANT
%token <std::string> INT_CONSTANT
%token <std::string> UINT_CONSTANT
%token <std::string> DECIMAL_CONSTANT
%token <std::string> BOOLEAN_CONSTANT
%token <std::string> BACKTICK_STRING_CONSTANT

// Constant suffixes
%token UNSIGNED_SUFFIX "'u' (Unsigned suffix)"

%type<db::QueryCommand*> query_unit
%type<db::QueryCommand*> cmd

%type<db::QueryCommand*> match_cmd
%type<db::QueryCommand*> create_cmd
%type<db::ReturnProjection*> return_fields
%type<db::ReturnField*> return_field
%type<db::MatchTarget*> match_target
%type<db::MatchTargets*> match_targets
%type<db::CreateTarget*> create_target
%type<db::CreateTargets*> create_targets
%type<db::PathPattern*> create_path_pattern
%type<db::PathPattern*> path_pattern
%type<db::EntityPattern*> create_node_pattern
%type<db::EntityPattern*> node_pattern
%type<db::EntityPattern*> edge_pattern
%type<db::EntityPattern*> entity_pattern
%type<db::EntityPattern*> known_entity_pattern
%type<db::EntityPattern*> edge_entity_pattern
%type<db::TypeConstraint*> type_constraint
%type<db::BinExpr*> prop_equals_expr
%type<db::BinExpr*> prop_approx_expr
%type<db::ExprConstraint*> prop_expr_constraint
%type<db::ExprConst*> prop_expr_constant
%type<std::string> prop_ID
%type<db::VarExpr*> entity_var

%type<db::QueryCommand*> create_graph_cmd

%type<db::QueryCommand*> list_graph_cmd

%type<db::QueryCommand*> load_graph_cmd

%type<db::QueryCommand*> explain_cmd

%type<db::QueryCommand*> history_cmd

%type<db::QueryCommand*> change_cmd

%type<db::QueryCommand*> commit_cmd

%type <db::ChangeOpType> change_subcmd

%start query_unit

%%

query_unit: cmd { $$ = $1; }
          | error { ctxt->setError(true); }
          ;
          
cmd: match_cmd { ctxt->setRoot($1); }
   | create_cmd { ctxt->setRoot($1); }
   | create_graph_cmd { ctxt->setRoot($1); }
   | list_graph_cmd { ctxt->setRoot($1); }
   | load_graph_cmd { ctxt->setRoot($1); }
   | explain_cmd { ctxt->setRoot($1); }
   | history_cmd { ctxt->setRoot($1); }
   | change_cmd { ctxt->setRoot($1); }
   | commit_cmd { ctxt->setRoot($1); }
   ;

match_cmd: MATCH match_targets RETURN return_fields {
                                                       auto cmd = MatchCommand::create(ctxt); 
                                                       cmd->setProjection($4);
                                                       cmd->setMatchTargets($2);
                                                       $$ = cmd;
                                                    }
         ;

match_targets: match_targets COMMA match_target {
                                                    $1->addTarget($3);
                                                    $$ = $1; 
                                                }
             | match_target                     {
                                                    auto targets = MatchTargets::create(ctxt);
                                                    targets->addTarget($1);
                                                    $$ = targets;
                                                }
             ;

create_targets: create_targets COMMA create_target
              {
                  $$ = $1;
                  $$->push_back($3);
              }
              | create_target
              {
                  $$ = CreateTargets::create(ctxt);
                  $$->push_back($1);
              }
              ;

create_target: create_path_pattern { $$ = CreateTarget::create(ctxt, $1); }
             ;

create_cmd: CREATE create_targets
          {
              $$ = CreateCommand::create(ctxt, $2);
          }
          ;

return_field: STAR {
                        auto field = ReturnField::create(ctxt);
                        field->setAll(true);
                        $$ = field;
                    }
             | ID { 
                    auto field = ReturnField::create(ctxt);
                    field->setName($1);
                    $$ = field;
                  }
             | ID POINT prop_ID {
                                auto field = ReturnField::create(ctxt);
                                field->setName($1);
                                field->setMemberName($3);
                                $$ = field;
                           }
             ;

return_fields: return_fields COMMA return_field {
                                                    $1->addField($3);
                                                    $$ = $1;
                                                }
             | return_field {
                                auto proj = ReturnProjection::create(ctxt);
                                proj->addField($1);
                                $$ = proj;
                            }
             ;

match_target: path_pattern {
                              auto target = MatchTarget::create(ctxt, $1);
                              $$ = target;
                          }
           ;

create_path_pattern: create_node_pattern
                   {
                       auto pattern = PathPattern::create(ctxt);
                       pattern->addElement($1);
                       $$ = pattern;
                   }
                   | create_path_pattern MINUS MINUS create_node_pattern
                   {
                       auto edge = EntityPattern::create(ctxt, nullptr, nullptr, nullptr);
                       $1->addElement(edge);
                       $1->addElement($4);
                       $$ = $1;
                   }
                   | create_path_pattern MINUS edge_pattern MINUS create_node_pattern 
                   {
                       $1->addElement($3);
                       $1->addElement($5);
                       $$ = $1;
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
                auto edge = EntityPattern::create(ctxt, nullptr, nullptr, nullptr);
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

create_node_pattern: OPAR entity_pattern CPAR { $$ = $2; }
                   | OPAR known_entity_pattern CPAR { $$ = $2; }
                   ;

node_pattern: OPAR entity_pattern CPAR { $$ = $2; }
            ;

edge_pattern: OSBRACK edge_entity_pattern CSBRACK { $$ = $2; }
            ;

edge_entity_pattern: entity_var COLON type_constraint OBRACK prop_expr_constraint CBRACK
              {
                  $$ = EntityPattern::create(ctxt, $1, $3, $5);
              }
              | entity_var COLON type_constraint
              { 
                  $$ = EntityPattern::create(ctxt, $1, $3, nullptr);
              }
              | entity_var OBRACK prop_expr_constraint CBRACK
              { $$ = EntityPattern::create(ctxt, $1, nullptr, $3); }
              | entity_var 
              { $$ = EntityPattern::create(ctxt, $1, nullptr, nullptr); }
              | COLON type_constraint OBRACK prop_expr_constraint CBRACK
              { 
                  $$ = EntityPattern::create(ctxt, nullptr, $2, $4); 
              }
              | COLON type_constraint
              { 
                  $$ = EntityPattern::create(ctxt, nullptr, $2, nullptr); 
              }
              ;

entity_pattern: entity_var COLON type_constraint OBRACK prop_expr_constraint CBRACK
              {
                  $$ = EntityPattern::create(ctxt, $1, $3, $5);
              }
              | entity_var COLON type_constraint
              { $$ = EntityPattern::create(ctxt, $1, $3, nullptr); }
              | entity_var OBRACK prop_expr_constraint CBRACK
              { $$ = EntityPattern::create(ctxt, $1, nullptr, $3); }
              | entity_var 
              { $$ = EntityPattern::create(ctxt, $1, nullptr, nullptr); }
              | COLON type_constraint OBRACK prop_expr_constraint CBRACK
              { $$ = EntityPattern::create(ctxt, nullptr, $2, $4); }
              | COLON type_constraint
              { $$ = EntityPattern::create(ctxt, nullptr, $2, nullptr); }
              ;

known_entity_pattern: entity_var COLON INT_CONSTANT
                    { $$ = EntityPattern::create(ctxt, $1, std::stoull($3)); }
                    | COLON INT_CONSTANT
                    { $$ = EntityPattern::create(ctxt, nullptr, std::stoull($2)); }
                    ;

entity_var: ID { $$ = VarExpr::create(ctxt, $1); }
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

prop_expr_constraint : prop_expr_constraint COMMA prop_equals_expr { 
                                                            $1->addExpr($3);
                                                            $$ = $1; 
                                                        }
         | prop_expr_constraint COMMA prop_approx_expr  {
                                                            $1->addExpr($3);
                                                            $$ = $1;
                                                        }
         | prop_equals_expr                             {
                                                            auto ExprConstraint = ExprConstraint::create(ctxt);
                                                            ExprConstraint->addExpr($1);
                                                            $$ = ExprConstraint;
                                                        }
         | prop_approx_expr                             {
                                                            auto ExprConstraint = ExprConstraint::create(ctxt);
                                                            ExprConstraint->addExpr($1);
                                                            $$ = ExprConstraint;
                                                        }
         ;

prop_equals_expr: prop_ID EQUAL prop_expr_constant { $$ = BinExpr::create(ctxt, VarExpr::create(ctxt,$1),$3, BinExpr::OpType::OP_EQUAL); }
          | prop_ID NOT_EQUAL prop_expr_constant { $$ = nullptr; }
          | prop_ID COLON prop_expr_constant { $$ = BinExpr::create(ctxt, VarExpr::create(ctxt,$1),$3, BinExpr::OpType::OP_EQUAL); }
          ;

prop_approx_expr: prop_ID STR_APPROX prop_expr_constant { $$ = BinExpr::create(ctxt, VarExpr::create(ctxt,$1), $3, BinExpr::OpType::OP_STR_APPROX); }
        ;

prop_expr_constant: STRING_CONSTANT  { $$ = StringExprConst::create(ctxt, $1); }
                     | DECIMAL_CONSTANT { $$ =  DoubleExprConst::create(ctxt, std::stod($1));}
                     | INT_CONSTANT UNSIGNED_SUFFIX { $$ = UInt64ExprConst::create(ctxt, std::stoull($1)); }
                     | MINUS INT_CONSTANT { $$ = Int64ExprConst::create(ctxt, std::stoll("-"+$2)); }
                     | INT_CONSTANT { $$ = Int64ExprConst::create(ctxt, std::stoll($1)); }
                     | BOOLEAN_CONSTANT { 
                                if($1[0] == 't' || $1[0] == 'T'){
                                    $$ = BoolExprConst::create(ctxt, true); 
                                }
                                else{
                                    $$ = BoolExprConst::create(ctxt, false); 
                                }
                            }
                     ;

prop_ID: ID
       | STRING_CONSTANT
       | BACKTICK_STRING_CONSTANT

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
           ;

// HISTORY
history_cmd: HISTORY {
                            auto history = HistoryCommand::create(ctxt);
                            $$ = history;
                         }
           ;

// CHANGE
change_subcmd: NEW { $$ = ChangeOpType::NEW; }
             | SUBMIT { $$ = ChangeOpType::SUBMIT; }
             | DELETE { $$ = ChangeOpType::DELETE; }
             | LIST { $$ = ChangeOpType::LIST; }
             ;

change_cmd: CHANGE change_subcmd {
                                    auto change = ChangeCommand::create(ctxt, $2);
                                    $$ = change;
                                 }
          ;

// COMMIT
commit_cmd: COMMIT { $$ = CommitCommand::create(ctxt); }
          ;

%%


// Using our own custom location in YScanner, not yylloc (loc)
void db::YParser::error(const location_type& loc, const std::string& msg) {
    location turingLoc = scanner.getLocation();
    throw db::YParser::syntax_error(turingLoc, msg);
}

