#include <gtest/gtest.h>

#include <ctype.h>
#include <algorithm>
#include <map>
#include <span>
#include <string>
#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "CypherLexer.h"
#include "CypherToken.h"
#include "SourceLocation.h"

using namespace db;

namespace {

struct LexCase {
    std::string_view _query;
    std::string_view _tokens;
};

const LexCase NUMBERS_AND_WORDS[] = {
    {".5",
     "DOUBLE(0.5)@1:1-1:3 PROG_END@1:1-1:3"},
    {"n.5",
     "ID(n)@1:1-1:2 DOUBLE(0.5)@1:2-1:4 PROG_END@1:2-1:4"},
    {"1..2",
     "DIGIT(1)@1:1-1:2 RANGE@1:2-1:4 DIGIT(2)@1:4-1:5 PROG_END@1:4-1:5"},
    {"1.",
     "DIGIT(1)@1:1-1:2 DOT@1:2-1:3 PROG_END@1:2-1:3"},
    {"1e",
     "DIGIT(1)@1:1-1:2 ID(e)@1:2-1:3 PROG_END@1:2-1:3"},
    {"0x",
     "DIGIT(0)@1:1-1:2 ID(x)@1:2-1:3 PROG_END@1:2-1:3"},
    {"MATCHES",
     "ID(MATCHES)@1:1-1:8 PROG_END@1:1-1:8"},
    {"merge_",
     "ID(merge_)@1:1-1:7 PROG_END@1:1-1:7"},
    {"1day",
     "DOUBLE(1)@1:1-1:3 ID(ay)@1:3-1:5 PROG_END@1:3-1:5"},
    {"1.5.5",
     "DOUBLE(1.5)@1:1-1:4 DOUBLE(0.5)@1:4-1:6 PROG_END@1:4-1:6"},
    {"1.5e",
     "DOUBLE(1.5)@1:1-1:4 ID(e)@1:4-1:5 PROG_END@1:4-1:5"},
    {".5e",
     "DOUBLE(0.5)@1:1-1:3 ID(e)@1:3-1:4 PROG_END@1:3-1:4"},
    {".5e3",
     "DOUBLE(500)@1:1-1:5 PROG_END@1:1-1:5"},
    {".5f",
     "DOUBLE(0.5)@1:1-1:4 PROG_END@1:1-1:4"},
    {"1e5",
     "DOUBLE(100000)@1:1-1:4 PROG_END@1:1-1:4"},
    {"1E5F",
     "DOUBLE(100000)@1:1-1:5 PROG_END@1:1-1:5"},
    {"1e+5",
     "DOUBLE(100000)@1:1-1:5 PROG_END@1:1-1:5"},
    {"1e-5d",
     "DOUBLE(1e-05)@1:1-1:6 PROG_END@1:1-1:6"},
    {"1e+",
     "DIGIT(1)@1:1-1:2 ID(e)@1:2-1:3 PLUS@1:3-1:4 PROG_END@1:3-1:4"},
    {"1.5e+3d",
     "DOUBLE(1500)@1:1-1:8 PROG_END@1:1-1:8"},
    {"0d",
     "DOUBLE(0)@1:1-1:3 PROG_END@1:1-1:3"},
    {"1F",
     "DOUBLE(1)@1:1-1:3 PROG_END@1:1-1:3"},
    {"12d3",
     "DOUBLE(12)@1:1-1:4 DIGIT(3)@1:4-1:5 PROG_END@1:4-1:5"},
    {"00x1",
     "DIGIT(0)@1:1-1:3 ID(x1)@1:3-1:5 PROG_END@1:3-1:5"},
    {"0xg",
     "DIGIT(0)@1:1-1:2 ID(xg)@1:2-1:4 PROG_END@1:2-1:4"},
    {"0o8",
     "DIGIT(0)@1:1-1:2 ID(o8)@1:2-1:4 PROG_END@1:2-1:4"},
    {"0o17",
     "DIGIT(15)@1:1-1:5 PROG_END@1:1-1:5"},
    {"0O17",
     "DIGIT(15)@1:1-1:5 PROG_END@1:1-1:5"},
    {"0X1Fd",
     "DIGIT(509)@1:1-1:6 PROG_END@1:1-1:6"},
    {"0x1.5",
     "DIGIT(1)@1:1-1:4 DOUBLE(0.5)@1:4-1:6 PROG_END@1:4-1:6"},
    {"0e5",
     "DOUBLE(0)@1:1-1:4 PROG_END@1:1-1:4"},
    {"0x7FFFFFFFFFFFFFFF",
     "DIGIT(9223372036854775807)@1:1-1:19 PROG_END@1:1-1:19"},
    {"0x8000000000000000",
     "LEXER_ERROR(Hexadecimal literal exceeded int64 range.)@1:1-1:19"},
    {"0o777777777777777777777",
     "DIGIT(9223372036854775807)@1:1-1:24 PROG_END@1:1-1:24"},
    {"0o1777777777777777777777",
     "LEXER_ERROR(Octal literal exceeded int64 range.)@1:1-1:25"},
    {"9223372036854775807",
     "DIGIT(9223372036854775807)@1:1-1:20 PROG_END@1:1-1:20"},
    {"9223372036854775808",
     "integer literal 9223372036854775808, which exceeds int64 range unless negated@1:1-1:20 PROG_END@1:1-1:20"},
    {"9223372036854775809",
     "LEXER_ERROR(Decimal literal exceeded int64 range.)@1:1-1:20"},
    {"-9223372036854775808",
     "SUB@1:1-1:2 integer literal 9223372036854775808, which exceeds int64 range unless negated@1:2-1:21 PROG_END@1:2-1:21"},
    {"99999999999999999999",
     "LEXER_ERROR(Decimal literal exceeded int64 range.)@1:1-1:21"},
    {"RETURN 1, 2.5, .5, 1.5e10",
     "RETURN(RETURN)@1:1-1:7 DIGIT(1)@1:8-1:9 COMMA@1:9-1:10 DOUBLE(2.5)@1:11-1:14 COMMA@1:14-1:15 DOUBLE(0.5)@1:16-1:18 COMMA@1:18-1:19 DOUBLE(15000000000)@1:20-1:26 PROG_END@1:20-1:26"},
    {"MERGE_DATAPARTS",
     "MERGE_DATAPARTS(MERGE_DATAPARTS)@1:1-1:16 PROG_END@1:1-1:16"},
    {"MERGE_DATAPARTS1",
     "ID(MERGE_DATAPARTS1)@1:1-1:17 PROG_END@1:1-1:17"},
    {"match",
     "MATCH(match)@1:1-1:6 PROG_END@1:1-1:6"},
    {"MaTcH",
     "MATCH(MaTcH)@1:1-1:6 PROG_END@1:1-1:6"},
    {"s3",
     "S3(s3)@1:1-1:3 PROG_END@1:1-1:3"},
    {"S3x",
     "ID(S3x)@1:1-1:4 PROG_END@1:1-1:4"},
    {"null",
     "NULL_(null)@1:1-1:5 PROG_END@1:1-1:5"},
    {"Null",
     "NULL_(Null)@1:1-1:5 PROG_END@1:1-1:5"},
    {"error",
     "ERROR_(error)@1:1-1:6 PROG_END@1:1-1:6"},
    {"ERROR_",
     "ID(ERROR_)@1:1-1:7 PROG_END@1:1-1:7"},
    {"NULL_",
     "ID(NULL_)@1:1-1:6 PROG_END@1:1-1:6"},
    {"_",
     "ID(_)@1:1-1:2 PROG_END@1:1-1:2"},
    {"_1",
     "ID(_1)@1:1-1:3 PROG_END@1:1-1:3"},
    {"a1_b",
     "ID(a1_b)@1:1-1:5 PROG_END@1:1-1:5"},
    {"x.y.z",
     "ID(x)@1:1-1:2 DOT@1:2-1:3 ID(y)@1:3-1:4 DOT@1:4-1:5 ID(z)@1:5-1:6 PROG_END@1:5-1:6"},
    {"1+=2",
     "DIGIT(1)@1:1-1:2 ADD_ASSIGN@1:2-1:4 DIGIT(2)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a<>b",
     "ID(a)@1:1-1:2 NOT_EQUAL@1:2-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a!=b",
     "ID(a)@1:1-1:2 NOT_EQUAL@1:2-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a<=b",
     "ID(a)@1:1-1:2 LE@1:2-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a>=b",
     "ID(a)@1:1-1:2 GE@1:2-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a||b",
     "ID(a)@1:1-1:2 CONCAT@1:2-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a|b",
     "ID(a)@1:1-1:2 PIPE@1:2-1:3 ID(b)@1:3-1:4 PROG_END@1:3-1:4"},
    {"a..b",
     "ID(a)@1:1-1:2 RANGE@1:2-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"a==b",
     "ID(a)@1:1-1:2 ASSIGN@1:2-1:3 ASSIGN@1:3-1:4 ID(b)@1:4-1:5 PROG_END@1:4-1:5"},
    {"$1",
     "DOLLAR@1:1-1:2 DIGIT(1)@1:2-1:3 PROG_END@1:2-1:3"},
    {"$a",
     "DOLLAR@1:1-1:2 ID(a)@1:2-1:3 PROG_END@1:2-1:3"},
};

const LexCase IS_FOLLOWERS[] = {
    {"x IS NOT NULL",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NOT(NOT)@1:6-1:9 NULL_(NULL)@1:10-1:14 PROG_END@1:10-1:14"},
    {"x IS NULL",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NULL_(NULL)@1:6-1:10 PROG_END@1:6-1:10"},
    {"x IS Nullable",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 ID(Nullable)@1:6-1:14 PROG_END@1:6-1:14"},
    {"x IS NOT_A",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 ID(NOT_A)@1:6-1:11 PROG_END@1:6-1:11"},
    {"x IS NOTX",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 ID(NOTX)@1:6-1:10 PROG_END@1:6-1:10"},
    {"x IS NOT(",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NOT(NOT)@1:6-1:9 OPAREN@1:9-1:10 PROG_END@1:9-1:10"},
    {"x IS/* c */NULL",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NULL_(NULL)@1:12-1:16 PROG_END@1:12-1:16"},
    {"x IS//c\nNULL",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NULL_(NULL)@2:1-2:5 PROG_END@2:1-2:5"},
    {"x IS //c",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 PROG_END@1:6-1:9"},
    {"x IS /* unclosed",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 LEXER_ERROR(A comment that opens with /* must close with */)@1:6-1:8"},
    {"x is not null",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(is)@1:3-1:5 NOT(not)@1:6-1:9 NULL_(null)@1:10-1:14 PROG_END@1:10-1:14"},
    {"x IS\nNORMALIZED",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NORMALIZED(NORMALIZED)@2:1-2:11 PROG_END@2:1-2:11"},
    {"x IS NFC",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NFC(NFC)@1:6-1:9 PROG_END@1:6-1:9"},
    {"x IS NFKD NORMALIZED",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NFKD(NFKD)@1:6-1:10 NORMALIZED(NORMALIZED)@1:11-1:21 PROG_END@1:11-1:21"},
    {"x IS NFD",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NFD(NFD)@1:6-1:9 PROG_END@1:6-1:9"},
    {"x IS NFKC",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NFKC(NFKC)@1:6-1:10 PROG_END@1:6-1:10"},
    {"xISNOT",
     "ID(xISNOT)@1:1-1:7 PROG_END@1:1-1:7"},
    {"x ISNOT",
     "ID(x)@1:1-1:2 ID(ISNOT)@1:3-1:8 PROG_END@1:3-1:8"},
    {"x IS IS NULL",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 IS_VALUE_TEST(IS)@1:6-1:8 NULL_(NULL)@1:9-1:13 PROG_END@1:9-1:13"},
    {"x IS IS IS NOT",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 IS(IS)@1:6-1:8 IS_VALUE_TEST(IS)@1:9-1:11 NOT(NOT)@1:12-1:15 PROG_END@1:12-1:15"},
    {"x IS 99999999999999999999",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 LEXER_ERROR(Decimal literal exceeded int64 range.)@1:6-1:26"},
    {"x IS",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 PROG_END@1:3-1:5"},
    {"x IS ",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 PROG_END@1:5-1:6"},
    {"x IS\n",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 PROG_END@1:5-2:1"},
    {"n:IS",
     "ID(n)@1:1-1:2 COLON@1:2-1:3 IS(IS)@1:3-1:5 PROG_END@1:3-1:5"},
    {"(n IS Person)",
     "OPAREN@1:1-1:2 ID(n)@1:2-1:3 IS(IS)@1:4-1:6 ID(Person)@1:7-1:13 CPAREN@1:13-1:14 PROG_END@1:13-1:14"},
    {"IS NOT NULL",
     "IS_VALUE_TEST(IS)@1:1-1:3 NOT(NOT)@1:4-1:7 NULL_(NULL)@1:8-1:12 PROG_END@1:8-1:12"},
    {"x IS NORMALIZEDX",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 ID(NORMALIZEDX)@1:6-1:17 PROG_END@1:6-1:17"},
    {"x IS NULL1",
     "ID(x)@1:1-1:2 IS(IS)@1:3-1:5 ID(NULL1)@1:6-1:11 PROG_END@1:6-1:11"},
    {"x IS\tNOT",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NOT(NOT)@1:6-1:9 PROG_END@1:6-1:9"},
    {"x IS\r\nNULL",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NULL_(NULL)@2:1-2:5 PROG_END@2:1-2:5"},
    {"x IS /**/ /* a */ // b\n NOT",
     "ID(x)@1:1-1:2 IS_VALUE_TEST(IS)@1:3-1:5 NOT(NOT)@2:2-2:5 PROG_END@2:2-2:5"},
};

const LexCase STRINGS_AND_COMMENTS[] = {
    {"\"abc",
     "UNKNOWN@1:1-1:2 ID(abc)@1:2-1:5 PROG_END@1:2-1:5"},
    {"'abc",
     "UNKNOWN@1:1-1:2 ID(abc)@1:2-1:5 PROG_END@1:2-1:5"},
    {"'a\\'",
     "UNKNOWN@1:1-1:2 ID(a)@1:2-1:3 UNKNOWN@1:3-1:4 UNKNOWN@1:4-1:5 PROG_END@1:4-1:5"},
    {"\"a\\\n\"",
     "UNKNOWN@1:1-1:2 ID(a)@1:2-1:3 UNKNOWN@1:3-1:4 UNKNOWN@2:1-2:2 PROG_END@2:1-2:2"},
    {"\"a\nb\"",
     "STRING_LITERAL(a\nb)@1:1-2:3 PROG_END@1:1-2:3"},
    {"'a\\nb'",
     "STRING_LITERAL(a\\nb)@1:1-1:7 PROG_END@1:1-1:7"},
    {"\"a\\\"b\"",
     "STRING_LITERAL(a\\\"b)@1:1-1:7 PROG_END@1:1-1:7"},
    {"'it''s'",
     "STRING_LITERAL(it)@1:1-1:5 STRING_LITERAL(s)@1:5-1:8 PROG_END@1:5-1:8"},
    {"\"\"",
     "STRING_LITERAL()@1:1-1:3 PROG_END@1:1-1:3"},
    {"''",
     "STRING_LITERAL()@1:1-1:3 PROG_END@1:1-1:3"},
    {"\"\\\\\"",
     "STRING_LITERAL(\\\\)@1:1-1:5 PROG_END@1:1-1:5"},
    {"\"a\\",
     "UNKNOWN@1:1-1:2 ID(a)@1:2-1:3 UNKNOWN@1:3-1:4 PROG_END@1:3-1:4"},
    {"'\\'",
     "UNKNOWN@1:1-1:2 UNKNOWN@1:2-1:3 UNKNOWN@1:3-1:4 PROG_END@1:3-1:4"},
    {"/* unclosed",
     "LEXER_ERROR(A comment that opens with /* must close with */)@1:1-1:3"},
    {"RETURN 1 /* /* */ */",
     "RETURN(RETURN)@1:1-1:7 DIGIT(1)@1:8-1:9 MULT@1:19-1:20 DIV@1:20-1:21 PROG_END@1:20-1:21"},
    {"/*/",
     "LEXER_ERROR(A comment that opens with /* must close with */)@1:1-1:3"},
    {"/**/",
     "PROG_END@1:1-1:5"},
    {"//",
     "PROG_END@1:1-1:3"},
    {"// a",
     "PROG_END@1:1-1:5"},
    {"/* a */",
     "PROG_END@1:1-1:8"},
    {"/* a **/",
     "PROG_END@1:1-1:9"},
    {"/** a **/x",
     "ID(x)@1:10-1:11 PROG_END@1:10-1:11"},
    {"/* a\nb */ +",
     "PLUS@2:6-2:7 PROG_END@2:6-2:7"},
    {"RETURN 1 // c\n+ 2",
     "RETURN(RETURN)@1:1-1:7 DIGIT(1)@1:8-1:9 PLUS@2:1-2:2 DIGIT(2)@2:3-2:4 PROG_END@2:3-2:4"},
    {"RETURN 1 //c",
     "RETURN(RETURN)@1:1-1:7 DIGIT(1)@1:8-1:9 PROG_END@1:10-1:13"},
    {"a/b",
     "ID(a)@1:1-1:2 DIV@1:2-1:3 ID(b)@1:3-1:4 PROG_END@1:3-1:4"},
    {"a /",
     "ID(a)@1:1-1:2 DIV@1:3-1:4 PROG_END@1:3-1:4"},
    {"/",
     "DIV@1:1-1:2 PROG_END@1:1-1:2"},
    {"`a b`",
     "ESC_LITERAL(a b)@1:1-1:6 PROG_END@1:1-1:6"},
    {"`",
     "ESC@1:1-1:2 PROG_END@1:1-1:2"},
    {"`a\nb`",
     "ESC_LITERAL(a\nb)@1:1-2:3 PROG_END@1:1-2:3"},
    {"``",
     "ESC_LITERAL()@1:1-1:3 PROG_END@1:1-1:3"},
    {"`a`b`",
     "ESC_LITERAL(a)@1:1-1:4 ID(b)@1:4-1:5 ESC@1:5-1:6 PROG_END@1:5-1:6"},
    {"",
     "PROG_END@1:1-1:1"},
    {" ",
     "PROG_END@1:1-1:2"},
    {"\n",
     "PROG_END@1:1-2:1"},
    {"\t\r\n",
     "PROG_END@1:1-2:1"},
    {"\v",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"\f",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"\xc3\xa9",
     "UNKNOWN@1:1-1:2 UNKNOWN@1:2-1:3 PROG_END@1:2-1:3"},
    {std::string_view {"\x00", 1},
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"~",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"@",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"#",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"?",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"\\",
     "UNKNOWN@1:1-1:2 PROG_END@1:1-1:2"},
    {"RETURN 'a' + \"b\"",
     "RETURN(RETURN)@1:1-1:7 STRING_LITERAL(a)@1:8-1:11 PLUS@1:12-1:13 STRING_LITERAL(b)@1:14-1:17 PROG_END@1:14-1:17"},
    {"RETURN 1 \x80 2",
     "RETURN(RETURN)@1:1-1:7 DIGIT(1)@1:8-1:9 UNKNOWN@1:10-1:11 DIGIT(2)@1:12-1:13 PROG_END@1:12-1:13"},
    {std::string_view {"MATCH (n\x00)", 10},
     "MATCH(MATCH)@1:1-1:6 OPAREN@1:7-1:8 ID(n)@1:8-1:9 UNKNOWN@1:9-1:10 CPAREN@1:10-1:11 PROG_END@1:10-1:11"},
};

const LexCase ARROWS[] = {
    {"(a)<--(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@1:6-1:7 OPAREN@1:7-1:8 ID(b)@1:8-1:9 CPAREN@1:9-1:10 PROG_END@1:9-1:10"},
    {"(a)< - -(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:6-1:7 SUB@1:8-1:9 OPAREN@1:9-1:10 ID(b)@1:10-1:11 CPAREN@1:11-1:12 PROG_END@1:11-1:12"},
    {"(a)<-\n-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@2:1-2:2 OPAREN@2:2-2:3 ID(b)@2:3-2:4 CPAREN@2:4-2:5 PROG_END@2:4-2:5"},
    {"(a)<-/* c */-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@1:13-1:14 OPAREN@1:14-1:15 ID(b)@1:15-1:16 CPAREN@1:16-1:17 PROG_END@1:16-1:17"},
    {"(a)<-// c\n-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@2:1-2:2 OPAREN@2:2-2:3 ID(b)@2:3-2:4 CPAREN@2:4-2:5 PROG_END@2:4-2:5"},
    {"(a)-->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:5-1:6 GT@1:6-1:7 OPAREN@1:7-1:8 ID(b)@1:8-1:9 CPAREN@1:9-1:10 PROG_END@1:9-1:10"},
    {"(a)- - >(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:6-1:7 GT@1:8-1:9 OPAREN@1:9-1:10 ID(b)@1:10-1:11 CPAREN@1:11-1:12 PROG_END@1:11-1:12"},
    {"(a)-\n->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@2:1-2:2 GT@2:2-2:3 OPAREN@2:3-2:4 ID(b)@2:4-2:5 CPAREN@2:5-2:6 PROG_END@2:5-2:6"},
    {"(a)-/* c */-/* d */>(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:12-1:13 GT@1:20-1:21 OPAREN@1:21-1:22 ID(b)@1:22-1:23 CPAREN@1:23-1:24 PROG_END@1:23-1:24"},
    {"(a)-// c\n-// d\n>(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@2:1-2:2 GT@3:1-3:2 OPAREN@3:2-3:3 ID(b)@3:3-3:4 CPAREN@3:4-3:5 PROG_END@3:4-3:5"},
    {"(a)-[e]->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 OBRACK@1:5-1:6 ID(e)@1:6-1:7 CBRACK@1:7-1:8 SUB@1:8-1:9 GT@1:9-1:10 OPAREN@1:10-1:11 ID(b)@1:11-1:12 CPAREN@1:12-1:13 PROG_END@1:12-1:13"},
    {"(a)- [e]->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 OBRACK@1:6-1:7 ID(e)@1:7-1:8 CBRACK@1:8-1:9 SUB@1:9-1:10 GT@1:10-1:11 OPAREN@1:11-1:12 ID(b)@1:12-1:13 CPAREN@1:13-1:14 PROG_END@1:13-1:14"},
    {"(a)-/**/[e]->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 OBRACK@1:9-1:10 ID(e)@1:10-1:11 CBRACK@1:11-1:12 SUB@1:12-1:13 GT@1:13-1:14 OPAREN@1:14-1:15 ID(b)@1:15-1:16 CPAREN@1:16-1:17 PROG_END@1:16-1:17"},
    {"(a)<-[e]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 OBRACK@1:6-1:7 ID(e)@1:7-1:8 CBRACK@1:8-1:9 SUB@1:9-1:10 OPAREN@1:10-1:11 ID(b)@1:11-1:12 CPAREN@1:12-1:13 PROG_END@1:12-1:13"},
    {"(a)< - [e]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:6-1:7 OBRACK@1:8-1:9 ID(e)@1:9-1:10 CBRACK@1:10-1:11 SUB@1:11-1:12 OPAREN@1:12-1:13 ID(b)@1:13-1:14 CPAREN@1:14-1:15 PROG_END@1:14-1:15"},
    {"(a)<-->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@1:6-1:7 GT@1:7-1:8 OPAREN@1:8-1:9 ID(b)@1:9-1:10 CPAREN@1:10-1:11 PROG_END@1:10-1:11"},
    {"(a)--[e]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:5-1:6 OBRACK@1:6-1:7 ID(e)@1:7-1:8 CBRACK@1:8-1:9 SUB@1:9-1:10 OPAREN@1:10-1:11 ID(b)@1:11-1:12 CPAREN@1:12-1:13 PROG_END@1:12-1:13"},
    {"(a)--(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:5-1:6 OPAREN@1:6-1:7 ID(b)@1:7-1:8 CPAREN@1:8-1:9 PROG_END@1:8-1:9"},
    {"(a)- // c",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 PROG_END@1:6-1:10"},
    {"(a)<- /* unclosed",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 LEXER_ERROR(A comment that opens with /* must close with */)@1:7-1:9"},
    {"(a)<-",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 PROG_END@1:5-1:6"},
    {"(a)<",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 PROG_END@1:4-1:5"},
    {"(a)-",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 PROG_END@1:4-1:5"},
    {"x <--1",
     "ID(x)@1:1-1:2 LT@1:3-1:4 SUB@1:4-1:5 SUB@1:5-1:6 DIGIT(1)@1:6-1:7 PROG_END@1:6-1:7"},
    {"x<-1",
     "ID(x)@1:1-1:2 LT@1:2-1:3 SUB@1:3-1:4 DIGIT(1)@1:4-1:5 PROG_END@1:4-1:5"},
    {"y<--1",
     "ID(y)@1:1-1:2 LT@1:2-1:3 SUB@1:3-1:4 SUB@1:4-1:5 DIGIT(1)@1:5-1:6 PROG_END@1:5-1:6"},
    {"z--1",
     "ID(z)@1:1-1:2 SUB@1:2-1:3 SUB@1:3-1:4 DIGIT(1)@1:4-1:5 PROG_END@1:4-1:5"},
    {"1 -[1]",
     "DIGIT(1)@1:1-1:2 SUB@1:3-1:4 OBRACK@1:4-1:5 DIGIT(1)@1:5-1:6 CBRACK@1:6-1:7 PROG_END@1:6-1:7"},
    {"1 - [1]",
     "DIGIT(1)@1:1-1:2 SUB@1:3-1:4 OBRACK@1:5-1:6 DIGIT(1)@1:6-1:7 CBRACK@1:7-1:8 PROG_END@1:7-1:8"},
    {"x < - -2",
     "ID(x)@1:1-1:2 LT@1:3-1:4 SUB@1:5-1:6 SUB@1:7-1:8 DIGIT(2)@1:8-1:9 PROG_END@1:8-1:9"},
    {"x <- -2",
     "ID(x)@1:1-1:2 LT@1:3-1:4 SUB@1:4-1:5 SUB@1:6-1:7 DIGIT(2)@1:7-1:8 PROG_END@1:7-1:8"},
    {"(a)<->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 GT@1:6-1:7 OPAREN@1:7-1:8 ID(b)@1:8-1:9 CPAREN@1:9-1:10 PROG_END@1:9-1:10"},
    {"(a)->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 GT@1:5-1:6 OPAREN@1:6-1:7 ID(b)@1:7-1:8 CPAREN@1:8-1:9 PROG_END@1:8-1:9"},
    {"(a)<-\t\r\n-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@2:1-2:2 OPAREN@2:2-2:3 ID(b)@2:3-2:4 CPAREN@2:4-2:5 PROG_END@2:4-2:5"},
    {"(a)-\t[e]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 OBRACK@1:6-1:7 ID(e)@1:7-1:8 CBRACK@1:8-1:9 SUB@1:9-1:10 OPAREN@1:10-1:11 ID(b)@1:11-1:12 CPAREN@1:12-1:13 PROG_END@1:12-1:13"},
    {"(a)<-//\n[e]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 OBRACK@2:1-2:2 ID(e)@2:2-2:3 CBRACK@2:3-2:4 SUB@2:4-2:5 OPAREN@2:5-2:6 ID(b)@2:6-2:7 CPAREN@2:7-2:8 PROG_END@2:7-2:8"},
    {"(a)- - - >(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:6-1:7 SUB@1:8-1:9 GT@1:10-1:11 OPAREN@1:11-1:12 ID(b)@1:12-1:13 CPAREN@1:13-1:14 PROG_END@1:13-1:14"},
    {"(a)---(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:5-1:6 SUB@1:6-1:7 OPAREN@1:7-1:8 ID(b)@1:8-1:9 CPAREN@1:9-1:10 PROG_END@1:9-1:10"},
    {"(a)<---(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 SUB@1:6-1:7 SUB@1:7-1:8 OPAREN@1:8-1:9 ID(b)@1:9-1:10 CPAREN@1:10-1:11 PROG_END@1:10-1:11"},
    {"(a)--->(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 SUB@1:5-1:6 SUB@1:6-1:7 GT@1:7-1:8 OPAREN@1:8-1:9 ID(b)@1:9-1:10 CPAREN@1:10-1:11 PROG_END@1:10-1:11"},
    {"(a)<-[]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 OBRACK@1:6-1:7 CBRACK@1:7-1:8 SUB@1:8-1:9 OPAREN@1:9-1:10 ID(b)@1:10-1:11 CPAREN@1:11-1:12 PROG_END@1:11-1:12"},
    {"(a)-[]-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 OBRACK@1:5-1:6 CBRACK@1:6-1:7 SUB@1:7-1:8 OPAREN@1:8-1:9 ID(b)@1:9-1:10 CPAREN@1:10-1:11 PROG_END@1:10-1:11"},
    {"<-[",
     "LT@1:1-1:2 SUB@1:2-1:3 OBRACK@1:3-1:4 PROG_END@1:3-1:4"},
    {"-[",
     "SUB@1:1-1:2 OBRACK@1:2-1:3 PROG_END@1:2-1:3"},
    {"-->",
     "SUB@1:1-1:2 SUB@1:2-1:3 GT@1:3-1:4 PROG_END@1:3-1:4"},
    {"<--",
     "LT@1:1-1:2 SUB@1:2-1:3 SUB@1:3-1:4 PROG_END@1:3-1:4"},
    {"(a)-/* c */",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 PROG_END@1:5-1:12"},
    {"(a)-//c",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 SUB@1:4-1:5 PROG_END@1:5-1:8"},
    {"(a)<-/*/-(b)",
     "OPAREN@1:1-1:2 ID(a)@1:2-1:3 CPAREN@1:3-1:4 LT@1:4-1:5 SUB@1:5-1:6 LEXER_ERROR(A comment that opens with /* must close with */)@1:6-1:8"},
};

void appendLocation(const SourceLocation& location, std::string& tokens) {
    tokens += fmt::format("@{}:{}-{}:{}", location._beginLine, location._beginColumn, location._endLine, location._endColumn);
}

void appendValue(const CypherToken& token, std::string& tokens) {
    const CypherTokenKind kind = token._kind;
    const bool holdsText = CypherToken::isName(kind)
                           || kind == CypherTokenKind::STRING_LITERAL
                           || kind == CypherTokenKind::IS_VALUE_TEST
                           || kind == CypherTokenKind::LEXER_ERROR;

    if (holdsText) {
        tokens += fmt::format("({})", token._text);
    } else if (kind == CypherTokenKind::DIGIT) {
        tokens += fmt::format("({})", token._integer);
    } else if (kind == CypherTokenKind::DOUBLE) {
        tokens += fmt::format("({})", token._double);
    }
}

void renderTokens(std::string_view query, std::string& tokens) {
    CypherLexer lexer(query);

    CypherToken token;
    do {
        lexer.lex(token);

        if (!tokens.empty()) {
            tokens += " ";
        }

        tokens += CypherToken::toString(token._kind);
        appendValue(token, tokens);
        appendLocation(token._location, tokens);
    } while (token._kind != CypherTokenKind::PROG_END && token._kind != CypherTokenKind::LEXER_ERROR);
}

CypherTokenKind lexKind(std::string_view word) {
    CypherLexer lexer(word);

    CypherToken token;
    lexer.lex(token);

    return token._kind;
}

void lowerCase(std::string& word) {
    std::ranges::transform(word, word.begin(), [](char character) {
        return static_cast<char>(tolower(static_cast<unsigned char>(character)));
    });
}

void upperCase(std::string& word) {
    std::ranges::transform(word, word.begin(), [](char character) {
        return static_cast<char>(toupper(static_cast<unsigned char>(character)));
    });
}

}

class CypherLexerTest : public ::testing::Test {
protected:
    static void expectTokens(std::span<const LexCase> cases) {
        for (const LexCase& lexCase : cases) {
            std::string tokens;
            renderTokens(lexCase._query, tokens);
            EXPECT_EQ(tokens, lexCase._tokens) << lexCase._query;
        }
    }
};

TEST_F(CypherLexerTest, lexesNumbersAndWords) {
    expectTokens(NUMBERS_AND_WORDS);
}

TEST_F(CypherLexerTest, lexesIsByTheWordAfterIt) {
    expectTokens(IS_FOLLOWERS);
}

TEST_F(CypherLexerTest, lexesStringsAndComments) {
    expectTokens(STRINGS_AND_COMMENTS);
}

TEST_F(CypherLexerTest, lexesArrows) {
    expectTokens(ARROWS);
}

TEST_F(CypherLexerTest, lexesExactlyTheKeywordsInAnyCase) {
    const size_t firstKeyword = static_cast<size_t>(CypherTokenKind::PROCEDURES);
    const size_t endOfKeywords = static_cast<size_t>(CypherTokenKind::ESC_LITERAL);

    std::map<std::string, CypherTokenKind> keywords;
    for (size_t index = firstKeyword; index < endOfKeywords; index++) {
        const CypherTokenKind kind = static_cast<CypherTokenKind>(index);
        std::string spelling(CypherToken::toString(kind));

        // NULL_ and ERROR_ are named with an underscore that their spelling does not have
        if (spelling.ends_with('_')) {
            spelling.pop_back();
        }

        keywords.emplace(spelling, kind);
    }

    for (const auto& [spelling, kind] : keywords) {
        std::string lower = spelling;
        lowerCase(lower);

        std::string capitalized = lower;
        capitalized[0] = static_cast<char>(toupper(static_cast<unsigned char>(capitalized[0])));

        const std::string words[] = {
            spelling,
            lower,
            capitalized,
            spelling.substr(0, spelling.size() - 1),
            spelling + "S",
            "X" + spelling,
        };

        for (const std::string& word : words) {
            std::string upper = word;
            upperCase(upper);

            const auto keyword = keywords.find(upper);
            const CypherTokenKind expected = keyword == keywords.end() ? CypherTokenKind::ID : keyword->second;

            EXPECT_EQ(CypherToken::toString(lexKind(word)), CypherToken::toString(expected)) << word;
        }
    }
}
