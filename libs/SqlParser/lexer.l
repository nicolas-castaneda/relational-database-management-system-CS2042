%{
    // Guide: https://github.com/jonathan-beard/simple_wc_example
    #include <string>

    #include "scanner.hpp"
    #undef YY_DECL
    #define YY_DECL int scanner::yylex(yy::parser::semantic_type * const lval, yy::parser::location_type * location)

    using token = yy::parser::token;

    //#define yyterminate() return(token::END)

    #define YY_NO_UNISTD_H

    #define YY_USER_ACTION loc->step(); loc->columns(yyleng);
%}
%option debug
%option nodefault
%option yyclass="scanner"
%option noyywrap
%option c++

letter [a-zA-Z]
num [0-9]*
floating {num}"."{num}
string \'[^\']*\'

id {letter}({letter}|{num}|_)*

spaces [ \t\n]*
endline ";"
sep ","

/* Verbs */
insert (?i:insert)
update (?i:update)
delete (?i:delete)
select (?i:select)
create (?i:create)
drop   (?i:drop)

/* Objects */
table (?i:table)
index (?i:index)
column (?i:column)
seq (?i:seq)
avl (?i:avl)
isam (?i:isam)

/* Conditional */
where (?i:where)
and (?i:and)
or (?i:or)
between (?i:between)
equal "="
l  "<"
g  ">"
le "<="
ge ">="

/* Context */
from (?i:from)
into (?i:into)
set (?i:set)
values (?i:values)

/* Data Types */
int (?i:int)
double (?i:double)
char (?i:char)
bool (?i:bool)

pk "primary key"
on (?i:on)

%%
%{
    yylval = lval;
%}
{spaces}    {;}
"*"         {return token::ALL;}
{endline}   {return token::ENDL;}
"("         {return token::PI;}
")"         {return token::PD;}
{sep}       {return token::SEP;}

{insert}    {return token::INSERT;}
{update}    {return token::UPDATE;}
{delete}    {return token::DELETE;}
{select}    {return token::SELECT;}
{create}    {return token::CREATE;}
{drop}      {return token::DROP;}

{from}      {return token::FROM;}
{into}      {return token::INTO;}
{set}       {return token::SET;}
{values}    {return token::VALUES;}

{where}     {return token::WHERE;}
{and}       {return token::AND;}
{or}        {return token::OR;}
{equal}     {return token::EQUAL;}
{between}   {return token::BETWEEN;}
{ge}        {return token::GE;}
{g}         {return token::G;}
{le}        {return token::LE;}
{l}         {return token::L;}

{table}     {return token::TABLE;}
{index}     {return token::INDEX;}
{column}    {return token::ID;}
{seq}       {return token::SEQ;}
{avl}       {return token::AVL;}
{isam}      {return token::ISAM;}

{int}       {return token::INT;}
{double}    {return token::DOUBLE;}
{char}      {return token::CHAR;}
{bool}      {return token::BOOL;}

{pk}        {return token::PK;}
{on}        {return token::ON;}

{id}        {yylval->emplace<std::string>() = yytext; return token::ID;}
{string}    {yylval->emplace<std::string>() = yytext; return token::STRING;}
{floating}  {yylval->emplace<double>() = std::stod(yytext); return token::FLOATING;}
{num}       {yylval->emplace<int>() = std::stoi(yytext); return token::NUM;}
<<EOF>>     {yyterminate();}
.           {spdlog::info("Unknown character");}
%%
