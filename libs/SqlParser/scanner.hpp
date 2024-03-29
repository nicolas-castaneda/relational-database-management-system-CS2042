#ifndef SCANNER_HPP
#define SCANNER_HPP 1

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include "location.hh"
#include "parser.tab.hh"

class scanner : public yyFlexLexer {
public:
  scanner(std::istream *in) : yyFlexLexer(in) {
    loc = new yy::parser::location_type();
  }

  ~scanner() override { delete loc; }

  using FlexLexer::yylex;

  virtual int yylex(yy::parser::semantic_type *const lval,
                    yy::parser::location_type *location);

private:
  yy::parser::semantic_type *yylval = nullptr;
  yy::parser::location_type *loc = nullptr;
};

#endif // SCANNER_HPP
