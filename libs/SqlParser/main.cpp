#include <cstdlib>
#include <cstring>
#include <iostream>

#include "SqlParser.hpp"

int main(const int argc, const char **argv) {
  /** instantiate driver object **/
  driver d;
  if (argc == 2) {

    std::cout << "ok" << std::endl;
    /** example for piping input from terminal, i.e., using cat **/
    if (std::strncmp(argv[1], "-o", 2) == 0) {
      std::cout << argv[1] << std::endl;
      d.parse(std::cin);
      std::cout << "ok" << std::endl;
    }
    /** simple help menu **/
    else if (std::strncmp(argv[1], "-h", 2) == 0) {
      std::cout << "use -o for pipe to std::cin\n";
      std::cout << "just give a filename to count from a file\n";
      std::cout << "use -h to get this menu\n";
      return (EXIT_SUCCESS);
    }
    /** example reading input from a file **/
    else {
      /** assume file, prod code, use stat to check **/
      d.parse(argv[1]);
    }
  } else {
    /** exit with failure condition **/
    d.parse(std::cin);
    return (EXIT_FAILURE);
  }
  return (EXIT_SUCCESS);
}
