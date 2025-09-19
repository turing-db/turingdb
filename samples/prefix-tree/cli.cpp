#include "cli.h"
#include "utils.h"
#include "prefix-tree.h"

#include <string>
#include <iostream>
#include <cctype>
#include <iostream>

inline static const std::string help = "usage: \n \
        insert a string into the trie: 'i <string>' \n \
        query a string in the trie: 'f <string>' \n \
        get the node ids which a own a string in the trie: 'o <string>' \n \
        print the trie: 'p' \n \
        ";

void cli() {
    std::cout << help << std::endl;

    StringApproximatorIndex tree{};

    std::string in;
    while (true) {
        std::cout << ">";
        getline(std::cin, in);
        std::vector<std::string> tokens;
        split(tokens, in, " ");
        if (tokens[0] == "i") {
            if (tokens.size() != 2) std::cout << "i <param>" << std::endl;
            else tree.insert(tokens[1]);
        }
        else if (tokens[0] == "p") tree.print();
        else if (tokens[0] == "f") {
            if (tokens.size() != 2) std::cout << "f <param>" << std::endl;
            else {
                StringApproximatorIndex::PrefixTreeIterator res = tree.find(tokens[1]);
                std::string verb;
                if (res._result == StringApproximatorIndex::FOUND) {
                    verb = " found ";
                }
                else if (res._result == StringApproximatorIndex::FOUND_PREFIX) {
                    verb = " found as a subtring ";
                }
                else {
                    verb = " not found ";
                }
                std::cout << "String " << tokens[1] << verb << std::endl;
            }
            
        }
        else if (tokens[0] == "o") {
            if (tokens.size() != 2) std::cout << "o <param>" << std::endl;
            else {
                std::vector<NodeID> owners{};
                tree.getOwners(owners, tokens[1]);
                for (const auto o : owners) std::cout << o << "    ";
                std::cout << std::endl;
            }
        }
        else std::cout << "unkown cmd" << std::endl;
    }
}
