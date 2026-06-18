#pragma once

namespace db {

class CypherAST;

class DBProgramGenerator {
public:
    void generate(const CypherAST* query);
private:
};
    
}
