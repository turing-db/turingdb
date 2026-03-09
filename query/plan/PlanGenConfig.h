#pragma once

namespace db {

class PlanGenConfig {
public:
    bool getUseValueHashJoin() const { return _useValueHashJoin; }
    bool getForceValueHashJoin() const { return _forceValueHashJoin; }

    void setUseValueHashJoin(bool useValueHashJoin) { _useValueHashJoin = useValueHashJoin; }
    void setForceValueHashJoin(bool forceValueHashJoin) { _forceValueHashJoin = forceValueHashJoin; }

private:
    bool _useValueHashJoin {true};
    bool _forceValueHashJoin {false};
};

}
