#include <gtest/gtest.h>

#include <stddef.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view DIVIDED_ROOM =
    "WITH ['############', "
    "      '#....#.....#', "
    "      '#....#.....#', "
    "      '#....#.....#', "
    "      '#....#.....#', "
    "      '#....#.....#', "
    "      '#..........#', "
    "      '############'] AS rows ";

constexpr std::string_view DIVIDED_ROOM_SPAWN =
    "MATCH (playerCell:Cell {x: 2, y: 2}), (firstCell:Cell {x: 8, y: 2}), (secondCell:Cell {x: 9, y: 5}) "
    "CREATE (:Player {px: 2.5, py: 2.5, angle: 0.0, health: 100, tick: 0})-[:AT]->(playerCell), "
    "       (:Enemy {id: 1, px: 8.5, py: 2.5, health: 30, speed: 0.05})-[:AT]->(firstCell), "
    "       (:Enemy {id: 2, px: 9.5, py: 5.5, health: 30, speed: 0.05})-[:AT]->(secondCell)";

constexpr std::string_view ARENA =
    "WITH ['################################', "
    "      '#..............#...............#', "
    "      '#..............#...............#', "
    "      '#..............#........#......#', "
    "      '#...##.........#......###......#', "
    "      '#...##.........#......###......#', "
    "      '#..............#........#......#', "
    "      '#.......................#......#', "
    "      '#..............#........#......#', "
    "      '#.........##...#........#......#', "
    "      '#.........##...#........#......#', "
    "      '#..............#........#......#', "
    "      '#..............#...............#', "
    "      '#..............#...............#', "
    "      '#..............#...............#', "
    "      '#####.####################.#####', "
    "      '#..............#...............#', "
    "      '#..............#...............#', "
    "      '#..............#...............#', "
    "      '#..............#...##########..#', "
    "      '#..............#...#........#..#', "
    "      '#....##........#...#........#..#', "
    "      '#....##........#...#........#..#', "
    "      '#..............#............#..#', "
    "      '#..................#........#..#', "
    "      '#..............#...#........#..#', "
    "      '#.........##...#...#........#..#', "
    "      '#.........##...#...#........#..#', "
    "      '#..............#...##########..#', "
    "      '#..............#...............#', "
    "      '#..............#...............#', "
    "      '################################'] AS rows ";

constexpr std::string_view ARENA_SPAWN =
    "MATCH (playerCell:Cell {x: 7, y: 7}), "
    "      (firstCell:Cell {x: 28, y: 2}), "
    "      (secondCell:Cell {x: 3, y: 28}), "
    "      (thirdCell:Cell {x: 23, y: 24}), "
    "      (fourthCell:Cell {x: 29, y: 29}) "
    "CREATE (:Player {px: 7.5, py: 7.5, angle: 0.0, health: 100, tick: 0})-[:AT]->(playerCell), "
    "       (:Enemy {id: 1, px: 28.5, py: 2.5, health: 30, speed: 0.2})-[:AT]->(firstCell), "
    "       (:Enemy {id: 2, px: 3.5, py: 28.5, health: 30, speed: 0.2})-[:AT]->(secondCell), "
    "       (:Enemy {id: 3, px: 23.5, py: 24.5, health: 30, speed: 0.2})-[:AT]->(thirdCell), "
    "       (:Enemy {id: 4, px: 29.5, py: 29.5, health: 30, speed: 0.2})-[:AT]->(fourthCell)";

constexpr std::string_view CREATE_LEVEL =
    "WITH rows, size(rows[0]) AS width, size(rows) AS height "
    "CREATE (:Level {width: width, "
    "                height: height, "
    "                rounds: width + height, "
    "                walls: [i IN range(0, width * height - 1) | substring(rows[i / width], i % width, 1) = '#']}) "
    "WITH rows, width, height "
    "UNWIND range(0, height - 1) AS y "
    "UNWIND range(0, width - 1) AS x "
    "WITH x, y, substring(rows[y], x, 1) AS tile "
    "WHERE tile = '.' "
    "CREATE (:Cell {x: x, y: y, dist: 1000.0})";

constexpr std::string_view LINK_NEIGHBOURS =
    "MATCH (l:Level), (a:Cell), (b:Cell) "
    "WHERE id(a) < id(b) AND abs(a.x - b.x) <= 1 AND abs(a.y - b.y) <= 1 "
    "AND (a.x = b.x OR a.y = b.y "
    "     OR (NOT l.walls[a.y * l.width + b.x] AND NOT l.walls[b.y * l.width + a.x])) "
    "CREATE (a)-[:ADJ {cost: sqrt(toFloat((a.x - b.x) ^ 2 + (a.y - b.y) ^ 2))}]->(b)";

constexpr std::string_view LINK_SIGHT_LINES =
    "MATCH (l:Level), (a:Cell), (b:Cell) "
    "WHERE id(a) < id(b) AND (a.x - b.x) ^ 2 + (a.y - b.y) ^ 2 <= 64 "
    "AND none(t IN range(1, 19) WHERE "
    "    l.walls[toInteger(floor(a.y + 0.5 + (b.y - a.y) * t / 20.0)) * l.width "
    "            + toInteger(floor(a.x + 0.5 + (b.x - a.x) * t / 20.0))]) "
    "CREATE (a)-[:SEES]->(b)";

constexpr std::string_view STAND_STILL = "WITH 0.0 AS forward, 0.0 AS turn, false AS fire ";
constexpr std::string_view WALK_FORWARD = "WITH 1.0 AS forward, 0.0 AS turn, false AS fire ";
constexpr std::string_view FIRE = "WITH 0.0 AS forward, 0.0 AS turn, true AS fire ";
constexpr std::string_view TURN_SOUTH_AND_FIRE = "WITH 0.0 AS forward, 1.5707963267948966 AS turn, true AS fire ";

constexpr std::string_view PLAYER_INTENT =
    "MATCH (p:Player)-[:AT]->(pc:Cell) "
    "WITH p, pc, forward, fire, p.angle + turn AS a "
    "SET p.angle = a, "
    "    p.vx = cos(a) * forward * 0.1, "
    "    p.vy = sin(a) * forward * 0.1, "
    "    p.tick = p.tick + 1 "
    "WITH p, pc, fire ";

constexpr std::string_view DISTANCE_FIELD =
    "CALL (pc) { "
    "    WITH pc WHERE pc.dist > 0.0 "
    "    MATCH (l:Level) "
    "    MATCH (c:Cell) "
    "    SET c.dist = CASE WHEN c = pc THEN 0.0 ELSE 1000.0 END "
    "    WITH l, count(*) AS reset "
    "    UNWIND range(1, l.rounds) AS round "
    "    CALL { "
    "        MATCH (a:Cell)-[r:ADJ]-(b:Cell) "
    "        WITH b, min(a.dist + r.cost) AS nd "
    "        WHERE nd < b.dist "
    "        SET b.dist = nd "
    "    } "
    "} "
    "WITH p, pc, fire ";

constexpr std::string_view ENEMY_INTENT =
    "CALL (p, pc) { "
    "    MATCH (e:Enemy)-[:AT]->(ec:Cell)-[:ADJ]-(n:Cell) "
    "    WITH p, pc, e, ec, n ORDER BY n.dist "
    "    WITH p, pc, e, ec, collect(n)[0] AS hop "
    "    WITH p, e, hop, ec = pc OR EXISTS { (ec)-[:SEES]-(pc) } AS sees "
    "    WITH p, e, "
    "         CASE WHEN sees THEN p.px ELSE hop.x + 0.5 END - e.px AS dx, "
    "         CASE WHEN sees THEN p.py ELSE hop.y + 0.5 END - e.py AS dy, "
    "         sqrt((p.px - e.px) ^ 2 + (p.py - e.py) ^ 2) AS reach "
    "    WITH p, e, dx, dy, reach, sqrt(dx * dx + dy * dy) AS d "
    "    SET e.vx = CASE WHEN reach > 1.0 AND d > 0.0 THEN dx / d * e.speed ELSE 0.0 END, "
    "        e.vy = CASE WHEN reach > 1.0 AND d > 0.0 THEN dy / d * e.speed ELSE 0.0 END "
    "    WITH p, sum(CASE WHEN reach <= 1.0 THEN 5 ELSE 0 END) AS damage "
    "    SET p.health = p.health - damage "
    "} "
    "WITH p, pc, fire ";

constexpr std::string_view HITSCAN =
    "CALL (p, pc, fire) { "
    "    MATCH (e:Enemy)-[:AT]->(ec:Cell) "
    "    WHERE fire AND (ec = pc OR EXISTS { (ec)-[:SEES]-(pc) }) "
    "    WITH p, e, e.px - p.px AS dx, e.py - p.py AS dy "
    "    WITH p, e, dx, dy, sqrt(dx * dx + dy * dy) AS d "
    "    WHERE d > 0.0 AND (dx * cos(p.angle) + dy * sin(p.angle)) / d > cos(0.2) "
    "    WITH e, d ORDER BY d LIMIT 1 "
    "    SET e.health = e.health - 15 "
    "} "
    "WITH p ";

constexpr std::string_view MOVE =
    "CALL { "
    "    MATCH (m)-[at:AT]->(c:Cell) "
    "    WITH m, at, c, m.px + m.vx AS nx, m.py + m.vy AS ny "
    "    WITH m, at, c, nx, ny, toInteger(floor(nx)) AS tx, toInteger(floor(ny)) AS ty "
    "    OPTIONAL MATCH (c)-[:ADJ]-(n:Cell) WHERE n.x = tx AND n.y = ty "
    "    WITH m, at, nx, ny, n, tx = c.x AND ty = c.y AS stays "
    "    SET m.px = CASE WHEN stays OR n IS NOT NULL THEN nx ELSE m.px END, "
    "        m.py = CASE WHEN stays OR n IS NOT NULL THEN ny ELSE m.py END "
    "    WITH m, at, n, stays WHERE NOT stays AND n IS NOT NULL "
    "    DELETE at "
    "    CREATE (m)-[:AT]->(n) "
    "} "
    "WITH p ";

constexpr std::string_view REMOVE_DEAD =
    "CALL { "
    "    MATCH (e:Enemy) WHERE e.health <= 0 DETACH DELETE e "
    "} "
    "RETURN p.tick";

constexpr std::string_view RENDER =
    "MATCH (c:Cell) "
    "OPTIONAL MATCH (m)-[:AT]->(c) "
    "WITH c, max(CASE WHEN m:Player THEN 2 WHEN m IS NOT NULL THEN 1 ELSE 0 END) AS rank, min(m.id) AS enemy "
    "WITH collect([c.x, c.y, CASE WHEN rank = 2 THEN '@' WHEN rank = 1 THEN toString(enemy) ELSE '.' END]) AS tiles, "
    "     max(c.x) + 1 AS right, "
    "     max(c.y) + 1 AS bottom "
    "UNWIND range(0, bottom) AS y "
    "RETURN y, reduce(line = '', x IN range(0, right) | line + coalesce(head([t IN tiles WHERE t[0] = x AND t[1] = y | t[2]]), '#')) AS line "
    "ORDER BY y";

constexpr std::string_view CELLS_ANOTHER_ROUND_IMPROVES =
    "MATCH (a:Cell)-[r:ADJ]-(b:Cell) "
    "WITH b, min(a.dist + r.cost) AS nd "
    "WHERE nd < b.dist "
    "RETURN count(b)";

constexpr std::string_view ENEMIES_IN_REACH =
    "MATCH (p:Player), (e:Enemy) "
    "RETURN e.id, sqrt((p.px - e.px) ^ 2 + (p.py - e.py) ^ 2) <= 1.0 ORDER BY e.id";

}

class DoomTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph(_graphName);

        const auto change = system.newChange(_graphName);
        ASSERT_TRUE(change);

        _changeID = change.value()->id();
    }

    void buildLevel(std::string_view rows, std::string_view spawn) {
        std::string level {rows};
        level += CREATE_LEVEL;

        write(level);
        commit();

        write(LINK_NEIGHBOURS);
        write(LINK_SIGHT_LINES);
        write(spawn);
        commit();
    }

    void write(std::string_view query) {
        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), _changeID, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void commit() {
        write("COMMIT");
    }

    // A tick re-reads the AT edges the tick before it created, which only a COMMIT makes
    // visible to the next statement of the change
    void tick(std::string_view input) {
        std::string query {input};
        query += PLAYER_INTENT;
        query += DISTANCE_FIELD;
        query += ENEMY_INTENT;
        query += HITSCAN;
        query += MOVE;
        query += REMOVE_DEAD;

        write(query);
        commit();
    }

    void ticks(std::string_view input, size_t count) {
        for (size_t tickIndex = 0; tickIndex < count; tickIndex++) {
            tick(input);
            if (HasFatalFailure()) {
                return;
            }
        }
    }

    void read(std::string_view query, Rows& rows) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), _changeID, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        rows = sink.rows();
    }

    void printFrame(size_t tickIndex) {
        Rows lines;
        read(RENDER, lines);

        std::cout << "tick " << tickIndex << '\n';
        for (const Row& line : lines) {
            std::cout << line[1] << '\n';
        }
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;
        read(query, actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName {"doom"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    ChangeID _changeID;
};

class DividedRoomTest : public DoomTest {
protected:
    void initialize() override {
        ASSERT_NO_FATAL_FAILURE(DoomTest::initialize());
        buildLevel(DIVIDED_ROOM, DIVIDED_ROOM_SPAWN);
    }
};

class ArenaTest : public DoomTest {
protected:
    void initialize() override {
        ASSERT_NO_FATAL_FAILURE(DoomTest::initialize());
        buildLevel(ARENA, ARENA_SPAWN);
    }
};

TEST_F(DividedRoomTest, buildsTheMeshFromTheMap) {
    expectRows("MATCH (c:Cell) RETURN count(c)", {{"55"}});
    expectRows("MATCH ()-[r:ADJ]->() RETURN count(r)", {{"159"}});

    expectRows("MATCH (:Cell {x: 4, y: 3})-[:ADJ]-(n:Cell) RETURN n.x, n.y ORDER BY n.x, n.y",
               {{"3", "2"}, {"3", "3"}, {"3", "4"}, {"4", "2"}, {"4", "4"}});

    expectRows("MATCH (a:Cell {x: 2, y: 2}), (b:Cell {x: 8, y: 2}) RETURN EXISTS { (a)-[:SEES]-(b) }", {{"false"}});
    expectRows("MATCH (a:Cell {x: 2, y: 2}), (b:Cell {x: 2, y: 6}) RETURN EXISTS { (a)-[:SEES]-(b) }", {{"true"}});
}

TEST_F(DividedRoomTest, rendersTheMapWithThePlayerAndTheEnemies) {
    expectRows(RENDER,
               {{"0", "############"},
                {"1", "#....#.....#"},
                {"2", "#.@..#..1..#"},
                {"3", "#....#.....#"},
                {"4", "#....#.....#"},
                {"5", "#....#...2.#"},
                {"6", "#..........#"},
                {"7", "############"}});
}

TEST_F(DividedRoomTest, measuresTheDistanceToThePlayerAroundTheWall) {
    tick(STAND_STILL);

    expectRows("MATCH (c:Cell) "
               "WHERE (c.x = 2 AND c.y = 2) OR (c.x = 6 AND c.y = 6) OR (c.x = 8 AND c.y = 2) OR (c.x = 9 AND c.y = 5) "
               "RETURN c.x, c.y, toInteger(round(c.dist * 1000)) ORDER BY c.x, c.y",
               {{"2", "2", "0"}, {"6", "6", "6828"}, {"8", "2", "11657"}, {"9", "5", "10243"}});

    expectRows("MATCH (c:Cell) WHERE c.dist >= 1000.0 RETURN count(c)", {{"0"}});
    expectRows(CELLS_ANOTHER_ROUND_IMPROVES, {{"0"}});
}

TEST_F(DividedRoomTest, enemiesWalkAroundTheWallToThePlayer) {
    bool passedTheGap = false;
    for (size_t tickIndex = 0; tickIndex < 240; tickIndex++) {
        tick(STAND_STILL);
        ASSERT_FALSE(HasFatalFailure());

        Rows cell;
        read("MATCH (:Enemy {id: 1})-[:AT]->(c:Cell) RETURN c.x, c.y", cell);
        if (cell == Rows {{"5", "6"}}) {
            passedTheGap = true;
        }
    }

    EXPECT_TRUE(passedTheGap);

    expectRows(ENEMIES_IN_REACH, {{"1", "true"}, {"2", "true"}});
    expectRows("MATCH (p:Player) RETURN p.health < 100", {{"true"}});
}

TEST_F(DividedRoomTest, playerStopsAtTheWall) {
    ticks(WALK_FORWARD, 30);

    expectRows("MATCH (p:Player)-[:AT]->(c:Cell) "
               "RETURN c.x, c.y, toInteger(floor(p.px)), toInteger(p.py * 10), toInteger(c.dist)",
               {{"4", "2", "4", "25", "0"}});
}

TEST_F(DividedRoomTest, doesNotShootThroughTheWall) {
    tick(FIRE);

    expectRows("MATCH (e:Enemy) RETURN e.id, e.health ORDER BY e.id", {{"1", "30"}, {"2", "30"}});
}

TEST_F(DividedRoomTest, shootsTheNearestEnemyInFront) {
    write("MATCH (near:Cell {x: 2, y: 5}), (far:Cell {x: 2, y: 6}) "
          "CREATE (:Enemy {id: 3, px: 2.5, py: 5.5, health: 30, speed: 0.05})-[:AT]->(near), "
          "       (:Enemy {id: 4, px: 2.5, py: 6.5, health: 30, speed: 0.05})-[:AT]->(far)");
    commit();

    tick(TURN_SOUTH_AND_FIRE);

    expectRows("MATCH (e:Enemy) RETURN e.id, e.health ORDER BY e.id",
               {{"1", "30"}, {"2", "30"}, {"3", "15"}, {"4", "30"}});
}

TEST_F(DividedRoomTest, removesAnEnemyKilledByTwoShots) {
    write("MATCH (c:Cell {x: 2, y: 5}) CREATE (:Enemy {id: 3, px: 2.5, py: 5.5, health: 30, speed: 0.05})-[:AT]->(c)");
    commit();

    tick(TURN_SOUTH_AND_FIRE);
    tick(FIRE);

    expectRows("MATCH (e:Enemy) RETURN e.id ORDER BY e.id", {{"1"}, {"2"}});
    expectRows("MATCH ()-[a:AT]->() RETURN count(a)", {{"3"}});
}

TEST_F(ArenaTest, rendersA32By32Frame) {
    Rows lines;
    read(RENDER, lines);

    ASSERT_EQ(lines.size(), 32u);
    for (const Row& line : lines) {
        EXPECT_EQ(line[1].size(), 32u) << line[1];
    }

    EXPECT_EQ(lines[7][1][7], '@');
    EXPECT_EQ(lines[2][1][28], '1');
    EXPECT_EQ(lines[28][1][3], '2');
    EXPECT_EQ(lines[24][1][23], '3');
    EXPECT_EQ(lines[29][1][29], '4');
}

TEST_F(ArenaTest, settlesTheDistanceFieldInWidthPlusHeightRounds) {
    tick(STAND_STILL);

    expectRows("MATCH (l:Level) RETURN l.rounds", {{"64"}});
    expectRows("MATCH (c:Cell) WHERE c.dist >= 1000.0 RETURN count(c)", {{"0"}});
    expectRows(CELLS_ANOTHER_ROUND_IMPROVES, {{"0"}});
}

TEST_F(ArenaTest, enemiesFindThePlayerThroughTheDoors) {
    printFrame(0);

    for (size_t tickIndex = 1; tickIndex <= 200; tickIndex++) {
        tick(STAND_STILL);
        ASSERT_FALSE(HasFatalFailure());

        if (tickIndex % 50 == 0) {
            printFrame(tickIndex);
        }
    }

    expectRows(ENEMIES_IN_REACH, {{"1", "true"}, {"2", "true"}, {"3", "true"}, {"4", "true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
