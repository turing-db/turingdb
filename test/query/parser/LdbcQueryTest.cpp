#include <gtest/gtest.h>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// Copied from ldbc_snb_interactive_v1_impls (11db98c) and ldbc_snb_bi (47dd38b). Only the
// indentation differs
class LdbcQueryTest : public ExpectedASTTest {
};

TEST_F(LdbcQueryTest, parsesInteractiveComplex2) {
    expectParse(R"(
        // Q2. Recent messages by your friends
        /*
        :param [{ personId, maxDate }] => { RETURN
          10995116278009 AS personId,
          1287230400000 AS maxDate
        }
        */
        MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
            WHERE message.creationDate <= $maxDate
            RETURN
                friend.id AS personId,
                friend.firstName AS personFirstName,
                friend.lastName AS personLastName,
                message.id AS postOrCommentId,
                coalesce(message.content,message.imageFile) AS postOrCommentContent,
                message.creationDate AS postOrCommentCreationDate
            ORDER BY
                postOrCommentCreationDate DESC,
                toInteger(postOrCommentId) ASC
            LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "friend", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})})})},
                                                   where(binary(BinaryOperator::LessThanOrEqual,
                                                                property({"message", "creationDate"}),
                                                                parameter("maxDate"))))})},
                       returning(projection({._items = {named(property({"friend", "id"}), "personId"),
                                                        named(property({"friend", "firstName"}), "personFirstName"),
                                                        named(property({"friend", "lastName"}), "personLastName"),
                                                        named(property({"message", "id"}), "postOrCommentId"),
                                                        named(invocation({"coalesce"},
                                                                         {property({"message", "content"}),
                                                                          property({"message", "imageFile"})}),
                                                              "postOrCommentContent"),
                                                        named(property({"message", "creationDate"}),
                                                              "postOrCommentCreationDate")},
                                             ._orderBy = orderBy({{symbol("postOrCommentCreationDate"), OrderByType::DESC},
                                                                  {invocation({"toInteger"}, {symbol("postOrCommentId")}),
                                                                   OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex3) {
    expectParse(R"(
        // Q3. Friends and friends of friends that have been to given countries
        /*
        :param [{ personId, countryXName, countryYName, startDate, endDate }] => { RETURN
          6597069766734 AS personId,
          "Angola" AS countryXName,
          "Colombia" AS countryYName,
          1275393600000 AS startDate,
          1277812800000 AS endDate
        }
        */
        MATCH (countryX:Country {name: $countryXName }),
              (countryY:Country {name: $countryYName }),
              (person:Person {id: $personId })
        WITH person, countryX, countryY
        LIMIT 1
        MATCH (city:City)-[:IS_PART_OF]->(country:Country)
        WHERE country IN [countryX, countryY]
        WITH person, countryX, countryY, collect(city) AS cities
        MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
        WHERE NOT person=friend AND NOT city IN cities
        WITH DISTINCT friend, countryX, countryY
        MATCH (friend)<-[:HAS_CREATOR]-(message),
              (message)-[:IS_LOCATED_IN]->(country)
        WHERE $endDate > message.creationDate >= $startDate AND
              country IN [countryX, countryY]
        WITH friend,
             CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
             CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
        WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
        WHERE xCount>0 AND yCount>0
        RETURN friend.id AS friendId,
               friend.firstName AS friendFirstName,
               friend.lastName AS friendLastName,
               xCount,
               yCount,
               xCount + yCount AS xyCount
        ORDER BY xyCount DESC, friendId ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "countryX",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("countryXName")}})})}),
                                                    path({node({._symbol = "countryY",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("countryYName")}})})}),
                                                    path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})})})})}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    symbol("countryX"),
                                                                    symbol("countryY")},
                                                         ._limit = limit(integer(1))})}),
                        match({._pattern = pattern({path({node({._symbol = "city", ._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._symbol = "country", ._labels = labels({"Country"})})})},
                                                   where(binary(BinaryOperator::In,
                                                                symbol("country"),
                                                                list({symbol("countryX"), symbol("countryY")}))))}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    symbol("countryX"),
                                                                    symbol("countryY"),
                                                                    named(invocation({"collect"}, {symbol("city")}),
                                                                          "cities")}})}),
                        match({._pattern = pattern({path({node({._symbol = "person"}),
                                                          walk({._quantifier = quantifier(1, 2),
                                                                ._hops = {edge({._labels = labels({"KNOWS"})})}}),
                                                          node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "city"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {unary(UnaryOperator::Not,
                                                                        binary(BinaryOperator::Equal,
                                                                               symbol("person"),
                                                                               symbol("friend"))),
                                                                  unary(UnaryOperator::Not,
                                                                        binary(BinaryOperator::In,
                                                                               symbol("city"),
                                                                               symbol("cities")))})))}),
                        with({._projection = projection({._items = {symbol("friend"),
                                                                    symbol("countryX"),
                                                                    symbol("countryY")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message"})}),
                                                    path({node({._symbol = "message"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "country"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {comparisonChain(parameter("endDate"),
                                                                                  {{BinaryOperator::GreaterThan, property({"message", "creationDate"})},
                                                                                   {BinaryOperator::GreaterThanOrEqual, parameter("startDate")}}),
                                                                  binary(BinaryOperator::In,
                                                                         symbol("country"),
                                                                         list({symbol("countryX"),
                                                                               symbol("countryY")}))})))}),
                        with({._projection = projection({._items = {symbol("friend"),
                                                                    named(caseExpr(nullptr,
                                                                                   {{._tests = {{._value = binary(BinaryOperator::Equal,
                                                                                                                  symbol("country"),
                                                                                                                  symbol("countryX"))}},
                                                                                     ._then = integer(1)}},
                                                                                   integer(0)),
                                                                          "messageX"),
                                                                    named(caseExpr(nullptr,
                                                                                   {{._tests = {{._value = binary(BinaryOperator::Equal,
                                                                                                                  symbol("country"),
                                                                                                                  symbol("countryY"))}},
                                                                                     ._then = integer(1)}},
                                                                                   integer(0)),
                                                                          "messageY")}})}),
                        with({._projection = projection({._items = {symbol("friend"),
                                                                    named(invocation({"sum"}, {symbol("messageX")}),
                                                                          "xCount"),
                                                                    named(invocation({"sum"}, {symbol("messageY")}),
                                                                          "yCount")}}),
                              ._where = where(logical(LogicalOperator::And,
                                                      {binary(BinaryOperator::GreaterThan, symbol("xCount"), integer(0)),
                                                       binary(BinaryOperator::GreaterThan,
                                                              symbol("yCount"),
                                                              integer(0))}))})},
                       returning(projection({._items = {named(property({"friend", "id"}), "friendId"),
                                                        named(property({"friend", "firstName"}), "friendFirstName"),
                                                        named(property({"friend", "lastName"}), "friendLastName"),
                                                        symbol("xCount"),
                                                        symbol("yCount"),
                                                        named(binary(BinaryOperator::Add,
                                                                     symbol("xCount"),
                                                                     symbol("yCount")),
                                                              "xyCount")},
                                             ._orderBy = orderBy({{symbol("xyCount"), OrderByType::DESC},
                                                                  {symbol("friendId"), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex4) {
    expectParse(R"(
        // Q4. New topics
        /*
        :param [{ personId, startDate, endDate }] => { RETURN
          4398046511333 AS personId,
          1275350400000 AS startDate,
          1277856000000 AS endDate
        }
        */
        MATCH (person:Person {id: $personId })-[:KNOWS]-(friend:Person),
              (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
        WITH DISTINCT tag, post
        WITH tag,
             CASE
               WHEN $startDate <= post.creationDate < $endDate THEN 1
               ELSE 0
             END AS valid,
             CASE
               WHEN post.creationDate < $startDate THEN 1
               ELSE 0
             END AS inValid
        WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
        WHERE postCount>0 AND inValidPostCount=0
        RETURN tag.name AS tagName, postCount
        ORDER BY postCount DESC, tagName ASC
        LIMIT 10
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "friend", ._labels = labels({"Person"})})}),
                                                    path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})})})}),
                        with({._projection = projection({._items = {symbol("tag"), symbol("post")}, ._distinct = true})}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    named(caseExpr(nullptr,
                                                                                   {{._tests = {{._value = comparisonChain(parameter("startDate"),
                                                                                                                           {{BinaryOperator::LessThanOrEqual, property({"post", "creationDate"})},
                                                                                                                            {BinaryOperator::LessThan, parameter("endDate")}})}},
                                                                                     ._then = integer(1)}},
                                                                                   integer(0)),
                                                                          "valid"),
                                                                    named(caseExpr(nullptr,
                                                                                   {{._tests = {{._value = binary(BinaryOperator::LessThan,
                                                                                                                  property({"post", "creationDate"}),
                                                                                                                  parameter("startDate"))}},
                                                                                     ._then = integer(1)}},
                                                                                   integer(0)),
                                                                          "inValid")}})}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    named(invocation({"sum"}, {symbol("valid")}),
                                                                          "postCount"),
                                                                    named(invocation({"sum"}, {symbol("inValid")}),
                                                                          "inValidPostCount")}}),
                              ._where = where(logical(LogicalOperator::And,
                                                      {binary(BinaryOperator::GreaterThan, symbol("postCount"), integer(0)),
                                                       binary(BinaryOperator::Equal,
                                                              symbol("inValidPostCount"),
                                                              integer(0))}))})},
                       returning(projection({._items = {named(property({"tag", "name"}), "tagName"), symbol("postCount")},
                                             ._orderBy = orderBy({{symbol("postCount"), OrderByType::DESC},
                                                                  {symbol("tagName"), OrderByType::ASC}}),
                                             ._limit = limit(integer(10))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex5) {
    expectParse(R"(
        // Q5. New groups
        /*
        :param [{ personId, minDate }] => { RETURN
          6597069766734 AS personId,
          1288612800000 AS minDate
        }
        */
        MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
        WHERE
            NOT person=friend
        WITH DISTINCT friend
        MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
        WHERE
            membership.joinDate > $minDate
        WITH
            forum,
            collect(friend) AS friends
        OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
        WHERE
            friend IN friends
        WITH
            forum,
            count(post) AS postCount
        RETURN
            forum.title AS forumName,
            postCount
        ORDER BY
            postCount DESC,
            forum.id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          walk({._quantifier = quantifier(1, 2),
                                                                ._hops = {edge({._labels = labels({"KNOWS"})})}}),
                                                          node({._symbol = "friend"})})},
                                                   where(unary(UnaryOperator::Not,
                                                               binary(BinaryOperator::Equal,
                                                                      symbol("person"),
                                                                      symbol("friend")))))}),
                        with({._projection = projection({._items = {symbol("friend")}, ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "membership",
                                                                ._labels = labels({"HAS_MEMBER"})}),
                                                          node({._symbol = "forum"})})},
                                                   where(binary(BinaryOperator::GreaterThan,
                                                                property({"membership", "joinDate"}),
                                                                parameter("minDate"))))}),
                        with({._projection = projection({._items = {symbol("forum"),
                                                                    named(invocation({"collect"}, {symbol("friend")}),
                                                                          "friends")}})}),
                        match({._pattern = pattern({path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "post"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"CONTAINER_OF"})}),
                                                          node({._symbol = "forum"})})},
                                                   where(binary(BinaryOperator::In, symbol("friend"), symbol("friends")))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("forum"),
                                                                    named(invocation({"count"}, {symbol("post")}),
                                                                          "postCount")}})})},
                       returning(projection({._items = {named(property({"forum", "title"}), "forumName"),
                                                        symbol("postCount")},
                                             ._orderBy = orderBy({{symbol("postCount"), OrderByType::DESC},
                                                                  {property({"forum", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex6) {
    expectParse(R"(
        // Q6. Tag co-occurrence
        /*
        :param [{ personId, tagName }] => { RETURN
          4398046511333 AS personId,
          "Carl_Gustaf_Emil_Mannerheim" AS tagName
        }
        */
        MATCH (knownTag:Tag { name: $tagName })
        WITH knownTag.id as knownTagId

        MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
        WHERE NOT person=friend
        WITH
            knownTagId,
            collect(distinct friend) as friends
        UNWIND friends as f
            MATCH (f)<-[:HAS_CREATOR]-(post:Post),
                  (post)-[:HAS_TAG]->(t:Tag{id: knownTagId}),
                  (post)-[:HAS_TAG]->(tag:Tag)
            WHERE NOT t = tag
            WITH
                tag.name as tagName,
                count(post) as postCount
        RETURN
            tagName,
            postCount
        ORDER BY
            postCount DESC,
            tagName ASC
        LIMIT 10
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "knownTag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tagName")}})})})})}),
                        with({._projection = projection({._items = {named(property({"knownTag", "id"}), "knownTagId")}})}),
                        match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          walk({._quantifier = quantifier(1, 2),
                                                                ._hops = {edge({._labels = labels({"KNOWS"})})}}),
                                                          node({._symbol = "friend"})})},
                                                   where(unary(UnaryOperator::Not,
                                                               binary(BinaryOperator::Equal,
                                                                      symbol("person"),
                                                                      symbol("friend")))))}),
                        with({._projection = projection({._items = {symbol("knownTagId"),
                                                                    named(distinct(invocation({"collect"},
                                                                                              {symbol("friend")})),
                                                                          "friends")}})}),
                        unwind(symbol("friends"), "f"),
                        match({._pattern = pattern({path({node({._symbol = "f"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})})}),
                                                    path({node({._symbol = "post"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "t",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"id", symbol("knownTagId")}})})}),
                                                    path({node({._symbol = "post"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag", ._labels = labels({"Tag"})})})},
                                                   where(unary(UnaryOperator::Not,
                                                               binary(BinaryOperator::Equal,
                                                                      symbol("t"),
                                                                      symbol("tag")))))}),
                        with({._projection = projection({._items = {named(property({"tag", "name"}), "tagName"),
                                                                    named(invocation({"count"}, {symbol("post")}),
                                                                          "postCount")}})})},
                       returning(projection({._items = {symbol("tagName"), symbol("postCount")},
                                             ._orderBy = orderBy({{symbol("postCount"), OrderByType::DESC},
                                                                  {symbol("tagName"), OrderByType::ASC}}),
                                             ._limit = limit(integer(10))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex7) {
    expectParse(R"(
        // Q7. Recent likers
        /*
        :param personId: 4398046511268
        */
        MATCH (person:Person {id: $personId})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
            WITH liker, message, like.creationDate AS likeTime, person
            ORDER BY likeTime DESC, toInteger(message.id) ASC
            WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
        RETURN
            liker.id AS personId,
            liker.firstName AS personFirstName,
            liker.lastName AS personLastName,
            latestLike.likeTime AS likeCreationDate,
            latestLike.msg.id AS commentOrPostId,
            coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
            toInteger(floor(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency,
            not((liker)-[:KNOWS]-(person)) AS isNew
        ORDER BY
            likeCreationDate DESC,
            toInteger(personId) ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "like",
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "liker", ._labels = labels({"Person"})})})})}),
                        with({._projection = projection({._items = {symbol("liker"),
                                                                    symbol("message"),
                                                                    named(property({"like", "creationDate"}), "likeTime"),
                                                                    symbol("person")},
                                                         ._orderBy = orderBy({{symbol("likeTime"), OrderByType::DESC},
                                                                              {invocation({"toInteger"},
                                                                                          {property({"message", "id"})}),
                                                                               OrderByType::ASC}})})}),
                        with({._projection = projection({._items = {symbol("liker"),
                                                                    named(invocation({"head"},
                                                                                     {invocation({"collect"},
                                                                                                 {map({{"msg", symbol("message")},
                                                                                                       {"likeTime", symbol("likeTime")}})})}),
                                                                          "latestLike"),
                                                                    symbol("person")}})})},
                       returning(projection({._items = {named(property({"liker", "id"}), "personId"),
                                                        named(property({"liker", "firstName"}), "personFirstName"),
                                                        named(property({"liker", "lastName"}), "personLastName"),
                                                        named(property({"latestLike", "likeTime"}), "likeCreationDate"),
                                                        named(lookup(property({"latestLike", "msg"}), "id"), "commentOrPostId"),
                                                        named(invocation({"coalesce"},
                                                                         {lookup(property({"latestLike", "msg"}), "content"),
                                                                          lookup(property({"latestLike", "msg"}), "imageFile")}),
                                                              "commentOrPostContent"),
                                                        named(invocation({"toInteger"},
                                                                         {binary(BinaryOperator::Div,
                                                                                 invocation({"floor"},
                                                                                            {binary(BinaryOperator::Div,
                                                                                                    invocation({"toFloat"},
                                                                                                               {binary(BinaryOperator::Sub,
                                                                                                                       property({"latestLike", "likeTime"}),
                                                                                                                       lookup(property({"latestLike", "msg"}), "creationDate"))}),
                                                                                                    real(1000.0))}),
                                                                                 real(60.0))}),
                                                              "minutesLatency"),
                                                        named(unary(UnaryOperator::Not,
                                                                    parenthesized(patternPredicate(pattern({path({node({._symbol = "liker"}),
                                                                                                                  edge({._labels = labels({"KNOWS"})}),
                                                                                                                  node({._symbol = "person"})})})))),
                                                              "isNew")},
                                             ._orderBy = orderBy({{symbol("likeCreationDate"), OrderByType::DESC},
                                                                  {invocation({"toInteger"}, {symbol("personId")}),
                                                                   OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex8) {
    expectParse(R"(
        // Q8. Recent replies
        /*
        :param personId: 143
        */
        MATCH (start:Person {id: $personId})<-[:HAS_CREATOR]-(:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
        RETURN
            person.id AS personId,
            person.firstName AS personFirstName,
            person.lastName AS personLastName,
            comment.creationDate AS commentCreationDate,
            comment.id AS commentId,
            comment.content AS commentContent
        ORDER BY
            commentCreationDate DESC,
            commentId ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "start",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._symbol = "comment", ._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person", ._labels = labels({"Person"})})})})})},
                       returning(projection({._items = {named(property({"person", "id"}), "personId"),
                                                        named(property({"person", "firstName"}), "personFirstName"),
                                                        named(property({"person", "lastName"}), "personLastName"),
                                                        named(property({"comment", "creationDate"}), "commentCreationDate"),
                                                        named(property({"comment", "id"}), "commentId"),
                                                        named(property({"comment", "content"}), "commentContent")},
                                             ._orderBy = orderBy({{symbol("commentCreationDate"), OrderByType::DESC},
                                                                  {symbol("commentId"), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex9) {
    expectParse(R"(
        // Q9. Recent messages by friends or friends of friends
        /*
        :param [{ personId, maxDate }] => { RETURN
          4398046511268 AS personId,
          1289908800000 AS maxDate
        }
        */
        MATCH (root:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
        WHERE NOT friend = root
        WITH collect(distinct friend) as friends
        UNWIND friends as friend
            MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
            WHERE message.creationDate < $maxDate
        RETURN
            friend.id AS personId,
            friend.firstName AS personFirstName,
            friend.lastName AS personLastName,
            message.id AS commentOrPostId,
            coalesce(message.content,message.imageFile) AS commentOrPostContent,
            message.creationDate AS commentOrPostCreationDate
        ORDER BY
            commentOrPostCreationDate DESC,
            message.id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "root",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          walk({._quantifier = quantifier(1, 2),
                                                                ._hops = {edge({._labels = labels({"KNOWS"})})}}),
                                                          node({._symbol = "friend", ._labels = labels({"Person"})})})},
                                                   where(unary(UnaryOperator::Not,
                                                               binary(BinaryOperator::Equal,
                                                                      symbol("friend"),
                                                                      symbol("root")))))}),
                        with({._projection = projection({._items = {named(distinct(invocation({"collect"},
                                                                                              {symbol("friend")})),
                                                                          "friends")}})}),
                        unwind(symbol("friends"), "friend"),
                        match({._pattern = pattern({path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})})})},
                                                   where(binary(BinaryOperator::LessThan,
                                                                property({"message", "creationDate"}),
                                                                parameter("maxDate"))))})},
                       returning(projection({._items = {named(property({"friend", "id"}), "personId"),
                                                        named(property({"friend", "firstName"}), "personFirstName"),
                                                        named(property({"friend", "lastName"}), "personLastName"),
                                                        named(property({"message", "id"}), "commentOrPostId"),
                                                        named(invocation({"coalesce"},
                                                                         {property({"message", "content"}),
                                                                          property({"message", "imageFile"})}),
                                                              "commentOrPostContent"),
                                                        named(property({"message", "creationDate"}),
                                                              "commentOrPostCreationDate")},
                                             ._orderBy = orderBy({{symbol("commentOrPostCreationDate"), OrderByType::DESC},
                                                                  {property({"message", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex10) {
    expectParse(R"(
        // Q10. Friend recommendation
        /*
        :param [{ personId, month }] => { RETURN
          4398046511333 AS personId,
          5 AS month
        }
        */
        MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
               (friend)-[:IS_LOCATED_IN]->(city:City)
        WHERE NOT friend=person AND
              NOT (friend)-[:KNOWS]-(person)
        WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
        WHERE  (birthday.month=$month AND birthday.day>=21) OR
                (birthday.month=($month%12)+1 AND birthday.day<22)
        WITH DISTINCT friend, city, person
        OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
        WITH friend, city, collect(post) AS posts, person
        WITH friend,
             city,
             size(posts) AS postCount,
             size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
        RETURN friend.id AS personId,
               friend.firstName AS personFirstName,
               friend.lastName AS personLastName,
               commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
               friend.gender AS personGender,
               city.name AS personCityName
        ORDER BY commonInterestScore DESC, personId ASC
        LIMIT 10
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          walk({._quantifier = quantifier(2, 2),
                                                                ._hops = {edge({._labels = labels({"KNOWS"})})}}),
                                                          node({._symbol = "friend"})}),
                                                    path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "city", ._labels = labels({"City"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {unary(UnaryOperator::Not,
                                                                        binary(BinaryOperator::Equal,
                                                                               symbol("friend"),
                                                                               symbol("person"))),
                                                                  unary(UnaryOperator::Not,
                                                                        patternPredicate(pattern({path({node({._symbol = "friend"}),
                                                                                                        edge({._labels = labels({"KNOWS"})}),
                                                                                                        node({._symbol = "person"})})})))})))}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    symbol("city"),
                                                                    symbol("friend"),
                                                                    named(invocation({"datetime"},
                                                                                     {map({{"epochMillis", property({"friend", "birthday"})}})}),
                                                                          "birthday")}}),
                              ._where = where(logical(LogicalOperator::Or,
                                                      {parenthesized(logical(LogicalOperator::And,
                                                                             {binary(BinaryOperator::Equal,
                                                                                     property({"birthday", "month"}),
                                                                                     parameter("month")),
                                                                              binary(BinaryOperator::GreaterThanOrEqual,
                                                                                     property({"birthday", "day"}),
                                                                                     integer(21))})),
                                                       parenthesized(logical(LogicalOperator::And,
                                                                             {binary(BinaryOperator::Equal,
                                                                                     property({"birthday", "month"}),
                                                                                     binary(BinaryOperator::Add,
                                                                                            parenthesized(binary(BinaryOperator::Mod,
                                                                                                                 parameter("month"),
                                                                                                                 integer(12))),
                                                                                            integer(1))),
                                                                              binary(BinaryOperator::LessThan,
                                                                                     property({"birthday", "day"}),
                                                                                     integer(22))}))}))}),
                        with({._projection = projection({._items = {symbol("friend"), symbol("city"), symbol("person")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("friend"),
                                                                    symbol("city"),
                                                                    named(invocation({"collect"}, {symbol("post")}),
                                                                          "posts"),
                                                                    symbol("person")}})}),
                        with({._projection = projection({._items = {symbol("friend"),
                                                                    symbol("city"),
                                                                    named(invocation({"size"}, {symbol("posts")}),
                                                                          "postCount"),
                                                                    named(invocation({"size"},
                                                                                     {listComprehension("p",
                                                                                                        symbol("posts"),
                                                                                                        patternPredicate(pattern({path({node({._symbol = "p"}),
                                                                                                                                        edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labels({"HAS_TAG"})}),
                                                                                                                                        node({}),
                                                                                                                                        edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                                              ._labels = labels({"HAS_INTEREST"})}),
                                                                                                                                        node({._symbol = "person"})})})))}),
                                                                          "commonPostCount")}})})},
                       returning(projection({._items = {named(property({"friend", "id"}), "personId"),
                                                        named(property({"friend", "firstName"}), "personFirstName"),
                                                        named(property({"friend", "lastName"}), "personLastName"),
                                                        named(binary(BinaryOperator::Sub,
                                                                     symbol("commonPostCount"),
                                                                     parenthesized(binary(BinaryOperator::Sub,
                                                                                          symbol("postCount"),
                                                                                          symbol("commonPostCount")))),
                                                              "commonInterestScore"),
                                                        named(property({"friend", "gender"}), "personGender"),
                                                        named(property({"city", "name"}), "personCityName")},
                                             ._orderBy = orderBy({{symbol("commonInterestScore"), OrderByType::DESC},
                                                                  {symbol("personId"), OrderByType::ASC}}),
                                             ._limit = limit(integer(10))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex11) {
    expectParse(R"(
        // Q11. Job referral
        /*
        :param [{ personId, countryName, workFromYear }] => { RETURN
          10995116277918 AS personId,
          "Hungary" AS countryName,
          2011 AS workFromYear
        }
        */
        MATCH (person:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
        WHERE not(person=friend)
        WITH DISTINCT friend
        MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: $countryName })
        WHERE workAt.workFrom < $workFromYear
        RETURN
                friend.id AS personId,
                friend.firstName AS personFirstName,
                friend.lastName AS personLastName,
                company.name AS organizationName,
                workAt.workFrom AS organizationWorkFromYear
        ORDER BY
                organizationWorkFromYear ASC,
                toInteger(personId) ASC,
                organizationName DESC
        LIMIT 10
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          walk({._quantifier = quantifier(1, 2),
                                                                ._hops = {edge({._labels = labels({"KNOWS"})})}}),
                                                          node({._symbol = "friend", ._labels = labels({"Person"})})})},
                                                   where(unary(UnaryOperator::Not,
                                                               parenthesized(binary(BinaryOperator::Equal,
                                                                                    symbol("person"),
                                                                                    symbol("friend"))))))}),
                        with({._projection = projection({._items = {symbol("friend")}, ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "friend"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._symbol = "workAt",
                                                                ._labels = labels({"WORK_AT"})}),
                                                          node({._symbol = "company", ._labels = labels({"Company"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("countryName")}})})})},
                                                   where(binary(BinaryOperator::LessThan,
                                                                property({"workAt", "workFrom"}),
                                                                parameter("workFromYear"))))})},
                       returning(projection({._items = {named(property({"friend", "id"}), "personId"),
                                                        named(property({"friend", "firstName"}), "personFirstName"),
                                                        named(property({"friend", "lastName"}), "personLastName"),
                                                        named(property({"company", "name"}), "organizationName"),
                                                        named(property({"workAt", "workFrom"}),
                                                              "organizationWorkFromYear")},
                                             ._orderBy = orderBy({{symbol("organizationWorkFromYear"), OrderByType::ASC},
                                                                  {invocation({"toInteger"}, {symbol("personId")}),
                                                                   OrderByType::ASC},
                                                                  {symbol("organizationName"), OrderByType::DESC}}),
                                             ._limit = limit(integer(10))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveComplex12) {
    expectParse(R"(
        // Q12. Expert search
        /*
        :param [{ personId, tagClassName }] => { RETURN
          10995116278009 AS personId,
          "Monarch" AS tagClassName
        }
        */
        MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
        WHERE tag.name = $tagClassName OR baseTagClass.name = $tagClassName
        WITH collect(tag.id) as tags
        MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
        WHERE tag.id in tags
        RETURN
            friend.id AS personId,
            friend.firstName AS personFirstName,
            friend.lastName AS personLastName,
            collect(DISTINCT tag.name) AS tagNames,
            count(DISTINCT comment) AS replyCount
        ORDER BY
            replyCount DESC,
            toInteger(personId) ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag", ._labels = labels({"Tag"})}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                ._labels = labelOr({labels({"HAS_TYPE"}),
                                                                                                    labels({"IS_SUBCLASS_OF"})})})}}),
                                                          node({._symbol = "baseTagClass",
                                                                ._labels = labels({"TagClass"})})})},
                                                   where(logical(LogicalOperator::Or,
                                                                 {binary(BinaryOperator::Equal,
                                                                         property({"tag", "name"}),
                                                                         parameter("tagClassName")),
                                                                  binary(BinaryOperator::Equal,
                                                                         property({"baseTagClass", "name"}),
                                                                         parameter("tagClassName"))})))}),
                        with({._projection = projection({._items = {named(invocation({"collect"},
                                                                                     {property({"tag", "id"})}),
                                                                          "tags")}})}),
                        match({._pattern = pattern({path({node({._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "friend", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "comment", ._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._labels = labels({"Post"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag", ._labels = labels({"Tag"})})})},
                                                   where(binary(BinaryOperator::In,
                                                                property({"tag", "id"}),
                                                                symbol("tags"))))})},
                       returning(projection({._items = {named(property({"friend", "id"}), "personId"),
                                                        named(property({"friend", "firstName"}), "personFirstName"),
                                                        named(property({"friend", "lastName"}), "personLastName"),
                                                        named(distinct(invocation({"collect"},
                                                                                  {property({"tag", "name"})})),
                                                              "tagNames"),
                                                        named(distinct(invocation({"count"}, {symbol("comment")})),
                                                              "replyCount")},
                                             ._orderBy = orderBy({{symbol("replyCount"), OrderByType::DESC},
                                                                  {invocation({"toInteger"}, {symbol("personId")}),
                                                                   OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort1) {
    expectParse(R"(
        // IS1. Profile of a person
        /*
        :param personId: 10995116277794
         */
        MATCH (n:Person {id: $personId })-[:IS_LOCATED_IN]->(p:City)
        RETURN
            n.firstName AS firstName,
            n.lastName AS lastName,
            n.birthday AS birthday,
            n.locationIP AS locationIP,
            n.browserUsed AS browserUsed,
            p.id AS cityId,
            n.gender AS gender,
            n.creationDate AS creationDate
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "n",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "p", ._labels = labels({"City"})})})})})},
                       returning({named(property({"n", "firstName"}), "firstName"),
                                  named(property({"n", "lastName"}), "lastName"),
                                  named(property({"n", "birthday"}), "birthday"),
                                  named(property({"n", "locationIP"}), "locationIP"),
                                  named(property({"n", "browserUsed"}), "browserUsed"),
                                  named(property({"p", "id"}), "cityId"),
                                  named(property({"n", "gender"}), "gender"),
                                  named(property({"n", "creationDate"}), "creationDate")}))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort2) {
    expectParse(R"(
        // IS2. Recent messages of a person
        /*
        :param personId: 10995116277795
         */
        MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(message)
        WITH
         message,
         message.id AS messageId,
         message.creationDate AS messageCreationDate
        ORDER BY messageCreationDate DESC, messageId ASC
        LIMIT 10
        MATCH (message)-[:REPLY_OF*0..]->(post:Post),
              (post)-[:HAS_CREATOR]->(person)
        RETURN
         messageId,
         coalesce(message.imageFile,message.content) AS messageContent,
         messageCreationDate,
         post.id AS postId,
         person.id AS personId,
         person.firstName AS personFirstName,
         person.lastName AS personLastName
        ORDER BY messageCreationDate DESC, messageId ASC
    )",
                {query({match({._pattern = pattern({path({node({._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message"})})})}),
                        with({._projection = projection({._items = {symbol("message"),
                                                                    named(property({"message", "id"}), "messageId"),
                                                                    named(property({"message", "creationDate"}),
                                                                          "messageCreationDate")},
                                                         ._orderBy = orderBy({{symbol("messageCreationDate"),
                                                                               OrderByType::DESC},
                                                                              {symbol("messageId"), OrderByType::ASC}}),
                                                         ._limit = limit(integer(10))})}),
                        match({._pattern = pattern({path({node({._symbol = "message"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})})}),
                                                    path({node({._symbol = "post"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person"})})})})},
                       returning(projection({._items = {symbol("messageId"),
                                                        named(invocation({"coalesce"},
                                                                         {property({"message", "imageFile"}),
                                                                          property({"message", "content"})}),
                                                              "messageContent"),
                                                        symbol("messageCreationDate"),
                                                        named(property({"post", "id"}), "postId"),
                                                        named(property({"person", "id"}), "personId"),
                                                        named(property({"person", "firstName"}), "personFirstName"),
                                                        named(property({"person", "lastName"}), "personLastName")},
                                             ._orderBy = orderBy({{symbol("messageCreationDate"), OrderByType::DESC},
                                                                  {symbol("messageId"), OrderByType::ASC}})})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort3) {
    expectParse(R"(
        // IS3. Friends of a person
        /*
        :param personId: 10995116277794
         */
        MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
        RETURN
            friend.id AS personId,
            friend.firstName AS firstName,
            friend.lastName AS lastName,
            r.creationDate AS friendshipCreationDate
        ORDER BY
            friendshipCreationDate DESC,
            toInteger(personId) ASC
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "n",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})}),
                                                          edge({._symbol = "r", ._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "friend"})})})})},
                       returning(projection({._items = {named(property({"friend", "id"}), "personId"),
                                                        named(property({"friend", "firstName"}), "firstName"),
                                                        named(property({"friend", "lastName"}), "lastName"),
                                                        named(property({"r", "creationDate"}), "friendshipCreationDate")},
                                             ._orderBy = orderBy({{symbol("friendshipCreationDate"), OrderByType::DESC},
                                                                  {invocation({"toInteger"}, {symbol("personId")}),
                                                                   OrderByType::ASC}})})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort4) {
    expectParse(R"(
        // IS4. Content of a message
        /*
        :param messageId: 206158431836
         */
        MATCH (m:Message {id:  $messageId })
        RETURN
            m.creationDate as messageCreationDate,
            coalesce(m.content, m.imageFile) as messageContent
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "m",
                                                                ._labels = labels({"Message"}),
                                                                ._properties = properties({{"id", parameter("messageId")}})})})})})},
                       returning({named(property({"m", "creationDate"}), "messageCreationDate"),
                                  named(invocation({"coalesce"},
                                                   {property({"m", "content"}), property({"m", "imageFile"})}),
                                        "messageContent")}))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort5) {
    expectParse(R"(
        // IS5. Creator of a message
        /*
        :param messageId: 206158431836
         */
        MATCH (m:Message {id:  $messageId })-[:HAS_CREATOR]->(p:Person)
        RETURN
            p.id AS personId,
            p.firstName AS firstName,
            p.lastName AS lastName
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "m",
                                                                ._labels = labels({"Message"}),
                                                                ._properties = properties({{"id", parameter("messageId")}})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "p", ._labels = labels({"Person"})})})})})},
                       returning({named(property({"p", "id"}), "personId"),
                                  named(property({"p", "firstName"}), "firstName"),
                                  named(property({"p", "lastName"}), "lastName")}))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort6) {
    expectParse(R"(
        // IS6. Forum of a message
        /*
        :param messageId: 206158431836
         */
        MATCH (m:Message {id: $messageId })-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
        RETURN
            f.id AS forumId,
            f.title AS forumTitle,
            mod.id AS moderatorId,
            mod.firstName AS moderatorFirstName,
            mod.lastName AS moderatorLastName
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "m",
                                                                ._labels = labels({"Message"}),
                                                                ._properties = properties({{"id", parameter("messageId")}})}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "p", ._labels = labels({"Post"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"CONTAINER_OF"})}),
                                                          node({._symbol = "f", ._labels = labels({"Forum"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_MODERATOR"})}),
                                                          node({._symbol = "mod", ._labels = labels({"Person"})})})})})},
                       returning({named(property({"f", "id"}), "forumId"),
                                  named(property({"f", "title"}), "forumTitle"),
                                  named(property({"mod", "id"}), "moderatorId"),
                                  named(property({"mod", "firstName"}), "moderatorFirstName"),
                                  named(property({"mod", "lastName"}), "moderatorLastName")}))});
}

TEST_F(LdbcQueryTest, parsesInteractiveShort7) {
    expectParse(R"(
        // IS7. Replies of a message
        /*
        :param messageId: 206158432794
         */
        MATCH (m:Message {id: $messageId })<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
            OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
            RETURN c.id AS commentId,
                c.content AS commentContent,
                c.creationDate AS commentCreationDate,
                p.id AS replyAuthorId,
                p.firstName AS replyAuthorFirstName,
                p.lastName AS replyAuthorLastName,
                CASE r
                    WHEN null THEN false
                    ELSE true
                END AS replyAuthorKnowsOriginalMessageAuthor
            ORDER BY commentCreationDate DESC, replyAuthorId
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "m",
                                                                ._labels = labels({"Message"}),
                                                                ._properties = properties({{"id", parameter("messageId")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._symbol = "c", ._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "p", ._labels = labels({"Person"})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "m"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "a", ._labels = labels({"Person"})}),
                                                          edge({._symbol = "r", ._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "p"})})}),
                               ._optional = true})},
                       returning(projection({._items = {named(property({"c", "id"}), "commentId"),
                                                        named(property({"c", "content"}), "commentContent"),
                                                        named(property({"c", "creationDate"}), "commentCreationDate"),
                                                        named(property({"p", "id"}), "replyAuthorId"),
                                                        named(property({"p", "firstName"}), "replyAuthorFirstName"),
                                                        named(property({"p", "lastName"}), "replyAuthorLastName"),
                                                        named(caseExpr(symbol("r"),
                                                                       {{._tests = {{._value = null()}},
                                                                         ._then = boolean(false)}},
                                                                       boolean(true)),
                                                              "replyAuthorKnowsOriginalMessageAuthor")},
                                             ._orderBy = orderBy({{symbol("commentCreationDate"), OrderByType::DESC},
                                                                  {symbol("replyAuthorId"), OrderByType::ASC}})})))});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate1) {
    expectParse(R"(
        MATCH (c:City {id: $cityId})
        CREATE (p:Person {
            id: $personId,
            firstName: $personFirstName,
            lastName: $personLastName,
            gender: $gender,
            birthday: $birthday,
            creationDate: $creationDate,
            locationIP: $locationIP,
            browserUsed: $browserUsed,
            languages: $languages,
            email: $emails
          })-[:IS_LOCATED_IN]->(c)
        WITH p, count(*) AS dummy1
        UNWIND $tagIds AS tagId
          MATCH (t:Tag {id: tagId})
          CREATE (p)-[:HAS_INTEREST]->(t)
        WITH p, count(*) AS dummy2
        UNWIND $studyAt AS s
          MATCH (u:Organisation {id: s[0]})
          CREATE (p)-[:STUDY_AT {classYear: s[1]}]->(u)
        WITH p, count(*) AS dummy3
        UNWIND $workAt AS w
          MATCH (comp:Organisation {id: w[0]})
          CREATE (p)-[:WORKS_AT {workFrom: w[1]}]->(comp)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "c",
                                                                ._labels = labels({"City"}),
                                                                ._properties = properties({{"id", parameter("cityId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "p",
                                                        ._labels = labels({"Person"}),
                                                        ._properties = properties({{"id", parameter("personId")},
                                                                                   {"firstName", parameter("personFirstName")},
                                                                                   {"lastName", parameter("personLastName")},
                                                                                   {"gender", parameter("gender")},
                                                                                   {"birthday", parameter("birthday")},
                                                                                   {"creationDate", parameter("creationDate")},
                                                                                   {"locationIP", parameter("locationIP")},
                                                                                   {"browserUsed", parameter("browserUsed")},
                                                                                   {"languages", parameter("languages")},
                                                                                   {"email", parameter("emails")}})}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"IS_LOCATED_IN"})}),
                                                  node({._symbol = "c"})})})),
                        with({._projection = projection({._items = {symbol("p"),
                                                                    named(invocation({"count"}, {wildcard()}),
                                                                          "dummy1")}})}),
                        unwind(parameter("tagIds"), "tagId"),
                        match({._pattern = pattern({path({node({._symbol = "t",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"id", symbol("tagId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "p"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"HAS_INTEREST"})}),
                                                  node({._symbol = "t"})})})),
                        with({._projection = projection({._items = {symbol("p"),
                                                                    named(invocation({"count"}, {wildcard()}),
                                                                          "dummy2")}})}),
                        unwind(parameter("studyAt"), "s"),
                        match({._pattern = pattern({path({node({._symbol = "u",
                                                                ._labels = labels({"Organisation"}),
                                                                ._properties = properties({{"id", index(symbol("s"),
                                                                                                        integer(0))}})})})})}),
                        createStmt(pattern({path({node({._symbol = "p"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"STUDY_AT"}),
                                                        ._properties = properties({{"classYear", index(symbol("s"),
                                                                                                       integer(1))}})}),
                                                  node({._symbol = "u"})})})),
                        with({._projection = projection({._items = {symbol("p"),
                                                                    named(invocation({"count"}, {wildcard()}),
                                                                          "dummy3")}})}),
                        unwind(parameter("workAt"), "w"),
                        match({._pattern = pattern({path({node({._symbol = "comp",
                                                                ._labels = labels({"Organisation"}),
                                                                ._properties = properties({{"id", index(symbol("w"),
                                                                                                        integer(0))}})})})})}),
                        createStmt(pattern({path({node({._symbol = "p"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"WORKS_AT"}),
                                                        ._properties = properties({{"workFrom", index(symbol("w"),
                                                                                                      integer(1))}})}),
                                                  node({._symbol = "comp"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate2) {
    expectParse(R"(
        MATCH (person:Person {id: $personId}), (post:Post {id: $postId})
        CREATE (person)-[:LIKES {creationDate: $creationDate}]->(post)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})})}),
                                                    path({node({._symbol = "post",
                                                                ._labels = labels({"Post"}),
                                                                ._properties = properties({{"id", parameter("postId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "person"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"LIKES"}),
                                                        ._properties = properties({{"creationDate", parameter("creationDate")}})}),
                                                  node({._symbol = "post"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate3) {
    expectParse(R"(
        MATCH (person:Person {id: $personId}), (comment:Comment {id: $commentId})
        CREATE (person)-[:LIKES {creationDate: $creationDate}]->(comment)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})})}),
                                                    path({node({._symbol = "comment",
                                                                ._labels = labels({"Comment"}),
                                                                ._properties = properties({{"id", parameter("commentId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "person"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"LIKES"}),
                                                        ._properties = properties({{"creationDate", parameter("creationDate")}})}),
                                                  node({._symbol = "comment"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate4) {
    expectParse(R"(
        MATCH (p:Person {id: $moderatorPersonId})
        CREATE (f:Forum {id: $forumId, title: $forumTitle, creationDate: $creationDate})-[:HAS_MODERATOR]->(p)
        WITH f
        UNWIND $tagIds AS tagId
          MATCH (t:Tag {id: tagId})
          CREATE (f)-[:HAS_TAG]->(t)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "p",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("moderatorPersonId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "f",
                                                        ._labels = labels({"Forum"}),
                                                        ._properties = properties({{"id", parameter("forumId")},
                                                                                   {"title", parameter("forumTitle")},
                                                                                   {"creationDate", parameter("creationDate")}})}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"HAS_MODERATOR"})}),
                                                  node({._symbol = "p"})})})),
                        with({._projection = projection({._items = {symbol("f")}})}),
                        unwind(parameter("tagIds"), "tagId"),
                        match({._pattern = pattern({path({node({._symbol = "t",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"id", symbol("tagId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "f"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"HAS_TAG"})}),
                                                  node({._symbol = "t"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate5) {
    expectParse(R"(
        MATCH (f:Forum {id: $forumId}), (p:Person {id: $personId})
        CREATE (f)-[:HAS_MEMBER {joinDate: $joinDate}]->(p)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "f",
                                                                ._labels = labels({"Forum"}),
                                                                ._properties = properties({{"id", parameter("forumId")}})})}),
                                                    path({node({._symbol = "p",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "f"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"HAS_MEMBER"}),
                                                        ._properties = properties({{"joinDate", parameter("joinDate")}})}),
                                                  node({._symbol = "p"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate6) {
    expectParse(R"(
        MATCH (author:Person {id: $authorPersonId}), (country:Country {id: $countryId}), (forum:Forum {id: $forumId})
        CREATE (author)<-[:HAS_CREATOR]-(p:Post:Message {
            id: $postId,
            creationDate: $creationDate,
            locationIP: $locationIP,
            browserUsed: $browserUsed,
            language: $language,
            content: CASE $content WHEN '' THEN NULL ELSE $content END,
            imageFile: CASE $imageFile WHEN '' THEN NULL ELSE $imageFile END,
            length: $length
          })<-[:CONTAINER_OF]-(forum), (p)-[:IS_LOCATED_IN]->(country)
        WITH p
        UNWIND $tagIds AS tagId
          MATCH (t:Tag {id: tagId})
          CREATE (p)-[:HAS_TAG]->(t)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "author",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("authorPersonId")}})})}),
                                                    path({node({._symbol = "country",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"id", parameter("countryId")}})})}),
                                                    path({node({._symbol = "forum",
                                                                ._labels = labels({"Forum"}),
                                                                ._properties = properties({{"id", parameter("forumId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "author"}),
                                                  edge({._direction = EdgePattern::Direction::Backward,
                                                        ._labels = labels({"HAS_CREATOR"})}),
                                                  node({._symbol = "p",
                                                        ._labels = labels({"Post", "Message"}),
                                                        ._properties = properties({{"id", parameter("postId")},
                                                                                   {"creationDate", parameter("creationDate")},
                                                                                   {"locationIP", parameter("locationIP")},
                                                                                   {"browserUsed", parameter("browserUsed")},
                                                                                   {"language", parameter("language")},
                                                                                   {"content", caseExpr(parameter("content"),
                                                                                                        {{._tests = {{._value = string("")}},
                                                                                                          ._then = null()}},
                                                                                                        parameter("content"))},
                                                                                   {"imageFile", caseExpr(parameter("imageFile"),
                                                                                                          {{._tests = {{._value = string("")}},
                                                                                                            ._then = null()}},
                                                                                                          parameter("imageFile"))},
                                                                                   {"length", parameter("length")}})}),
                                                  edge({._direction = EdgePattern::Direction::Backward,
                                                        ._labels = labels({"CONTAINER_OF"})}),
                                                  node({._symbol = "forum"})}),
                                            path({node({._symbol = "p"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"IS_LOCATED_IN"})}),
                                                  node({._symbol = "country"})})})),
                        with({._projection = projection({._items = {symbol("p")}})}),
                        unwind(parameter("tagIds"), "tagId"),
                        match({._pattern = pattern({path({node({._symbol = "t",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"id", symbol("tagId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "p"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"HAS_TAG"})}),
                                                  node({._symbol = "t"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate7) {
    expectParse(R"(
        MATCH
          (author:Person {id: $authorPersonId}),
          (country:Country {id: $countryId}),
          (message:Message {id: $replyToPostId + $replyToCommentId + 1}) // $replyToCommentId is -1 if the message is a reply to a post and vica versa (see spec)
        CREATE (author)<-[:HAS_CREATOR]-(c:Comment:Message {
            id: $commentId,
            creationDate: $creationDate,
            locationIP: $locationIP,
            browserUsed: $browserUsed,
            content: $content,
            length: $length
          })-[:REPLY_OF]->(message),
          (c)-[:IS_LOCATED_IN]->(country)
        WITH c
        UNWIND $tagIds AS tagId
          MATCH (t:Tag {id: tagId})
          CREATE (c)-[:HAS_TAG]->(t)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "author",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("authorPersonId")}})})}),
                                                    path({node({._symbol = "country",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"id", parameter("countryId")}})})}),
                                                    path({node({._symbol = "message",
                                                                ._labels = labels({"Message"}),
                                                                ._properties = properties({{"id", binary(BinaryOperator::Add,
                                                                                                         binary(BinaryOperator::Add,
                                                                                                                parameter("replyToPostId"),
                                                                                                                parameter("replyToCommentId")),
                                                                                                         integer(1))}})})})})}),
                        createStmt(pattern({path({node({._symbol = "author"}),
                                                  edge({._direction = EdgePattern::Direction::Backward,
                                                        ._labels = labels({"HAS_CREATOR"})}),
                                                  node({._symbol = "c",
                                                        ._labels = labels({"Comment", "Message"}),
                                                        ._properties = properties({{"id", parameter("commentId")},
                                                                                   {"creationDate", parameter("creationDate")},
                                                                                   {"locationIP", parameter("locationIP")},
                                                                                   {"browserUsed", parameter("browserUsed")},
                                                                                   {"content", parameter("content")},
                                                                                   {"length", parameter("length")}})}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"REPLY_OF"})}),
                                                  node({._symbol = "message"})}),
                                            path({node({._symbol = "c"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"IS_LOCATED_IN"})}),
                                                  node({._symbol = "country"})})})),
                        with({._projection = projection({._items = {symbol("c")}})}),
                        unwind(parameter("tagIds"), "tagId"),
                        match({._pattern = pattern({path({node({._symbol = "t",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"id", symbol("tagId")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "c"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"HAS_TAG"})}),
                                                  node({._symbol = "t"})})}))})});
}

TEST_F(LdbcQueryTest, parsesInteractiveUpdate8) {
    expectParse(R"(
        MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
        CREATE (p1)-[:KNOWS {creationDate: $creationDate}]->(p2)
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "p1",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("person1Id")}})})}),
                                                    path({node({._symbol = "p2",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("person2Id")}})})})})}),
                        createStmt(pattern({path({node({._symbol = "p1"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"KNOWS"}),
                                                        ._properties = properties({{"creationDate", parameter("creationDate")}})}),
                                                  node({._symbol = "p2"})})}))})});
}

TEST_F(LdbcQueryTest, parsesBi1) {
    expectParse(R"(
        // Q1. Posting summary
        /*
        :params { datetime: datetime('2011-12-01T00:00:00.000') }
        */
        MATCH (message:Message)
        WHERE message.creationDate < $datetime
        WITH count(message) AS totalMessageCountInt // this should be a subquery once Cypher supports it
        WITH toFloat(totalMessageCountInt) AS totalMessageCount
        MATCH (message:Message)
        WHERE message.creationDate < $datetime
          AND message.content IS NOT NULL
        WITH
          totalMessageCount,
          message,
          message.creationDate.year AS year
        WITH
          totalMessageCount,
          year,
          message:Comment AS isComment,
          CASE
            WHEN message.length <  40 THEN 0
            WHEN message.length <  80 THEN 1
            WHEN message.length < 160 THEN 2
            ELSE                           3
          END AS lengthCategory,
          count(message) AS messageCount,
          sum(message.length) / toFloat(count(message)) AS averageMessageLength,
          sum(message.length) AS sumMessageLength
        RETURN
          year,
          isComment,
          lengthCategory,
          messageCount,
          averageMessageLength,
          sumMessageLength,
          messageCount / totalMessageCount AS percentageOfMessages
        ORDER BY
          year DESC,
          isComment ASC,
          lengthCategory ASC
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "message", ._labels = labels({"Message"})})})},
                                                   where(binary(BinaryOperator::LessThan,
                                                                property({"message", "creationDate"}),
                                                                parameter("datetime"))))}),
                        with({._projection = projection({._items = {named(invocation({"count"}, {symbol("message")}),
                                                                          "totalMessageCountInt")}})}),
                        with({._projection = projection({._items = {named(invocation({"toFloat"},
                                                                                     {symbol("totalMessageCountInt")}),
                                                                          "totalMessageCount")}})}),
                        match({._pattern = pattern({path({node({._symbol = "message", ._labels = labels({"Message"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThan,
                                                                         property({"message", "creationDate"}),
                                                                         parameter("datetime")),
                                                                  binary(BinaryOperator::IsNotNull,
                                                                         property({"message", "content"}),
                                                                         null())})))}),
                        with({._projection = projection({._items = {symbol("totalMessageCount"),
                                                                    symbol("message"),
                                                                    named(lookup(property({"message", "creationDate"}), "year"),
                                                                          "year")}})}),
                        with({._projection = projection({._items = {symbol("totalMessageCount"),
                                                                    symbol("year"),
                                                                    named(entityType("message", labels({"Comment"})),
                                                                          "isComment"),
                                                                    named(caseExpr(nullptr,
                                                                                   {{._tests = {{._value = binary(BinaryOperator::LessThan,
                                                                                                                  property({"message", "length"}),
                                                                                                                  integer(40))}},
                                                                                     ._then = integer(0)},
                                                                                    {._tests = {{._value = binary(BinaryOperator::LessThan,
                                                                                                                  property({"message", "length"}),
                                                                                                                  integer(80))}},
                                                                                     ._then = integer(1)},
                                                                                    {._tests = {{._value = binary(BinaryOperator::LessThan,
                                                                                                                  property({"message", "length"}),
                                                                                                                  integer(160))}},
                                                                                     ._then = integer(2)}},
                                                                                   integer(3)),
                                                                          "lengthCategory"),
                                                                    named(invocation({"count"}, {symbol("message")}),
                                                                          "messageCount"),
                                                                    named(binary(BinaryOperator::Div,
                                                                                 invocation({"sum"},
                                                                                            {property({"message", "length"})}),
                                                                                 invocation({"toFloat"},
                                                                                            {invocation({"count"},
                                                                                                        {symbol("message")})})),
                                                                          "averageMessageLength"),
                                                                    named(invocation({"sum"},
                                                                                     {property({"message", "length"})}),
                                                                          "sumMessageLength")}})})},
                       returning(projection({._items = {symbol("year"),
                                                        symbol("isComment"),
                                                        symbol("lengthCategory"),
                                                        symbol("messageCount"),
                                                        symbol("averageMessageLength"),
                                                        symbol("sumMessageLength"),
                                                        named(binary(BinaryOperator::Div,
                                                                     symbol("messageCount"),
                                                                     symbol("totalMessageCount")),
                                                              "percentageOfMessages")},
                                             ._orderBy = orderBy({{symbol("year"), OrderByType::DESC},
                                                                  {symbol("isComment"), OrderByType::ASC},
                                                                  {symbol("lengthCategory"), OrderByType::ASC}})})))});
}

TEST_F(LdbcQueryTest, parsesBi2) {
    expectParse(R"(
        // Q2. Tag evolution
        /*
        :params { date: datetime('2012-06-01'), tagClass: 'MusicalArtist' }
        */
        MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
        // window 1
        OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
          WHERE $date <= message1.creationDate
            AND message1.creationDate < $date + duration({days: 100})
        WITH tag, count(message1) AS countWindow1
        // window 2
        OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
          WHERE $date + duration({days: 100}) <= message2.creationDate
            AND message2.creationDate < $date + duration({days: 200})
        WITH
          tag,
          countWindow1,
          count(message2) AS countWindow2
        RETURN
          tag.name,
          countWindow1,
          countWindow2,
          abs(countWindow1 - countWindow2) AS diff
        ORDER BY
          diff DESC,
          tag.name ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag", ._labels = labels({"Tag"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TYPE"})}),
                                                          node({._labels = labels({"TagClass"}),
                                                                ._properties = properties({{"name", parameter("tagClass")}})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "message1", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThanOrEqual,
                                                                         parameter("date"),
                                                                         property({"message1", "creationDate"})),
                                                                  binary(BinaryOperator::LessThan,
                                                                         property({"message1", "creationDate"}),
                                                                         binary(BinaryOperator::Add,
                                                                                parameter("date"),
                                                                                invocation({"duration"},
                                                                                           {map({{"days", integer(100)}})})))}))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    named(invocation({"count"}, {symbol("message1")}),
                                                                          "countWindow1")}})}),
                        match({._pattern = pattern({path({node({._symbol = "message2", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThanOrEqual,
                                                                         binary(BinaryOperator::Add,
                                                                                parameter("date"),
                                                                                invocation({"duration"},
                                                                                           {map({{"days", integer(100)}})})),
                                                                         property({"message2", "creationDate"})),
                                                                  binary(BinaryOperator::LessThan,
                                                                         property({"message2", "creationDate"}),
                                                                         binary(BinaryOperator::Add,
                                                                                parameter("date"),
                                                                                invocation({"duration"},
                                                                                           {map({{"days", integer(200)}})})))}))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    symbol("countWindow1"),
                                                                    named(invocation({"count"}, {symbol("message2")}),
                                                                          "countWindow2")}})})},
                       returning(projection({._items = {property({"tag", "name"}),
                                                        symbol("countWindow1"),
                                                        symbol("countWindow2"),
                                                        named(invocation({"abs"},
                                                                         {binary(BinaryOperator::Sub,
                                                                                 symbol("countWindow1"),
                                                                                 symbol("countWindow2"))}),
                                                              "diff")},
                                             ._orderBy = orderBy({{symbol("diff"), OrderByType::DESC},
                                                                  {property({"tag", "name"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi3) {
    expectParse(R"(
        // Q3. Popular topics in a country
        /*
        :params { tagClass: 'MusicalArtist', country: 'Burma' }
        */
        MATCH
          (:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
          (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
          (post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
        RETURN
          forum.id,
          forum.title,
          forum.creationDate,
          person.id,
          count(DISTINCT message) AS messageCount
        ORDER BY
          messageCount DESC,
          forum.id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("country")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "person", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_MODERATOR"})}),
                                                          node({._symbol = "forum", ._labels = labels({"Forum"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"CONTAINER_OF"})}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})}),
                                                          walk({._direction = EdgePattern::Direction::Backward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Backward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._labels = labels({"Tag"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TYPE"})}),
                                                          node({._labels = labels({"TagClass"}),
                                                                ._properties = properties({{"name", parameter("tagClass")}})})})})})},
                       returning(projection({._items = {property({"forum", "id"}),
                                                        property({"forum", "title"}),
                                                        property({"forum", "creationDate"}),
                                                        property({"person", "id"}),
                                                        named(distinct(invocation({"count"}, {symbol("message")})),
                                                              "messageCount")},
                                             ._orderBy = orderBy({{symbol("messageCount"), OrderByType::DESC},
                                                                  {property({"forum", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesBi4) {
    expectParse(R"(
        // Q4. Top message creators in a country
        /*
        :params { date: datetime('2010-01-29') }
        */
        MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum)
        WHERE forum.creationDate > $date
        WITH country, forum, count(person) AS numberOfMembers
        ORDER BY numberOfMembers DESC, forum.id ASC, country.id
        WITH DISTINCT forum AS topForum
        LIMIT 100

        WITH collect(topForum) AS topForums

        CALL {
          WITH topForums
          UNWIND topForums AS topForum1
          MATCH (topForum1)-[:CONTAINER_OF]->(post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_CREATOR]->(person:Person)<-[:HAS_MEMBER]-(topForum2:Forum)
          WITH person, message, topForum2
          WHERE topForum2 IN topForums
          RETURN person, count(DISTINCT message) AS messageCount
        UNION ALL
          // Ensure that people who are members of top forums but have 0 messages are also returned.
          // To this end, we return each person with a 0 messageCount
          WITH topForums
          UNWIND topForums AS topForum1
          MATCH (person:Person)<-[:HAS_MEMBER]-(topForum1:Forum)
          RETURN person, 0 AS messageCount
        }
        RETURN
          person.id AS personId,
          person.firstName AS personFirstName,
          person.lastName AS personLastName,
          person.creationDate AS personCreationDate,
          sum(messageCount) AS messageCount
        ORDER BY
          messageCount DESC,
          person.id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "country", ._labels = labels({"Country"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "person", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_MEMBER"})}),
                                                          node({._symbol = "forum", ._labels = labels({"Forum"})})})},
                                                   where(binary(BinaryOperator::GreaterThan,
                                                                property({"forum", "creationDate"}),
                                                                parameter("date"))))}),
                        with({._projection = projection({._items = {symbol("country"),
                                                                    symbol("forum"),
                                                                    named(invocation({"count"}, {symbol("person")}),
                                                                          "numberOfMembers")},
                                                         ._orderBy = orderBy({{symbol("numberOfMembers"),
                                                                               OrderByType::DESC},
                                                                              {property({"forum", "id"}), OrderByType::ASC},
                                                                              {property({"country", "id"}),
                                                                               OrderByType::ASC}})})}),
                        with({._projection = projection({._items = {named(symbol("forum"), "topForum")},
                                                         ._distinct = true,
                                                         ._limit = limit(integer(100))})}),
                        with({._projection = projection({._items = {named(invocation({"collect"}, {symbol("topForum")}),
                                                                          "topForums")}})}),
                        callSubquery({._body = unionQuery(query({with({._projection = projection({._items = {symbol("topForums")}})}),
                                                                 unwind(symbol("topForums"), "topForum1"),
                                                                 match({._pattern = pattern({path({node({._symbol = "topForum1"}),
                                                                                                   edge({._direction = EdgePattern::Direction::Forward,
                                                                                                         ._labels = labels({"CONTAINER_OF"})}),
                                                                                                   node({._symbol = "post",
                                                                                                         ._labels = labels({"Post"})}),
                                                                                                   walk({._direction = EdgePattern::Direction::Backward,
                                                                                                         ._quantifier = quantifier(0,
                                                                                                                                   QuantifiedPath::UNBOUNDED),
                                                                                                         ._hops = {edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                         ._labels = labels({"REPLY_OF"})})}}),
                                                                                                   node({._symbol = "message",
                                                                                                         ._labels = labels({"Message"})}),
                                                                                                   edge({._direction = EdgePattern::Direction::Forward,
                                                                                                         ._labels = labels({"HAS_CREATOR"})}),
                                                                                                   node({._symbol = "person",
                                                                                                         ._labels = labels({"Person"})}),
                                                                                                   edge({._direction = EdgePattern::Direction::Backward,
                                                                                                         ._labels = labels({"HAS_MEMBER"})}),
                                                                                                   node({._symbol = "topForum2",
                                                                                                         ._labels = labels({"Forum"})})})})}),
                                                                 with({._projection = projection({._items = {symbol("person"),
                                                                                                             symbol("message"),
                                                                                                             symbol("topForum2")}}),
                                                                       ._where = where(binary(BinaryOperator::In,
                                                                                              symbol("topForum2"),
                                                                                              symbol("topForums")))})},
                                                                returning({symbol("person"),
                                                                           named(distinct(invocation({"count"},
                                                                                                     {symbol("message")})),
                                                                                 "messageCount")})),
                                                          {{._query = query({with({._projection = projection({._items = {symbol("topForums")}})}),
                                                                             unwind(symbol("topForums"), "topForum1"),
                                                                             match({._pattern = pattern({path({node({._symbol = "person",
                                                                                                                     ._labels = labels({"Person"})}),
                                                                                                               edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                     ._labels = labels({"HAS_MEMBER"})}),
                                                                                                               node({._symbol = "topForum1",
                                                                                                                     ._labels = labels({"Forum"})})})})})},
                                                                            returning({symbol("person"),
                                                                                       named(integer(0), "messageCount")})),
                                                            ._all = true}})})},
                       returning(projection({._items = {named(property({"person", "id"}), "personId"),
                                                        named(property({"person", "firstName"}), "personFirstName"),
                                                        named(property({"person", "lastName"}), "personLastName"),
                                                        named(property({"person", "creationDate"}), "personCreationDate"),
                                                        named(invocation({"sum"}, {symbol("messageCount")}),
                                                              "messageCount")},
                                             ._orderBy = orderBy({{symbol("messageCount"), OrderByType::DESC},
                                                                  {property({"person", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi5) {
    expectParse(R"(
        // Q5. Most active Posters of a given Topic
        /*
        :params { tag: 'Abbas_I_of_Persia' }
        */
        MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
        OPTIONAL MATCH (message)<-[likes:LIKES]-(:Person)
        WITH person, message, count(likes) AS likeCount
        OPTIONAL MATCH (message)<-[:REPLY_OF]-(reply:Comment)
        WITH person, message, likeCount, count(reply) AS replyCount
        WITH person, count(message) AS messageCount, sum(likeCount) AS likeCount, sum(replyCount) AS replyCount
        RETURN
          person.id,
          replyCount,
          likeCount,
          messageCount,
          1*messageCount + 2*replyCount + 10*likeCount AS score
        ORDER BY
          score DESC,
          person.id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tag")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person", ._labels = labels({"Person"})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "message"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "likes",
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._labels = labels({"Person"})})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    symbol("message"),
                                                                    named(invocation({"count"}, {symbol("likes")}),
                                                                          "likeCount")}})}),
                        match({._pattern = pattern({path({node({._symbol = "message"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._symbol = "reply", ._labels = labels({"Comment"})})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    symbol("message"),
                                                                    symbol("likeCount"),
                                                                    named(invocation({"count"}, {symbol("reply")}),
                                                                          "replyCount")}})}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    named(invocation({"count"}, {symbol("message")}),
                                                                          "messageCount"),
                                                                    named(invocation({"sum"}, {symbol("likeCount")}),
                                                                          "likeCount"),
                                                                    named(invocation({"sum"}, {symbol("replyCount")}),
                                                                          "replyCount")}})})},
                       returning(projection({._items = {property({"person", "id"}),
                                                        symbol("replyCount"),
                                                        symbol("likeCount"),
                                                        symbol("messageCount"),
                                                        named(binary(BinaryOperator::Add,
                                                                     binary(BinaryOperator::Add,
                                                                            binary(BinaryOperator::Mult,
                                                                                   integer(1),
                                                                                   symbol("messageCount")),
                                                                            binary(BinaryOperator::Mult,
                                                                                   integer(2),
                                                                                   symbol("replyCount"))),
                                                                     binary(BinaryOperator::Mult,
                                                                            integer(10),
                                                                            symbol("likeCount"))),
                                                              "score")},
                                             ._orderBy = orderBy({{symbol("score"), OrderByType::DESC},
                                                                  {property({"person", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi6) {
    expectParse(R"(
        // Q6. Most authoritative users on a given topic
        /*
        :params { tag: 'Arnold_Schwarzenegger' }
        */
        MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
        OPTIONAL MATCH (message1)<-[:LIKES]-(person2:Person)
        OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message2:Message)<-[like:LIKES]-(person3:Person)
        RETURN
          person1.id,
          // Using 'DISTINCT like' here ensures that each person2's popularity score is only added once for each person1
          count(DISTINCT like) AS authorityScore
        ORDER BY
          authorityScore DESC,
          person1.id ASC
        LIMIT 100

        // We need to use a redundant computation due to the lack of composable graph queries in the currently supported Cypher version.
        // This might change in the future with new Cypher versions and GQL.
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tag")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "message1", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person1", ._labels = labels({"Person"})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "message1"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "person2", ._labels = labels({"Person"})})})}),
                               ._optional = true}),
                        match({._pattern = pattern({path({node({._symbol = "person2"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message2", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "like",
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "person3", ._labels = labels({"Person"})})})}),
                               ._optional = true})},
                       returning(projection({._items = {property({"person1", "id"}),
                                                        named(distinct(invocation({"count"}, {symbol("like")})),
                                                              "authorityScore")},
                                             ._orderBy = orderBy({{symbol("authorityScore"), OrderByType::DESC},
                                                                  {property({"person1", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi7) {
    expectParse(R"(
        // Q7. Related Topics
        /*
        :params { tag: 'Enrique_Iglesias' }
        */
        MATCH
          (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message),
          (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
        WHERE NOT (comment)-[:HAS_TAG]->(tag)
        RETURN
          relatedTag.name,
          count(DISTINCT comment) AS count
        ORDER BY
          count DESC,
          relatedTag.name ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tag")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})})}),
                                                    path({node({._symbol = "message"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._symbol = "comment", ._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "relatedTag", ._labels = labels({"Tag"})})})},
                                                   where(unary(UnaryOperator::Not,
                                                               patternPredicate(pattern({path({node({._symbol = "comment"}),
                                                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                                                     ._labels = labels({"HAS_TAG"})}),
                                                                                               node({._symbol = "tag"})})})))))})},
                       returning(projection({._items = {property({"relatedTag", "name"}),
                                                        named(distinct(invocation({"count"}, {symbol("comment")})),
                                                              "count")},
                                             ._orderBy = orderBy({{symbol("count"), OrderByType::DESC},
                                                                  {property({"relatedTag", "name"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi8) {
    expectParse(R"(
        // Q8. Central Person for a Tag
        /*
        :params { tag: 'Che_Guevara', startDate: datetime('2011-07-20'), endDate: datetime('2011-07-25') }
        */
        MATCH (tag:Tag {name: $tag})
        // score
        OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person)
        WITH tag, collect(person) AS interestedPersons
        OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
                 WHERE $startDate < message.creationDate
                   AND message.creationDate < $endDate
        WITH tag, interestedPersons, interestedPersons + collect(person) AS persons
        UNWIND persons AS person
        WITH DISTINCT tag, person
        WITH
          tag,
          person,
          100 * size([(tag)<-[interest:HAS_INTEREST]-(person) | interest]) + size([(tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person) WHERE $startDate < message.creationDate AND message.creationDate < $endDate | message])
          AS score
        OPTIONAL MATCH (person)-[:KNOWS]-(friend)
        // We need to use a redundant computation due to the lack of composable graph queries in the currently supported Cypher version.
        // This might change in the future with new Cypher versions and GQL.
        WITH
          person,
          score,
          100 * size([(tag)<-[interest:HAS_INTEREST]-(friend) | interest]) + size([(tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(friend) WHERE $startDate < message.creationDate AND message.creationDate < $endDate | message])
          AS friendScore
        RETURN
          person.id,
          score,
          sum(friendScore) AS friendsScore
        ORDER BY
          score + friendsScore DESC,
          person.id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tag")}})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "tag"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "interest",
                                                                ._labels = labels({"HAS_INTEREST"})}),
                                                          node({._symbol = "person", ._labels = labels({"Person"})})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    named(invocation({"collect"}, {symbol("person")}),
                                                                          "interestedPersons")}})}),
                        match({._pattern = pattern({path({node({._symbol = "tag"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person", ._labels = labels({"Person"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThan,
                                                                         parameter("startDate"),
                                                                         property({"message", "creationDate"})),
                                                                  binary(BinaryOperator::LessThan,
                                                                         property({"message", "creationDate"}),
                                                                         parameter("endDate"))}))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    symbol("interestedPersons"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 symbol("interestedPersons"),
                                                                                 invocation({"collect"},
                                                                                            {symbol("person")})),
                                                                          "persons")}})}),
                        unwind(symbol("persons"), "person"),
                        with({._projection = projection({._items = {symbol("tag"), symbol("person")}, ._distinct = true})}),
                        with({._projection = projection({._items = {symbol("tag"),
                                                                    symbol("person"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 binary(BinaryOperator::Mult,
                                                                                        integer(100),
                                                                                        invocation({"size"},
                                                                                                   {patternComprehension(match({._pattern = pattern({path({node({._symbol = "tag"}),
                                                                                                                                                           edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                                                                 ._symbol = "interest",
                                                                                                                                                                 ._labels = labels({"HAS_INTEREST"})}),
                                                                                                                                                           node({._symbol = "person"})})})}),
                                                                                                                         symbol("interest"))})),
                                                                                 invocation({"size"},
                                                                                            {patternComprehension(match({._pattern = pattern({path({node({._symbol = "tag"}),
                                                                                                                                                    edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                                                          ._labels = labels({"HAS_TAG"})}),
                                                                                                                                                    node({._symbol = "message",
                                                                                                                                                          ._labels = labels({"Message"})}),
                                                                                                                                                    edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                                          ._labels = labels({"HAS_CREATOR"})}),
                                                                                                                                                    node({._symbol = "person"})})},
                                                                                                                                             where(logical(LogicalOperator::And,
                                                                                                                                                           {binary(BinaryOperator::LessThan,
                                                                                                                                                                   parameter("startDate"),
                                                                                                                                                                   property({"message", "creationDate"})),
                                                                                                                                                            binary(BinaryOperator::LessThan,
                                                                                                                                                                   property({"message", "creationDate"}),
                                                                                                                                                                   parameter("endDate"))})))}),
                                                                                                                  symbol("message"))})),
                                                                          "score")}})}),
                        match({._pattern = pattern({path({node({._symbol = "person"}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "friend"})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    symbol("score"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 binary(BinaryOperator::Mult,
                                                                                        integer(100),
                                                                                        invocation({"size"},
                                                                                                   {patternComprehension(match({._pattern = pattern({path({node({._symbol = "tag"}),
                                                                                                                                                           edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                                                                 ._symbol = "interest",
                                                                                                                                                                 ._labels = labels({"HAS_INTEREST"})}),
                                                                                                                                                           node({._symbol = "friend"})})})}),
                                                                                                                         symbol("interest"))})),
                                                                                 invocation({"size"},
                                                                                            {patternComprehension(match({._pattern = pattern({path({node({._symbol = "tag"}),
                                                                                                                                                    edge({._direction = EdgePattern::Direction::Backward,
                                                                                                                                                          ._labels = labels({"HAS_TAG"})}),
                                                                                                                                                    node({._symbol = "message",
                                                                                                                                                          ._labels = labels({"Message"})}),
                                                                                                                                                    edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                                          ._labels = labels({"HAS_CREATOR"})}),
                                                                                                                                                    node({._symbol = "friend"})})},
                                                                                                                                             where(logical(LogicalOperator::And,
                                                                                                                                                           {binary(BinaryOperator::LessThan,
                                                                                                                                                                   parameter("startDate"),
                                                                                                                                                                   property({"message", "creationDate"})),
                                                                                                                                                            binary(BinaryOperator::LessThan,
                                                                                                                                                                   property({"message", "creationDate"}),
                                                                                                                                                                   parameter("endDate"))})))}),
                                                                                                                  symbol("message"))})),
                                                                          "friendScore")}})})},
                       returning(projection({._items = {property({"person", "id"}),
                                                        symbol("score"),
                                                        named(invocation({"sum"}, {symbol("friendScore")}),
                                                              "friendsScore")},
                                             ._orderBy = orderBy({{binary(BinaryOperator::Add,
                                                                          symbol("score"),
                                                                          symbol("friendsScore")),
                                                                   OrderByType::DESC},
                                                                  {property({"person", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi9) {
    expectParse(R"(
        // Q9. Top thread initiators
        /*
        :params { startDate: datetime('2011-10-01'), endDate: datetime('2011-10-15') }
        */
        MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply:Message)
        WHERE  post.creationDate >= $startDate
          AND  post.creationDate <= $endDate
          AND reply.creationDate >= $startDate
          AND reply.creationDate <= $endDate
        RETURN
          person.id,
          person.firstName,
          person.lastName,
          count(DISTINCT post) AS threadCount,
          count(DISTINCT reply) AS messageCount
        ORDER BY
          messageCount DESC,
          person.id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})}),
                                                          walk({._direction = EdgePattern::Direction::Backward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Backward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "reply", ._labels = labels({"Message"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::GreaterThanOrEqual,
                                                                         property({"post", "creationDate"}),
                                                                         parameter("startDate")),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         property({"post", "creationDate"}),
                                                                         parameter("endDate")),
                                                                  binary(BinaryOperator::GreaterThanOrEqual,
                                                                         property({"reply", "creationDate"}),
                                                                         parameter("startDate")),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         property({"reply", "creationDate"}),
                                                                         parameter("endDate"))})))})},
                       returning(projection({._items = {property({"person", "id"}),
                                                        property({"person", "firstName"}),
                                                        property({"person", "lastName"}),
                                                        named(distinct(invocation({"count"}, {symbol("post")})),
                                                              "threadCount"),
                                                        named(distinct(invocation({"count"}, {symbol("reply")})),
                                                              "messageCount")},
                                             ._orderBy = orderBy({{symbol("messageCount"), OrderByType::DESC},
                                                                  {property({"person", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi10) {
    expectParse(R"(
        // Q10. Experts in social circle
        // Requires the Neo4j APOC library
        /*
        :params {
            personId: 30786325588624,
            country: 'China',
            tagClass: 'MusicalArtist',
            minPathDistance: 3,
            maxPathDistance: 4
        }
        */
        MATCH (startPerson:Person {id: $personId})
        CALL apoc.path.subgraphNodes(startPerson, {
            relationshipFilter: "KNOWS",
            minLevel: 1,
            maxLevel: $minPathDistance-1
        })
        YIELD node
        WITH startPerson, collect(DISTINCT node) AS nodesCloserThanMinPathDistance
        CALL apoc.path.subgraphNodes(startPerson, {
            relationshipFilter: "KNOWS",
            minLevel: 1,
            maxLevel: $maxPathDistance
        })
        YIELD node
        WITH nodesCloserThanMinPathDistance, collect(DISTINCT node) AS nodesCloserThanMaxPathDistance
        // compute the difference of sets: nodesCloserThanMaxPathDistance - nodesCloserThanMinPathDistance
        WITH [n IN nodesCloserThanMaxPathDistance WHERE NOT n IN nodesCloserThanMinPathDistance] AS expertCandidatePersons
        UNWIND expertCandidatePersons AS expertCandidatePerson
        MATCH
          (expertCandidatePerson)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: $country}),
          (expertCandidatePerson)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->
          (:TagClass {name: $tagClass})
        MATCH
          (message)-[:HAS_TAG]->(tag:Tag)
        RETURN
          expertCandidatePerson.id,
          tag.name,
          count(DISTINCT message) AS messageCount
        ORDER BY
          messageCount DESC,
          tag.name ASC,
          expertCandidatePerson.id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "startPerson",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("personId")}})})})})}),
                        call({._invocation = invocation({"apoc", "path", "subgraphNodes"},
                                                        {symbol("startPerson"),
                                                         map({{"relationshipFilter", string("KNOWS")},
                                                              {"minLevel", integer(1)},
                                                              {"maxLevel", binary(BinaryOperator::Sub,
                                                                                  parameter("minPathDistance"),
                                                                                  integer(1))}})}),
                              ._yield = yield({symbol("node")})}),
                        with({._projection = projection({._items = {symbol("startPerson"),
                                                                    named(distinct(invocation({"collect"},
                                                                                              {symbol("node")})),
                                                                          "nodesCloserThanMinPathDistance")}})}),
                        call({._invocation = invocation({"apoc", "path", "subgraphNodes"},
                                                        {symbol("startPerson"),
                                                         map({{"relationshipFilter", string("KNOWS")},
                                                              {"minLevel", integer(1)},
                                                              {"maxLevel", parameter("maxPathDistance")}})}),
                              ._yield = yield({symbol("node")})}),
                        with({._projection = projection({._items = {symbol("nodesCloserThanMinPathDistance"),
                                                                    named(distinct(invocation({"collect"},
                                                                                              {symbol("node")})),
                                                                          "nodesCloserThanMaxPathDistance")}})}),
                        with({._projection = projection({._items = {named(listComprehension("n",
                                                                                            symbol("nodesCloserThanMaxPathDistance"),
                                                                                            unary(UnaryOperator::Not,
                                                                                                  binary(BinaryOperator::In,
                                                                                                         symbol("n"),
                                                                                                         symbol("nodesCloserThanMinPathDistance")))),
                                                                          "expertCandidatePersons")}})}),
                        unwind(symbol("expertCandidatePersons"), "expertCandidatePerson"),
                        match({._pattern = pattern({path({node({._symbol = "expertCandidatePerson"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("country")}})})}),
                                                    path({node({._symbol = "expertCandidatePerson"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._labels = labels({"Tag"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TYPE"})}),
                                                          node({._labels = labels({"TagClass"}),
                                                                ._properties = properties({{"name", parameter("tagClass")}})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "message"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag", ._labels = labels({"Tag"})})})})})},
                       returning(projection({._items = {property({"expertCandidatePerson", "id"}),
                                                        property({"tag", "name"}),
                                                        named(distinct(invocation({"count"}, {symbol("message")})),
                                                              "messageCount")},
                                             ._orderBy = orderBy({{symbol("messageCount"), OrderByType::DESC},
                                                                  {property({"tag", "name"}), OrderByType::ASC},
                                                                  {property({"expertCandidatePerson", "id"}),
                                                                   OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi11) {
    expectParse(R"(
        // Q11. Friend triangles
        /*
        :params {
          country: 'India',
          startDate: datetime('2012-09-29'),
          endDate: datetime('2013-01-01')
        }
        */
        MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country {name: $country}),
              (a)-[k1:KNOWS]-(b:Person)
        WHERE a.id < b.id
          AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate
        WITH DISTINCT country, a, b
        MATCH (b)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
        WITH DISTINCT country, a, b
        MATCH (b)-[k2:KNOWS]-(c:Person),
              (c)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
        WHERE b.id < c.id
          AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate
        WITH DISTINCT a, b, c
        MATCH (c)-[k3:KNOWS]-(a)
        WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
        WITH DISTINCT a, b, c
        RETURN count(*) AS count
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "a", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._symbol = "country",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("country")}})})}),
                                                    path({node({._symbol = "a"}),
                                                          edge({._symbol = "k1", ._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "b", ._labels = labels({"Person"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThan,
                                                                         property({"a", "id"}),
                                                                         property({"b", "id"})),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         parameter("startDate"),
                                                                         property({"k1", "creationDate"})),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         property({"k1", "creationDate"}),
                                                                         parameter("endDate"))})))}),
                        with({._projection = projection({._items = {symbol("country"), symbol("a"), symbol("b")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "b"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._symbol = "country"})})})}),
                        with({._projection = projection({._items = {symbol("country"), symbol("a"), symbol("b")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "b"}),
                                                          edge({._symbol = "k2", ._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "c", ._labels = labels({"Person"})})}),
                                                    path({node({._symbol = "c"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._symbol = "country"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThan,
                                                                         property({"b", "id"}),
                                                                         property({"c", "id"})),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         parameter("startDate"),
                                                                         property({"k2", "creationDate"})),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         property({"k2", "creationDate"}),
                                                                         parameter("endDate"))})))}),
                        with({._projection = projection({._items = {symbol("a"), symbol("b"), symbol("c")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "c"}),
                                                          edge({._symbol = "k3", ._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "a"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::LessThanOrEqual,
                                                                         parameter("startDate"),
                                                                         property({"k3", "creationDate"})),
                                                                  binary(BinaryOperator::LessThanOrEqual,
                                                                         property({"k3", "creationDate"}),
                                                                         parameter("endDate"))})))}),
                        with({._projection = projection({._items = {symbol("a"), symbol("b"), symbol("c")},
                                                         ._distinct = true})})},
                       returning({named(invocation({"count"}, {wildcard()}), "count")}))});
}

TEST_F(LdbcQueryTest, parsesBi12) {
    expectParse(R"(
        // Q12. How many persons have a given number of posts
        /*
        :params {
          startDate: datetime('2010-07-22'),
          lengthThreshold: 20,
          languages: ['ar', 'hu']
        }
        */
        MATCH (person:Person)
        OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)
        WHERE message.content IS NOT NULL
          AND message.length < $lengthThreshold
          AND message.creationDate > $startDate
          AND post.language IN $languages
        WITH
          person,
          count(message) AS messageCount
        RETURN
          messageCount,
          count(person) AS personCount
        ORDER BY
          personCount DESC,
          messageCount DESC
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person", ._labels = labels({"Person"})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "person"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "post", ._labels = labels({"Post"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::IsNotNull,
                                                                         property({"message", "content"}),
                                                                         null()),
                                                                  binary(BinaryOperator::LessThan,
                                                                         property({"message", "length"}),
                                                                         parameter("lengthThreshold")),
                                                                  binary(BinaryOperator::GreaterThan,
                                                                         property({"message", "creationDate"}),
                                                                         parameter("startDate")),
                                                                  binary(BinaryOperator::In,
                                                                         property({"post", "language"}),
                                                                         parameter("languages"))}))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person"),
                                                                    named(invocation({"count"}, {symbol("message")}),
                                                                          "messageCount")}})})},
                       returning(projection({._items = {symbol("messageCount"),
                                                        named(invocation({"count"}, {symbol("person")}), "personCount")},
                                             ._orderBy = orderBy({{symbol("personCount"), OrderByType::DESC},
                                                                  {symbol("messageCount"), OrderByType::DESC}})})))});
}

TEST_F(LdbcQueryTest, parsesBi13) {
    expectParse(R"(
        // Q13. Zombies in a country
        /*
        :params { country: 'France', endDate: datetime('2013-01-01') }
        */
        MATCH (country:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
        WHERE zombie.creationDate < $endDate
        WITH country, zombie
        OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message:Message)
        WHERE message.creationDate < $endDate
        WITH
          country,
          zombie,
          count(message) AS messageCount
        WITH
          country,
          zombie,
          12 * ($endDate.year  - zombie.creationDate.year )
             + ($endDate.month - zombie.creationDate.month)
             + 1 AS months,
          messageCount
        WHERE messageCount / months < 1
        WITH
          country,
          collect(zombie) AS zombies
        UNWIND zombies AS zombie
        OPTIONAL MATCH
          (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person)
        WHERE likerZombie IN zombies
        WITH
          zombie,
          count(likerZombie) AS zombieLikeCount
        OPTIONAL MATCH
          (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person)
        WHERE likerPerson.creationDate < $endDate
        WITH
          zombie,
          zombieLikeCount,
          count(likerPerson) AS totalLikeCount
        RETURN
          zombie.id,
          zombieLikeCount,
          totalLikeCount,
          CASE totalLikeCount
            WHEN 0 THEN 0.0
            ELSE zombieLikeCount / toFloat(totalLikeCount)
          END AS zombieScore
        ORDER BY
          zombieScore DESC,
          zombie.id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "country",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("country")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "zombie", ._labels = labels({"Person"})})})},
                                                   where(binary(BinaryOperator::LessThan,
                                                                property({"zombie", "creationDate"}),
                                                                parameter("endDate"))))}),
                        with({._projection = projection({._items = {symbol("country"), symbol("zombie")}})}),
                        match({._pattern = pattern({path({node({._symbol = "zombie"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})})})},
                                                   where(binary(BinaryOperator::LessThan,
                                                                property({"message", "creationDate"}),
                                                                parameter("endDate")))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("country"),
                                                                    symbol("zombie"),
                                                                    named(invocation({"count"}, {symbol("message")}),
                                                                          "messageCount")}})}),
                        with({._projection = projection({._items = {symbol("country"),
                                                                    symbol("zombie"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 binary(BinaryOperator::Add,
                                                                                        binary(BinaryOperator::Mult,
                                                                                               integer(12),
                                                                                               parenthesized(binary(BinaryOperator::Sub,
                                                                                                                    lookup(parameter("endDate"),
                                                                                                                           "year"),
                                                                                                                    lookup(property({"zombie", "creationDate"}), "year")))),
                                                                                        parenthesized(binary(BinaryOperator::Sub,
                                                                                                             lookup(parameter("endDate"),
                                                                                                                    "month"),
                                                                                                             lookup(property({"zombie", "creationDate"}), "month")))),
                                                                                 integer(1)),
                                                                          "months"),
                                                                    symbol("messageCount")}}),
                              ._where = where(binary(BinaryOperator::LessThan,
                                                     binary(BinaryOperator::Div, symbol("messageCount"), symbol("months")),
                                                     integer(1)))}),
                        with({._projection = projection({._items = {symbol("country"),
                                                                    named(invocation({"collect"}, {symbol("zombie")}),
                                                                          "zombies")}})}),
                        unwind(symbol("zombies"), "zombie"),
                        match({._pattern = pattern({path({node({._symbol = "zombie"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "likerZombie",
                                                                ._labels = labels({"Person"})})})},
                                                   where(binary(BinaryOperator::In,
                                                                symbol("likerZombie"),
                                                                symbol("zombies")))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("zombie"),
                                                                    named(invocation({"count"}, {symbol("likerZombie")}),
                                                                          "zombieLikeCount")}})}),
                        match({._pattern = pattern({path({node({._symbol = "zombie"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "likerPerson",
                                                                ._labels = labels({"Person"})})})},
                                                   where(binary(BinaryOperator::LessThan,
                                                                property({"likerPerson", "creationDate"}),
                                                                parameter("endDate")))),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("zombie"),
                                                                    symbol("zombieLikeCount"),
                                                                    named(invocation({"count"}, {symbol("likerPerson")}),
                                                                          "totalLikeCount")}})})},
                       returning(projection({._items = {property({"zombie", "id"}),
                                                        symbol("zombieLikeCount"),
                                                        symbol("totalLikeCount"),
                                                        named(caseExpr(symbol("totalLikeCount"),
                                                                       {{._tests = {{._value = integer(0)}},
                                                                         ._then = real(0.0)}},
                                                                       binary(BinaryOperator::Div,
                                                                              symbol("zombieLikeCount"),
                                                                              invocation({"toFloat"},
                                                                                         {symbol("totalLikeCount")}))),
                                                              "zombieScore")},
                                             ._orderBy = orderBy({{symbol("zombieScore"), OrderByType::DESC},
                                                                  {property({"zombie", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi14) {
    expectParse(R"(
        // Q14. International dialog
        /*
        :params { country1: 'Chile', country2: 'Argentina' }
        */
        MATCH
          (country1:Country {name: $country1})<-[:IS_PART_OF]-(city1:City)<-[:IS_LOCATED_IN]-(person1:Person),
          (country2:Country {name: $country2})<-[:IS_PART_OF]-(city2:City)<-[:IS_LOCATED_IN]-(person2:Person),
          (person1)-[:KNOWS]-(person2)
        WITH person1, person2, city1, 0 AS score
        // case 1
        OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(:Message)-[:HAS_CREATOR]->(person2)
        WITH DISTINCT person1, person2, city1, score + (CASE WHEN c IS NULL THEN 0 ELSE  4 END) AS score
        // case 2
        OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m:Message)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(person2)
        WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
        // case 3
        OPTIONAL MATCH (person1)-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(person2)
        WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE 10 END) AS score
        // case 4
        OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m:Message)<-[:LIKES]-(person2)
        WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
        // preorder
        ORDER BY
          city1.name ASC,
          score DESC,
          person1.id ASC,
          person2.id ASC
        WITH city1, collect({score: score, person1Id: person1.id, person2Id: person2.id})[0] AS top
        RETURN
          top.person1Id,
          top.person2Id,
          city1.name,
          top.score
        ORDER BY
          top.score DESC,
          top.person1Id ASC,
          top.person2Id ASC
        LIMIT 100
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "country1",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("country1")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._symbol = "city1", ._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "person1", ._labels = labels({"Person"})})}),
                                                    path({node({._symbol = "country2",
                                                                ._labels = labels({"Country"}),
                                                                ._properties = properties({{"name", parameter("country2")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_PART_OF"})}),
                                                          node({._symbol = "city2", ._labels = labels({"City"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "person2", ._labels = labels({"Person"})})}),
                                                    path({node({._symbol = "person1"}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "person2"})})})}),
                        with({._projection = projection({._items = {symbol("person1"),
                                                                    symbol("person2"),
                                                                    symbol("city1"),
                                                                    named(integer(0), "score")}})}),
                        match({._pattern = pattern({path({node({._symbol = "person1"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "c", ._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person2"})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person1"),
                                                                    symbol("person2"),
                                                                    symbol("city1"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 symbol("score"),
                                                                                 parenthesized(caseExpr(nullptr,
                                                                                                        {{._tests = {{._value = binary(BinaryOperator::IsNull,
                                                                                                                                       symbol("c"),
                                                                                                                                       null())}},
                                                                                                          ._then = integer(0)}},
                                                                                                        integer(4)))),
                                                                          "score")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "person1"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "m", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person2"})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person1"),
                                                                    symbol("person2"),
                                                                    symbol("city1"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 symbol("score"),
                                                                                 parenthesized(caseExpr(nullptr,
                                                                                                        {{._tests = {{._value = binary(BinaryOperator::IsNull,
                                                                                                                                       symbol("m"),
                                                                                                                                       null())}},
                                                                                                          ._then = integer(0)}},
                                                                                                        integer(1)))),
                                                                          "score")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "person1"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "m", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "person2"})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person1"),
                                                                    symbol("person2"),
                                                                    symbol("city1"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 symbol("score"),
                                                                                 parenthesized(caseExpr(nullptr,
                                                                                                        {{._tests = {{._value = binary(BinaryOperator::IsNull,
                                                                                                                                       symbol("m"),
                                                                                                                                       null())}},
                                                                                                          ._then = integer(0)}},
                                                                                                        integer(10)))),
                                                                          "score")},
                                                         ._distinct = true})}),
                        match({._pattern = pattern({path({node({._symbol = "person1"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "m", ._labels = labels({"Message"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"LIKES"})}),
                                                          node({._symbol = "person2"})})}),
                               ._optional = true}),
                        with({._projection = projection({._items = {symbol("person1"),
                                                                    symbol("person2"),
                                                                    symbol("city1"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 symbol("score"),
                                                                                 parenthesized(caseExpr(nullptr,
                                                                                                        {{._tests = {{._value = binary(BinaryOperator::IsNull,
                                                                                                                                       symbol("m"),
                                                                                                                                       null())}},
                                                                                                          ._then = integer(0)}},
                                                                                                        integer(1)))),
                                                                          "score")},
                                                         ._distinct = true,
                                                         ._orderBy = orderBy({{property({"city1", "name"}),
                                                                               OrderByType::ASC},
                                                                              {symbol("score"), OrderByType::DESC},
                                                                              {property({"person1", "id"}),
                                                                               OrderByType::ASC},
                                                                              {property({"person2", "id"}),
                                                                               OrderByType::ASC}})})}),
                        with({._projection = projection({._items = {symbol("city1"),
                                                                    named(index(invocation({"collect"},
                                                                                           {map({{"score", symbol("score")},
                                                                                                 {"person1Id", property({"person1", "id"})},
                                                                                                 {"person2Id", property({"person2", "id"})}})}),
                                                                                integer(0)),
                                                                          "top")}})})},
                       returning(projection({._items = {property({"top", "person1Id"}),
                                                        property({"top", "person2Id"}),
                                                        property({"city1", "name"}),
                                                        property({"top", "score"})},
                                             ._orderBy = orderBy({{property({"top", "score"}), OrderByType::DESC},
                                                                  {property({"top", "person1Id"}), OrderByType::ASC},
                                                                  {property({"top", "person2Id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(100))})))});
}

TEST_F(LdbcQueryTest, parsesBi15) {
    expectParse(R"(
        // Q15. Weighted interaction paths
        // Requires the Neo4j Graph Data Science library
        /*
        :params {
            person1Id: 14,
            person2Id: 16,
            startDate: datetime('2010-11-01'),
            endDate: datetime('2010-12-01')
        }
        */
        CALL gds.graph.drop('bi15', false)
        YIELD graphName

        // ----------------------------------------------------------------------------------------------------
        WITH count(*) AS dummy
        // ----------------------------------------------------------------------------------------------------

        CALL gds.graph.project.cypher(
          'bi15',
          'MATCH (p:Person) RETURN id(p) AS id',
          'MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)
              OPTIONAL MATCH (pA)<-[:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[:HAS_CREATOR]->(pB)
              OPTIONAL MATCH (m1)-[:REPLY_OF*0..]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
                      WHERE forum.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
                        AND forum.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
              WITH pA, pB,
                  sum(CASE forum IS NOT NULL
                      WHEN true THEN
                          CASE (m1:Post OR m2:Post) WHEN true THEN 1.0
                          ELSE 0.5 END
                      ELSE 0.0 END
                  ) AS w
              RETURN
                id(pA) AS source,
                id(pB) AS target,
                1/(w+1) AS weight
          '
        )
        YIELD graphName

        // ----------------------------------------------------------------------------------------------------
        WITH count(*) AS dummy
        // ----------------------------------------------------------------------------------------------------

        CALL {
          MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
          CALL gds.shortestPath.dijkstra.stream('bi15', {
            sourceNode: person1,
            targetNode: person2,
            relationshipWeightProperty: 'weight'
          })
          YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
          RETURN totalCost
          UNION ALL
          RETURN -1.0 AS totalCost
        }
        RETURN max(totalCost) AS totalCost
    )",
                {query({call({._invocation = invocation({"gds", "graph", "drop"}, {string("bi15"), boolean(false)}),
                              ._yield = yield({symbol("graphName")})}),
                        with({._projection = projection({._items = {named(invocation({"count"}, {wildcard()}),
                                                                          "dummy")}})}),
                        call({._invocation = invocation({"gds", "graph", "project", "cypher"},
                                                        {string("bi15"),
                                                         string("MATCH (p:Person) RETURN id(p) AS id"),
                                                         binary(BinaryOperator::Add,
                                                                binary(BinaryOperator::Add,
                                                                       binary(BinaryOperator::Add,
                                                                              binary(BinaryOperator::Add,
                                                                                     string("MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)\n"
                                                                                            "              OPTIONAL MATCH (pA)<-[:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[:HAS_CREATOR]->(pB)\n"
                                                                                            "              OPTIONAL MATCH (m1)-[:REPLY_OF*0..]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)\n"
                                                                                            "                      WHERE forum.creationDate >= datetime({epochmillis: "),
                                                                                     lookup(parameter("startDate"),
                                                                                            "epochMillis")),
                                                                              string("})\n"
                                                                                     "                        AND forum.creationDate <= datetime({epochmillis: ")),
                                                                       lookup(parameter("endDate"), "epochMillis")),
                                                                string("})\n"
                                                                       "              WITH pA, pB,\n"
                                                                       "                  sum(CASE forum IS NOT NULL\n"
                                                                       "                      WHEN true THEN\n"
                                                                       "                          CASE (m1:Post OR m2:Post) WHEN true THEN 1.0\n"
                                                                       "                          ELSE 0.5 END\n"
                                                                       "                      ELSE 0.0 END\n"
                                                                       "                  ) AS w\n"
                                                                       "              RETURN\n"
                                                                       "                id(pA) AS source,\n"
                                                                       "                id(pB) AS target,\n"
                                                                       "                1/(w+1) AS weight\n"
                                                                       "          "))}),
                              ._yield = yield({symbol("graphName")})}),
                        with({._projection = projection({._items = {named(invocation({"count"}, {wildcard()}),
                                                                          "dummy")}})}),
                        callSubquery({._body = unionQuery(query({match({._pattern = pattern({path({node({._symbol = "person1",
                                                                                                         ._labels = labels({"Person"}),
                                                                                                         ._properties = properties({{"id", parameter("person1Id")}})})}),
                                                                                             path({node({._symbol = "person2",
                                                                                                         ._labels = labels({"Person"}),
                                                                                                         ._properties = properties({{"id", parameter("person2Id")}})})})})}),
                                                                 call({._invocation = invocation({"gds",
                                                                                                  "shortestPath",
                                                                                                  "dijkstra",
                                                                                                  "stream"},
                                                                                                 {string("bi15"),
                                                                                                  map({{"sourceNode", symbol("person1")},
                                                                                                       {"targetNode", symbol("person2")},
                                                                                                       {"relationshipWeightProperty", string("weight")}})}),
                                                                       ._yield = yield({symbol("index"),
                                                                                        symbol("sourceNode"),
                                                                                        symbol("targetNode"),
                                                                                        symbol("totalCost"),
                                                                                        symbol("nodeIds"),
                                                                                        symbol("costs"),
                                                                                        symbol("path")})})},
                                                                returning({symbol("totalCost")})),
                                                          {{._query = query({},
                                                                            returning({named(real(-1.0), "totalCost")})),
                                                            ._all = true}})})},
                       returning({named(invocation({"max"}, {symbol("totalCost")}), "totalCost")}))});
}

TEST_F(LdbcQueryTest, parsesBi16) {
    expectParse(R"(
        // Q16. Fake news detection
        // These parameters return a 'false positive' as the maxKnowsLimit is set too high.
        /*
        :params {
          tagA: 'Meryl_Streep',
          dateA: datetime('2012-09-16'),
          tagB: 'Hank_Williams',
          dateB: datetime('2012-05-08'),
          maxKnowsLimit: 4
        }
        */
        UNWIND [
            {letter: 'A', tag: $tagA, date: $dateA},
            {letter: 'B', tag: $tagB, date: $dateB}
          ] AS param
        WITH param.letter AS paramLetter, param.tag AS paramTagX, param.date AS paramDateX
        CALL {
          WITH paramTagX, paramDateX
          MATCH (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:HAS_TAG]->(tag:Tag {name: paramTagX})
          WHERE date(message1.creationDate) = date(paramDateX)
          // filter out Persons with more than $maxKnowsLimit friends who created the same kind of Message
          OPTIONAL MATCH (person1)-[:KNOWS]-(person2:Person)<-[:HAS_CREATOR]-(message2:Message)-[:HAS_TAG]->(tag)
          WHERE date(message2.creationDate) = date(paramDateX)
          WITH person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
          WHERE cp2 <= $maxKnowsLimit
          // return count
          RETURN person1, cm
        }
        WITH person1, collect({letter: paramLetter, messageCount: cm}) AS results
        WHERE size(results) = 2
        RETURN
          person1.id,
          [r IN results WHERE r.letter = 'A' | r.messageCount][0] AS messageCountA,
          [r IN results WHERE r.letter = 'B' | r.messageCount][0] AS messageCountB
        ORDER BY messageCountA + messageCountB DESC, person1.id ASC
        LIMIT 20
    )",
                {query({unwind(list({map({{"letter", string("A")},
                                          {"tag", parameter("tagA")},
                                          {"date", parameter("dateA")}}),
                                     map({{"letter", string("B")},
                                          {"tag", parameter("tagB")},
                                          {"date", parameter("dateB")}})}),
                               "param"),
                        with({._projection = projection({._items = {named(property({"param", "letter"}), "paramLetter"),
                                                                    named(property({"param", "tag"}), "paramTagX"),
                                                                    named(property({"param", "date"}), "paramDateX")}})}),
                        callSubquery({._body = query({with({._projection = projection({._items = {symbol("paramTagX"),
                                                                                                  symbol("paramDateX")}})}),
                                                      match({._pattern = pattern({path({node({._symbol = "person1",
                                                                                              ._labels = labels({"Person"})}),
                                                                                        edge({._direction = EdgePattern::Direction::Backward,
                                                                                              ._labels = labels({"HAS_CREATOR"})}),
                                                                                        node({._symbol = "message1",
                                                                                              ._labels = labels({"Message"})}),
                                                                                        edge({._direction = EdgePattern::Direction::Forward,
                                                                                              ._labels = labels({"HAS_TAG"})}),
                                                                                        node({._symbol = "tag",
                                                                                              ._labels = labels({"Tag"}),
                                                                                              ._properties = properties({{"name", symbol("paramTagX")}})})})},
                                                                                 where(binary(BinaryOperator::Equal,
                                                                                              invocation({"date"},
                                                                                                         {property({"message1", "creationDate"})}),
                                                                                              invocation({"date"},
                                                                                                         {symbol("paramDateX")}))))}),
                                                      match({._pattern = pattern({path({node({._symbol = "person1"}),
                                                                                        edge({._labels = labels({"KNOWS"})}),
                                                                                        node({._symbol = "person2",
                                                                                              ._labels = labels({"Person"})}),
                                                                                        edge({._direction = EdgePattern::Direction::Backward,
                                                                                              ._labels = labels({"HAS_CREATOR"})}),
                                                                                        node({._symbol = "message2",
                                                                                              ._labels = labels({"Message"})}),
                                                                                        edge({._direction = EdgePattern::Direction::Forward,
                                                                                              ._labels = labels({"HAS_TAG"})}),
                                                                                        node({._symbol = "tag"})})},
                                                                                 where(binary(BinaryOperator::Equal,
                                                                                              invocation({"date"},
                                                                                                         {property({"message2", "creationDate"})}),
                                                                                              invocation({"date"},
                                                                                                         {symbol("paramDateX")})))),
                                                             ._optional = true}),
                                                      with({._projection = projection({._items = {symbol("person1"),
                                                                                                  named(distinct(invocation({"count"},
                                                                                                                            {symbol("message1")})),
                                                                                                        "cm"),
                                                                                                  named(distinct(invocation({"count"},
                                                                                                                            {symbol("person2")})),
                                                                                                        "cp2")}}),
                                                            ._where = where(binary(BinaryOperator::LessThanOrEqual,
                                                                                   symbol("cp2"),
                                                                                   parameter("maxKnowsLimit")))})},
                                                     returning({symbol("person1"), symbol("cm")}))}),
                        with({._projection = projection({._items = {symbol("person1"),
                                                                    named(invocation({"collect"},
                                                                                     {map({{"letter", symbol("paramLetter")},
                                                                                           {"messageCount", symbol("cm")}})}),
                                                                          "results")}}),
                              ._where = where(binary(BinaryOperator::Equal,
                                                     invocation({"size"}, {symbol("results")}),
                                                     integer(2)))})},
                       returning(projection({._items = {property({"person1", "id"}),
                                                        named(index(listComprehension("r",
                                                                                      symbol("results"),
                                                                                      binary(BinaryOperator::Equal,
                                                                                             property({"r", "letter"}),
                                                                                             string("A")),
                                                                                      property({"r", "messageCount"})),
                                                                    integer(0)),
                                                              "messageCountA"),
                                                        named(index(listComprehension("r",
                                                                                      symbol("results"),
                                                                                      binary(BinaryOperator::Equal,
                                                                                             property({"r", "letter"}),
                                                                                             string("B")),
                                                                                      property({"r", "messageCount"})),
                                                                    integer(0)),
                                                              "messageCountB")},
                                             ._orderBy = orderBy({{binary(BinaryOperator::Add,
                                                                          symbol("messageCountA"),
                                                                          symbol("messageCountB")),
                                                                   OrderByType::DESC},
                                                                  {property({"person1", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesBi17) {
    expectParse(R"(
        // Q17. Information propagation analysis
        /*
        :params { tag: 'Slavoj_Žižek', delta: 4 }
        */
        MATCH
          (tag:Tag {name: $tag}),
          (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:REPLY_OF*0..]->(post1:Post)<-[:CONTAINER_OF]-(forum1:Forum),
          (message1)-[:HAS_TAG]->(tag),
        // Having two HAS_MEMBER edges in the same MATCH clause ensures that person2 and person3 are different
        // as Cypher's edge-isomorphic matching does not allow for such a match in a single MATCH clause.
          (forum1)<-[:HAS_MEMBER]->(person2:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:HAS_TAG]->(tag),
          (forum1)<-[:HAS_MEMBER]->(person3:Person)<-[:HAS_CREATOR]-(message2:Message),
          (comment)-[:REPLY_OF]->(message2)-[:REPLY_OF*0..]->(post2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
        // The query allows message2 = post2. If this is the case, their HAS_TAG edges to tag overlap,
        // and Cypher's edge-isomorphic matching does not allow for such a match in a single MATCH clause.
        // To work around this, we add them in separate MATCH clauses.
        MATCH (comment)-[:HAS_TAG]->(tag)
        MATCH (message2)-[:HAS_TAG]->(tag)
        WHERE forum1 <> forum2
          AND message2.creationDate > message1.creationDate + duration({hours: $delta})
          AND NOT (forum2)-[:HAS_MEMBER]->(person1)
        RETURN person1.id, count(DISTINCT message2) AS messageCount
        ORDER BY messageCount DESC, person1.id ASC
        LIMIT 10
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tag")}})})}),
                                                    path({node({._symbol = "person1", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message1", ._labels = labels({"Message"})}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "post1", ._labels = labels({"Post"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"CONTAINER_OF"})}),
                                                          node({._symbol = "forum1", ._labels = labels({"Forum"})})}),
                                                    path({node({._symbol = "message1"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})}),
                                                    path({node({._symbol = "forum1"}),
                                                          edge({._labels = labels({"HAS_MEMBER"})}),
                                                          node({._symbol = "person2", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "comment", ._labels = labels({"Comment"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})}),
                                                    path({node({._symbol = "forum1"}),
                                                          edge({._labels = labels({"HAS_MEMBER"})}),
                                                          node({._symbol = "person3", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_CREATOR"})}),
                                                          node({._symbol = "message2", ._labels = labels({"Message"})})}),
                                                    path({node({._symbol = "comment"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"REPLY_OF"})}),
                                                          node({._symbol = "message2"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                ._labels = labels({"REPLY_OF"})})}}),
                                                          node({._symbol = "post2", ._labels = labels({"Post"})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"CONTAINER_OF"})}),
                                                          node({._symbol = "forum2", ._labels = labels({"Forum"})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "comment"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})})})}),
                        match({._pattern = pattern({path({node({._symbol = "message2"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_TAG"})}),
                                                          node({._symbol = "tag"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::NotEqual,
                                                                         symbol("forum1"),
                                                                         symbol("forum2")),
                                                                  binary(BinaryOperator::GreaterThan,
                                                                         property({"message2", "creationDate"}),
                                                                         binary(BinaryOperator::Add,
                                                                                property({"message1", "creationDate"}),
                                                                                invocation({"duration"},
                                                                                           {map({{"hours", parameter("delta")}})}))),
                                                                  unary(UnaryOperator::Not,
                                                                        patternPredicate(pattern({path({node({._symbol = "forum2"}),
                                                                                                        edge({._direction = EdgePattern::Direction::Forward,
                                                                                                              ._labels = labels({"HAS_MEMBER"})}),
                                                                                                        node({._symbol = "person1"})})})))})))})},
                       returning(projection({._items = {property({"person1", "id"}),
                                                        named(distinct(invocation({"count"}, {symbol("message2")})),
                                                              "messageCount")},
                                             ._orderBy = orderBy({{symbol("messageCount"), OrderByType::DESC},
                                                                  {property({"person1", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(10))})))});
}

TEST_F(LdbcQueryTest, parsesBi18) {
    expectParse(R"(
        // Q18. Friend recommendation
        /*
        :params { tag: 'Frank_Sinatra' }
        */
        MATCH (tag:Tag {name: $tag})<-[:HAS_INTEREST]-(person1:Person)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)-[:HAS_INTEREST]->(tag)
        WHERE person1 <> person2
          AND NOT (person1)-[:KNOWS]-(person2)
        RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
        ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "tag",
                                                                ._labels = labels({"Tag"}),
                                                                ._properties = properties({{"name", parameter("tag")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"HAS_INTEREST"})}),
                                                          node({._symbol = "person1", ._labels = labels({"Person"})}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "mutualFriend", ._labels = labels({"Person"})}),
                                                          edge({._labels = labels({"KNOWS"})}),
                                                          node({._symbol = "person2", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"HAS_INTEREST"})}),
                                                          node({._symbol = "tag"})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::NotEqual,
                                                                         symbol("person1"),
                                                                         symbol("person2")),
                                                                  unary(UnaryOperator::Not,
                                                                        patternPredicate(pattern({path({node({._symbol = "person1"}),
                                                                                                        edge({._labels = labels({"KNOWS"})}),
                                                                                                        node({._symbol = "person2"})})})))})))})},
                       returning(projection({._items = {named(property({"person1", "id"}), "person1Id"),
                                                        named(property({"person2", "id"}), "person2Id"),
                                                        named(distinct(invocation({"count"}, {symbol("mutualFriend")})),
                                                              "mutualFriendCount")},
                                             ._orderBy = orderBy({{symbol("mutualFriendCount"), OrderByType::DESC},
                                                                  {symbol("person1Id"), OrderByType::ASC},
                                                                  {symbol("person2Id"), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesBi19CreateGraph) {
    expectParse(R"(
        CALL gds.graph.project.cypher(
            'bi19',
            'MATCH (p:Person) RETURN id(p) AS id',
            'MATCH
              (personA:Person)-[:KNOWS]-(personB:Person),
              (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
            WITH
              id(personA) AS source,
              id(personB) AS target,
              count(replyOf) AS numInteractions
            RETURN
              source,
              target,
              CASE WHEN round(40-sqrt(numInteractions)) > 1 THEN round(40-sqrt(numInteractions)) ELSE 1 END AS weight'
        )
    )",
                {query({call({._invocation = invocation({"gds", "graph", "project", "cypher"},
                                                        {string("bi19"),
                                                         string("MATCH (p:Person) RETURN id(p) AS id"),
                                                         string("MATCH\n"
                                                                "              (personA:Person)-[:KNOWS]-(personB:Person),\n"
                                                                "              (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)\n"
                                                                "            WITH\n"
                                                                "              id(personA) AS source,\n"
                                                                "              id(personB) AS target,\n"
                                                                "              count(replyOf) AS numInteractions\n"
                                                                "            RETURN\n"
                                                                "              source,\n"
                                                                "              target,\n"
                                                                "              CASE WHEN round(40-sqrt(numInteractions)) > 1 THEN round(40-sqrt(numInteractions)) ELSE 1 END AS weight")}),
                              ._standalone = true})})});
}

TEST_F(LdbcQueryTest, parsesBi19DropGraph) {
    expectParse(R"(
        CALL gds.graph.drop('bi19', false)
    )",
                {query({call({._invocation = invocation({"gds", "graph", "drop"}, {string("bi19"), boolean(false)}),
                              ._standalone = true})})});
}

TEST_F(LdbcQueryTest, parsesBi19WithoutPrecomputation) {
    expectParse(R"(
        // Q19. Interaction path between cities
        // Requires the Neo4j Graph Data Science library
        /*
        :params { city1Id: 669, city2Id: 648 }
        */
        MATCH
          (person1:Person)-[:IS_LOCATED_IN]->(city1:City {id: $city1Id}),
          (person2:Person)-[:IS_LOCATED_IN]->(city2:City {id: $city2Id})
        CALL gds.shortestPath.dijkstra.stream({
          nodeQuery: 'MATCH (p:Person) RETURN id(p) AS id',
          relationshipQuery:
            'MATCH
               (personA:Person)-[:KNOWS]-(personB:Person),
               (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
             RETURN
               id(personA) AS source,
               id(personB) AS target,
               1.0/count(replyOf) AS weight',
          sourceNode: person1,
          targetNode: person2,
          relationshipWeightProperty: 'weight'
        })
        YIELD totalCost
        RETURN person1.id, person2.id, totalCost AS totalWeight
        ORDER BY totalWeight ASC, person1.id ASC, person2.id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person1", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "city1",
                                                                ._labels = labels({"City"}),
                                                                ._properties = properties({{"id", parameter("city1Id")}})})}),
                                                    path({node({._symbol = "person2", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "city2",
                                                                ._labels = labels({"City"}),
                                                                ._properties = properties({{"id", parameter("city2Id")}})})})})}),
                        call({._invocation = invocation({"gds", "shortestPath", "dijkstra", "stream"},
                                                        {map({{"nodeQuery", string("MATCH (p:Person) RETURN id(p) AS id")},
                                                              {"relationshipQuery", string("MATCH\n"
                                                                                           "               (personA:Person)-[:KNOWS]-(personB:Person),\n"
                                                                                           "               (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)\n"
                                                                                           "             RETURN\n"
                                                                                           "               id(personA) AS source,\n"
                                                                                           "               id(personB) AS target,\n"
                                                                                           "               1.0/count(replyOf) AS weight")},
                                                              {"sourceNode", symbol("person1")},
                                                              {"targetNode", symbol("person2")},
                                                              {"relationshipWeightProperty", string("weight")}})}),
                              ._yield = yield({symbol("totalCost")})})},
                       returning(projection({._items = {property({"person1", "id"}),
                                                        property({"person2", "id"}),
                                                        named(symbol("totalCost"), "totalWeight")},
                                             ._orderBy = orderBy({{symbol("totalWeight"), OrderByType::ASC},
                                                                  {property({"person1", "id"}), OrderByType::ASC},
                                                                  {property({"person2", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesBi19) {
    expectParse(R"(
        // Q19. Interaction path between cities
        // Requires the Neo4j Graph Data Science library
        /*
        :params { city1Id: 669, city2Id: 648 }
        */
        MATCH
          (person1:Person)-[:IS_LOCATED_IN]->(city1:City {id: $city1Id}),
          (person2:Person)-[:IS_LOCATED_IN]->(city2:City {id: $city2Id})
        CALL gds.shortestPath.dijkstra.stream('bi19', {
          sourceNode: person1,
          targetNode: person2,
          relationshipWeightProperty: 'weight'
        })
        YIELD totalCost
        WITH person1.id AS person1Id, person2.id AS person2Id, totalCost AS totalWeight
        ORDER BY totalWeight ASC, person1.id ASC, person2.id ASC
        WITH collect({person1Id: person1Id, person2Id: person2Id, totalWeight: totalWeight}) AS results
        UNWIND results AS result
        WITH result.person1Id AS person1Id, result.person2Id AS person2Id, result.totalWeight AS totalWeight
        WHERE totalWeight = results[0].totalWeight
        RETURN person1Id, person2Id, totalWeight
        ORDER BY person1Id, person2Id
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "person1", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "city1",
                                                                ._labels = labels({"City"}),
                                                                ._properties = properties({{"id", parameter("city1Id")}})})}),
                                                    path({node({._symbol = "person2", ._labels = labels({"Person"})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"IS_LOCATED_IN"})}),
                                                          node({._symbol = "city2",
                                                                ._labels = labels({"City"}),
                                                                ._properties = properties({{"id", parameter("city2Id")}})})})})}),
                        call({._invocation = invocation({"gds", "shortestPath", "dijkstra", "stream"},
                                                        {string("bi19"),
                                                         map({{"sourceNode", symbol("person1")},
                                                              {"targetNode", symbol("person2")},
                                                              {"relationshipWeightProperty", string("weight")}})}),
                              ._yield = yield({symbol("totalCost")})}),
                        with({._projection = projection({._items = {named(property({"person1", "id"}), "person1Id"),
                                                                    named(property({"person2", "id"}), "person2Id"),
                                                                    named(symbol("totalCost"), "totalWeight")},
                                                         ._orderBy = orderBy({{symbol("totalWeight"), OrderByType::ASC},
                                                                              {property({"person1", "id"}),
                                                                               OrderByType::ASC},
                                                                              {property({"person2", "id"}),
                                                                               OrderByType::ASC}})})}),
                        with({._projection = projection({._items = {named(invocation({"collect"},
                                                                                     {map({{"person1Id", symbol("person1Id")},
                                                                                           {"person2Id", symbol("person2Id")},
                                                                                           {"totalWeight", symbol("totalWeight")}})}),
                                                                          "results")}})}),
                        unwind(symbol("results"), "result"),
                        with({._projection = projection({._items = {named(property({"result", "person1Id"}), "person1Id"),
                                                                    named(property({"result", "person2Id"}), "person2Id"),
                                                                    named(property({"result", "totalWeight"}),
                                                                          "totalWeight")}}),
                              ._where = where(binary(BinaryOperator::Equal,
                                                     symbol("totalWeight"),
                                                     lookup(index(symbol("results"), integer(0)), "totalWeight")))})},
                       returning(projection({._items = {symbol("person1Id"), symbol("person2Id"), symbol("totalWeight")},
                                             ._orderBy = orderBy({{symbol("person1Id"), OrderByType::ASC},
                                                                  {symbol("person2Id"), OrderByType::ASC}})})))});
}

TEST_F(LdbcQueryTest, parsesBi20CreateGraph) {
    expectParse(R"(
        CALL gds.graph.project.cypher(
            'bi20',
            'MATCH (p:Person) RETURN id(p) AS id',
            'MATCH
              (personA:Person)-[:KNOWS]-(personB:Person),
              (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
            RETURN
              id(personA) AS source,
              id(personB) AS target,
              min(abs(saA.classYear - saB.classYear)) + 1 AS weight'
        )
    )",
                {query({call({._invocation = invocation({"gds", "graph", "project", "cypher"},
                                                        {string("bi20"),
                                                         string("MATCH (p:Person) RETURN id(p) AS id"),
                                                         string("MATCH\n"
                                                                "              (personA:Person)-[:KNOWS]-(personB:Person),\n"
                                                                "              (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)\n"
                                                                "            RETURN\n"
                                                                "              id(personA) AS source,\n"
                                                                "              id(personB) AS target,\n"
                                                                "              min(abs(saA.classYear - saB.classYear)) + 1 AS weight")}),
                              ._standalone = true})})});
}

TEST_F(LdbcQueryTest, parsesBi20DropGraph) {
    expectParse(R"(
        CALL gds.graph.drop('bi20', false)
    )",
                {query({call({._invocation = invocation({"gds", "graph", "drop"}, {string("bi20"), boolean(false)}),
                              ._standalone = true})})});
}

TEST_F(LdbcQueryTest, parsesBi20WithoutPrecomputation) {
    expectParse(R"(
        // Q20. Recruitment
        // Requires the Neo4j Graph Data Science library
        /*
        :params { company: 'Falcon_Air', person2Id: 66 }
        */
        MATCH
          (company:Company {name: $company})<-[:WORK_AT]-(person1:Person),
          (person2:Person {id: $person2Id})
        CALL gds.shortestPath.dijkstra.stream({
          nodeQuery: 'MATCH (p:Person) RETURN id(p) AS id',
          relationshipQuery:
            'MATCH
               (personA:Person)-[:KNOWS]-(personB:Person),
               (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
             RETURN
               id(personA) AS source,
               id(personB) AS target,
               min(abs(saA.classYear - saB.classYear)) + 1 AS weight',
          sourceNode: person1,
          targetNode: person2,
          relationshipWeightProperty: 'weight'
        })
        YIELD totalCost
        WHERE person1.id <> $person2Id
        RETURN person1.id, totalCost AS totalWeight
        ORDER BY totalWeight ASC, person1.id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "company",
                                                                ._labels = labels({"Company"}),
                                                                ._properties = properties({{"name", parameter("company")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"WORK_AT"})}),
                                                          node({._symbol = "person1", ._labels = labels({"Person"})})}),
                                                    path({node({._symbol = "person2",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("person2Id")}})})})})}),
                        call({._invocation = invocation({"gds", "shortestPath", "dijkstra", "stream"},
                                                        {map({{"nodeQuery", string("MATCH (p:Person) RETURN id(p) AS id")},
                                                              {"relationshipQuery", string("MATCH\n"
                                                                                           "               (personA:Person)-[:KNOWS]-(personB:Person),\n"
                                                                                           "               (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)\n"
                                                                                           "             RETURN\n"
                                                                                           "               id(personA) AS source,\n"
                                                                                           "               id(personB) AS target,\n"
                                                                                           "               min(abs(saA.classYear - saB.classYear)) + 1 AS weight")},
                                                              {"sourceNode", symbol("person1")},
                                                              {"targetNode", symbol("person2")},
                                                              {"relationshipWeightProperty", string("weight")}})}),
                              ._yield = yield({symbol("totalCost")},
                                              where(binary(BinaryOperator::NotEqual,
                                                           property({"person1", "id"}),
                                                           parameter("person2Id"))))})},
                       returning(projection({._items = {property({"person1", "id"}),
                                                        named(symbol("totalCost"), "totalWeight")},
                                             ._orderBy = orderBy({{symbol("totalWeight"), OrderByType::ASC},
                                                                  {property({"person1", "id"}), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}

TEST_F(LdbcQueryTest, parsesBi20) {
    expectParse(R"(
        // Q20. Recruitment
        // Requires the Neo4j Graph Data Science library
        /*
        :params { company: 'Falcon_Air', person2Id: 66 }
        */
        MATCH
          (company:Company {name: $company})<-[:WORK_AT]-(person1:Person),
          (person2:Person {id: $person2Id})
        CALL gds.shortestPath.dijkstra.stream('bi20', {
          sourceNode: person1,
          targetNode: person2,
          relationshipWeightProperty: 'weight'
        })
        YIELD totalCost
        WHERE person1.id <> $person2Id
        WITH person1.id AS person1Id, totalCost AS totalWeight
        ORDER BY totalWeight ASC
        WITH collect({person1Id: person1Id, totalWeight: totalWeight}) AS results
        UNWIND results AS result
        WITH result.person1Id AS person1Id, result.totalWeight AS totalWeight
        WHERE totalWeight = results[0].totalWeight
        RETURN person1Id, totalWeight
        ORDER BY person1Id ASC
        LIMIT 20
    )",
                {query({match({._pattern = pattern({path({node({._symbol = "company",
                                                                ._labels = labels({"Company"}),
                                                                ._properties = properties({{"name", parameter("company")}})}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._labels = labels({"WORK_AT"})}),
                                                          node({._symbol = "person1", ._labels = labels({"Person"})})}),
                                                    path({node({._symbol = "person2",
                                                                ._labels = labels({"Person"}),
                                                                ._properties = properties({{"id", parameter("person2Id")}})})})})}),
                        call({._invocation = invocation({"gds", "shortestPath", "dijkstra", "stream"},
                                                        {string("bi20"),
                                                         map({{"sourceNode", symbol("person1")},
                                                              {"targetNode", symbol("person2")},
                                                              {"relationshipWeightProperty", string("weight")}})}),
                              ._yield = yield({symbol("totalCost")},
                                              where(binary(BinaryOperator::NotEqual,
                                                           property({"person1", "id"}),
                                                           parameter("person2Id"))))}),
                        with({._projection = projection({._items = {named(property({"person1", "id"}), "person1Id"),
                                                                    named(symbol("totalCost"), "totalWeight")},
                                                         ._orderBy = orderBy({{symbol("totalWeight"),
                                                                               OrderByType::ASC}})})}),
                        with({._projection = projection({._items = {named(invocation({"collect"},
                                                                                     {map({{"person1Id", symbol("person1Id")},
                                                                                           {"totalWeight", symbol("totalWeight")}})}),
                                                                          "results")}})}),
                        unwind(symbol("results"), "result"),
                        with({._projection = projection({._items = {named(property({"result", "person1Id"}), "person1Id"),
                                                                    named(property({"result", "totalWeight"}),
                                                                          "totalWeight")}}),
                              ._where = where(binary(BinaryOperator::Equal,
                                                     symbol("totalWeight"),
                                                     lookup(index(symbol("results"), integer(0)), "totalWeight")))})},
                       returning(projection({._items = {symbol("person1Id"), symbol("totalWeight")},
                                             ._orderBy = orderBy({{symbol("person1Id"), OrderByType::ASC}}),
                                             ._limit = limit(integer(20))})))});
}
