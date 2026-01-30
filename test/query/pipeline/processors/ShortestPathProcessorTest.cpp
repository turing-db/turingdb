#include <gtest/gtest.h>

#include <math.h>

#include "SystemManager.h"
#include "Graph.h"
#include "TuringDB.h"
#include "processors/ProcessorTester.h"
#include "versioning/Transaction.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"
#include "versioning/Change.h"
#include "LocalMemory.h"

#include "PipelineV2.h"
#include "PipelineBuilder.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"
#include "processors/MaterializeProcessor.h"
#include "processors/ShortestPathProcessor.h"
#include "processors/LambdaProcessor.h"

#include "FileUtils.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Road network Cypher query - creates 100 cities and 400 roads
static const char* ROAD_NETWORK_CYPHER = R"(
CREATE (n0:City {id: '0', name: 'Springfield'}),
(n1:City {id: '1', name: 'Riverside'}),
(n2:City {id: '2', name: 'Fairview'}),
(n3:City {id: '3', name: 'Clinton'}),
(n4:City {id: '4', name: 'Georgetown'}),
(n5:City {id: '5', name: 'Madison'}),
(n6:City {id: '6', name: 'Arlington'}),
(n7:City {id: '7', name: 'Marion'}),
(n8:City {id: '8', name: 'Salem'}),
(n9:City {id: '9', name: 'Franklin'}),
(n10:City {id: '10', name: 'Greenville'}),
(n11:City {id: '11', name: 'Bristol'}),
(n12:City {id: '12', name: 'Manchester'}),
(n13:City {id: '13', name: 'Oxford'}),
(n14:City {id: '14', name: 'Clayton'}),
(n15:City {id: '15', name: 'Ashland'}),
(n16:City {id: '16', name: 'Burlington'}),
(n17:City {id: '17', name: 'Jackson'}),
(n18:City {id: '18', name: 'Milton'}),
(n19:City {id: '19', name: 'Newton'}),
(n20:City {id: '20', name: 'Lexington'}),
(n21:City {id: '21', name: 'Auburn'}),
(n22:City {id: '22', name: 'Concord'}),
(n23:City {id: '23', name: 'Windsor'}),
(n24:City {id: '24', name: 'Hudson'}),
(n25:City {id: '25', name: 'Lancaster'}),
(n26:City {id: '26', name: 'Centerville'}),
(n27:City {id: '27', name: 'Lincoln'}),
(n28:City {id: '28', name: 'Troy'}),
(n29:City {id: '29', name: 'Dayton'}),
(n30:City {id: '30', name: 'Chester'}),
(n31:City {id: '31', name: 'Newport'}),
(n32:City {id: '32', name: 'Florence'}),
(n33:City {id: '33', name: 'Clifton'}),
(n34:City {id: '34', name: 'Princeton'}),
(n35:City {id: '35', name: 'Hamilton'}),
(n36:City {id: '36', name: 'Richmond'}),
(n37:City {id: '37', name: 'Dover'}),
(n38:City {id: '38', name: 'Cleveland'}),
(n39:City {id: '39', name: 'Harrison'}),
(n40:City {id: '40', name: 'Fairfield'}),
(n41:City {id: '41', name: 'Milford'}),
(n42:City {id: '42', name: 'Shelby'}),
(n43:City {id: '43', name: 'Warren'}),
(n44:City {id: '44', name: 'Lebanon'}),
(n45:City {id: '45', name: 'Quincy'}),
(n46:City {id: '46', name: 'Plymouth'}),
(n47:City {id: '47', name: 'Trenton'}),
(n48:City {id: '48', name: 'Camden'}),
(n49:City {id: '49', name: 'Oakland'}),
(n50:City {id: '50', name: 'Lakewood'}),
(n51:City {id: '51', name: 'Wilmington'}),
(n52:City {id: '52', name: 'Monroe'}),
(n53:City {id: '53', name: 'Marietta'}),
(n54:City {id: '54', name: 'Perry'}),
(n55:City {id: '55', name: 'Athens'}),
(n56:City {id: '56', name: 'Sparta'}),
(n57:City {id: '57', name: 'Bloomfield'}),
(n58:City {id: '58', name: 'Waverly'}),
(n59:City {id: '59', name: 'Vernon'}),
(n60:City {id: '60', name: 'Somerset'}),
(n61:City {id: '61', name: 'Brighton'}),
(n62:City {id: '62', name: 'Durham'}),
(n63:City {id: '63', name: 'Midway'}),
(n64:City {id: '64', name: 'Lakeside'}),
(n65:City {id: '65', name: 'Rockville'}),
(n66:City {id: '66', name: 'Washington'}),
(n67:City {id: '67', name: 'Jefferson'}),
(n68:City {id: '68', name: 'Adams'}),
(n69:City {id: '69', name: 'Monroe'}),
(n70:City {id: '70', name: 'Tyler'}),
(n71:City {id: '71', name: 'Clayton'}),
(n72:City {id: '72', name: 'Fulton'}),
(n73:City {id: '73', name: 'Douglas'}),
(n74:City {id: '74', name: 'Marshall'}),
(n75:City {id: '75', name: 'Franklin'}),
(n76:City {id: '76', name: 'Warren'}),
(n77:City {id: '77', name: 'Lincoln'}),
(n78:City {id: '78', name: 'Jackson'}),
(n79:City {id: '79', name: 'Madison'}),
(n80:City {id: '80', name: 'Harrison'}),
(n81:City {id: '81', name: 'Marion'}),
(n82:City {id: '82', name: 'Clinton'}),
(n83:City {id: '83', name: 'Monroe'}),
(n84:City {id: '84', name: 'Washington'}),
(n85:City {id: '85', name: 'Jefferson'}),
(n86:City {id: '86', name: 'Adams'}),
(n87:City {id: '87', name: 'Tyler'}),
(n88:City {id: '88', name: 'Fulton'}),
(n89:City {id: '89', name: 'Douglas'}),
(n90:City {id: '90', name: 'Marshall'}),
(n91:City {id: '91', name: 'Perry'}),
(n92:City {id: '92', name: 'Grant'}),
(n93:City {id: '93', name: 'Sherman'}),
(n94:City {id: '94', name: 'Logan'}),
(n95:City {id: '95', name: 'Porter'}),
(n96:City {id: '96', name: 'Hayes'}),
(n97:City {id: '97', name: 'Garfield'}),
(n98:City {id: '98', name: 'Arthur'}),
(n99:City {id: '99', name: 'Cleveland'})

CREATE (n0)-[:ROAD {distance: 99.5, name: 'Highway 95'}]->(n1),
(n0)-[:ROAD {distance: 48.5, name: 'Route 18'}]->(n99),
(n0)-[:ROAD {distance: 113.1, name: 'Highway 76'}]->(n2),
(n0)-[:ROAD {distance: 69.1, name: 'Highway 12'}]->(n98),
(n1)-[:ROAD {distance: 40.6, name: 'Highway 72'}]->(n0),
(n1)-[:ROAD {distance: 37.8, name: 'Expressway 29'}]->(n3),
(n1)-[:ROAD {distance: 72.9, name: 'Road 1'}]->(n99),
(n1)-[:ROAD {distance: 116.2, name: 'Route 90'}]->(n35),
(n2)-[:ROAD {distance: 69.2, name: 'Road 20'}]->(n3),
(n2)-[:ROAD {distance: 40.1, name: 'Road 14'}]->(n0),
(n2)-[:ROAD {distance: 23.0, name: 'Highway 46'}]->(n4),
(n2)-[:ROAD {distance: 128.6, name: 'Road 6'}]->(n48),
(n3)-[:ROAD {distance: 112.2, name: 'Highway 49'}]->(n2),
(n3)-[:ROAD {distance: 21.0, name: 'Road 81'}]->(n4),
(n3)-[:ROAD {distance: 96.6, name: 'Road 74'}]->(n1),
(n3)-[:ROAD {distance: 36.9, name: 'Highway 6'}]->(n92),
(n4)-[:ROAD {distance: 102.6, name: 'Road 11'}]->(n3),
(n4)-[:ROAD {distance: 129.7, name: 'Highway 49'}]->(n5),
(n4)-[:ROAD {distance: 48.9, name: 'Road 21'}]->(n2),
(n4)-[:ROAD {distance: 61.8, name: 'Route 86'}]->(n6),
(n5)-[:ROAD {distance: 47.4, name: 'Highway 78'}]->(n4),
(n5)-[:ROAD {distance: 98.9, name: 'Route 21'}]->(n6),
(n5)-[:ROAD {distance: 74.7, name: 'Road 82'}]->(n7),
(n6)-[:ROAD {distance: 106.3, name: 'Route 88'}]->(n5),
(n6)-[:ROAD {distance: 55.4, name: 'Highway 30'}]->(n7),
(n6)-[:ROAD {distance: 125.1, name: 'Road 52'}]->(n4),
(n6)-[:ROAD {distance: 47.5, name: 'Route 73'}]->(n8),
(n6)-[:ROAD {distance: 132.7, name: 'Road 28'}]->(n83),
(n7)-[:ROAD {distance: 101.8, name: 'Expressway 83'}]->(n6),
(n7)-[:ROAD {distance: 74.2, name: 'Road 18'}]->(n8),
(n7)-[:ROAD {distance: 44.5, name: 'Road 96'}]->(n5),
(n7)-[:ROAD {distance: 91.8, name: 'Expressway 47'}]->(n9),
(n8)-[:ROAD {distance: 40.7, name: 'Route 66'}]->(n7),
(n8)-[:ROAD {distance: 79.1, name: 'Highway 15'}]->(n6),
(n8)-[:ROAD {distance: 31.4, name: 'Route 88'}]->(n10),
(n8)-[:ROAD {distance: 69.1, name: 'Highway 50'}]->(n11),
(n8)-[:ROAD {distance: 63.4, name: 'Expressway 68'}]->(n53),
(n9)-[:ROAD {distance: 45.2, name: 'Highway 88'}]->(n10),
(n9)-[:ROAD {distance: 110.9, name: 'Road 99'}]->(n7),
(n9)-[:ROAD {distance: 99.7, name: 'Highway 38'}]->(n11),
(n10)-[:ROAD {distance: 70.9, name: 'Expressway 1'}]->(n9),
(n10)-[:ROAD {distance: 143.5, name: 'Road 65'}]->(n11),
(n10)-[:ROAD {distance: 116.7, name: 'Highway 81'}]->(n8),
(n10)-[:ROAD {distance: 51.8, name: 'Route 20'}]->(n12),
(n11)-[:ROAD {distance: 62.3, name: 'Route 70'}]->(n10),
(n11)-[:ROAD {distance: 143.5, name: 'Highway 77'}]->(n9),
(n11)-[:ROAD {distance: 55.4, name: 'Highway 15'}]->(n13),
(n11)-[:ROAD {distance: 140.1, name: 'Road 31'}]->(n8),
(n11)-[:ROAD {distance: 18.1, name: 'Highway 11'}]->(n25),
(n12)-[:ROAD {distance: 112.5, name: 'Highway 98'}]->(n13),
(n12)-[:ROAD {distance: 84.6, name: 'Route 17'}]->(n10),
(n12)-[:ROAD {distance: 102.4, name: 'Route 34'}]->(n14),
(n13)-[:ROAD {distance: 83.9, name: 'Expressway 28'}]->(n12),
(n13)-[:ROAD {distance: 140.0, name: 'Route 92'}]->(n14),
(n13)-[:ROAD {distance: 53.6, name: 'Road 57'}]->(n11),
(n13)-[:ROAD {distance: 135.9, name: 'Expressway 16'}]->(n15),
(n14)-[:ROAD {distance: 44.7, name: 'Highway 44'}]->(n13),
(n14)-[:ROAD {distance: 12.9, name: 'Route 76'}]->(n15),
(n14)-[:ROAD {distance: 40.8, name: 'Highway 91'}]->(n12),
(n14)-[:ROAD {distance: 98.4, name: 'Route 9'}]->(n16),
(n15)-[:ROAD {distance: 136.8, name: 'Road 10'}]->(n14),
(n15)-[:ROAD {distance: 82.0, name: 'Road 86'}]->(n16),
(n15)-[:ROAD {distance: 78.0, name: 'Route 93'}]->(n13),
(n15)-[:ROAD {distance: 141.0, name: 'Expressway 32'}]->(n17),
(n16)-[:ROAD {distance: 119.8, name: 'Expressway 25'}]->(n15),
(n16)-[:ROAD {distance: 23.2, name: 'Expressway 46'}]->(n17),
(n16)-[:ROAD {distance: 69.3, name: 'Expressway 94'}]->(n14),
(n16)-[:ROAD {distance: 17.6, name: 'Highway 8'}]->(n18),
(n17)-[:ROAD {distance: 66.4, name: 'Road 14'}]->(n16),
(n17)-[:ROAD {distance: 44.8, name: 'Route 69'}]->(n18),
(n17)-[:ROAD {distance: 72.8, name: 'Expressway 24'}]->(n15),
(n17)-[:ROAD {distance: 49.0, name: 'Route 10'}]->(n19),
(n18)-[:ROAD {distance: 72.0, name: 'Highway 7'}]->(n17),
(n18)-[:ROAD {distance: 101.3, name: 'Highway 12'}]->(n19),
(n18)-[:ROAD {distance: 139.7, name: 'Route 22'}]->(n16),
(n18)-[:ROAD {distance: 66.9, name: 'Expressway 28'}]->(n41),
(n19)-[:ROAD {distance: 131.1, name: 'Highway 22'}]->(n18),
(n19)-[:ROAD {distance: 63.1, name: 'Expressway 34'}]->(n20),
(n19)-[:ROAD {distance: 139.7, name: 'Expressway 37'}]->(n17),
(n19)-[:ROAD {distance: 69.2, name: 'Expressway 20'}]->(n21),
(n20)-[:ROAD {distance: 36.6, name: 'Route 8'}]->(n19),
(n20)-[:ROAD {distance: 91.1, name: 'Highway 96'}]->(n21),
(n20)-[:ROAD {distance: 53.9, name: 'Highway 75'}]->(n22),
(n21)-[:ROAD {distance: 76.8, name: 'Route 8'}]->(n20),
(n21)-[:ROAD {distance: 144.5, name: 'Highway 24'}]->(n22),
(n21)-[:ROAD {distance: 19.6, name: 'Highway 87'}]->(n19),
(n21)-[:ROAD {distance: 130.7, name: 'Expressway 16'}]->(n23),
(n22)-[:ROAD {distance: 141.8, name: 'Route 75'}]->(n21),
(n22)-[:ROAD {distance: 93.2, name: 'Highway 54'}]->(n23),
(n22)-[:ROAD {distance: 102.0, name: 'Road 34'}]->(n20),
(n22)-[:ROAD {distance: 38.6, name: 'Road 31'}]->(n24),
(n23)-[:ROAD {distance: 47.2, name: 'Route 86'}]->(n22),
(n23)-[:ROAD {distance: 100.4, name: 'Expressway 41'}]->(n24),
(n23)-[:ROAD {distance: 140.1, name: 'Highway 2'}]->(n21),
(n23)-[:ROAD {distance: 74.2, name: 'Highway 10'}]->(n25),
(n24)-[:ROAD {distance: 85.3, name: 'Road 17'}]->(n23),
(n24)-[:ROAD {distance: 140.7, name: 'Highway 32'}]->(n25),
(n24)-[:ROAD {distance: 61.7, name: 'Route 57'}]->(n22),
(n24)-[:ROAD {distance: 126.7, name: 'Road 79'}]->(n26),
(n25)-[:ROAD {distance: 147.7, name: 'Highway 86'}]->(n24),
(n25)-[:ROAD {distance: 124.4, name: 'Road 85'}]->(n26),
(n25)-[:ROAD {distance: 24.5, name: 'Route 34'}]->(n23),
(n25)-[:ROAD {distance: 26.2, name: 'Highway 96'}]->(n27),
(n25)-[:ROAD {distance: 87.5, name: 'Road 37'}]->(n11),
(n26)-[:ROAD {distance: 94.7, name: 'Road 27'}]->(n25),
(n26)-[:ROAD {distance: 106.2, name: 'Road 65'}]->(n27),
(n26)-[:ROAD {distance: 78.4, name: 'Highway 12'}]->(n24),
(n26)-[:ROAD {distance: 98.8, name: 'Road 6'}]->(n28),
(n27)-[:ROAD {distance: 10.5, name: 'Route 82'}]->(n26),
(n27)-[:ROAD {distance: 147.2, name: 'Route 95'}]->(n28),
(n27)-[:ROAD {distance: 71.9, name: 'Expressway 72'}]->(n25),
(n27)-[:ROAD {distance: 11.4, name: 'Highway 89'}]->(n93),
(n28)-[:ROAD {distance: 136.6, name: 'Highway 48'}]->(n27),
(n28)-[:ROAD {distance: 91.6, name: 'Route 56'}]->(n29),
(n28)-[:ROAD {distance: 27.8, name: 'Road 47'}]->(n26),
(n28)-[:ROAD {distance: 135.9, name: 'Highway 46'}]->(n30),
(n29)-[:ROAD {distance: 39.4, name: 'Route 86'}]->(n28),
(n29)-[:ROAD {distance: 24.4, name: 'Expressway 80'}]->(n58),
(n29)-[:ROAD {distance: 114.9, name: 'Route 21'}]->(n39),
(n29)-[:ROAD {distance: 146.7, name: 'Route 53'}]->(n97),
(n30)-[:ROAD {distance: 13.5, name: 'Road 53'}]->(n31),
(n30)-[:ROAD {distance: 122.3, name: 'Route 35'}]->(n28),
(n30)-[:ROAD {distance: 32.3, name: 'Highway 49'}]->(n32),
(n30)-[:ROAD {distance: 132.1, name: 'Expressway 29'}]->(n56),
(n31)-[:ROAD {distance: 37.9, name: 'Expressway 45'}]->(n30),
(n31)-[:ROAD {distance: 52.7, name: 'Route 29'}]->(n32),
(n31)-[:ROAD {distance: 13.3, name: 'Route 52'}]->(n33),
(n32)-[:ROAD {distance: 56.0, name: 'Highway 99'}]->(n31),
(n32)-[:ROAD {distance: 49.1, name: 'Expressway 87'}]->(n33),
(n32)-[:ROAD {distance: 147.4, name: 'Road 4'}]->(n30),
(n32)-[:ROAD {distance: 26.1, name: 'Road 23'}]->(n34),
(n33)-[:ROAD {distance: 91.3, name: 'Road 5'}]->(n32),
(n33)-[:ROAD {distance: 25.2, name: 'Expressway 45'}]->(n34),
(n33)-[:ROAD {distance: 112.0, name: 'Road 56'}]->(n31),
(n33)-[:ROAD {distance: 94.9, name: 'Highway 50'}]->(n35),
(n34)-[:ROAD {distance: 135.9, name: 'Route 33'}]->(n33),
(n34)-[:ROAD {distance: 16.2, name: 'Expressway 1'}]->(n35),
(n34)-[:ROAD {distance: 82.8, name: 'Route 47'}]->(n32),
(n34)-[:ROAD {distance: 70.4, name: 'Road 80'}]->(n36),
(n35)-[:ROAD {distance: 53.9, name: 'Highway 93'}]->(n34),
(n35)-[:ROAD {distance: 136.0, name: 'Road 86'}]->(n36),
(n35)-[:ROAD {distance: 67.2, name: 'Expressway 90'}]->(n33),
(n35)-[:ROAD {distance: 51.4, name: 'Route 25'}]->(n37),
(n35)-[:ROAD {distance: 68.9, name: 'Expressway 87'}]->(n1),
(n36)-[:ROAD {distance: 114.7, name: 'Route 79'}]->(n35),
(n36)-[:ROAD {distance: 89.7, name: 'Expressway 71'}]->(n37),
(n36)-[:ROAD {distance: 126.7, name: 'Road 37'}]->(n34),
(n36)-[:ROAD {distance: 39.4, name: 'Road 60'}]->(n38),
(n37)-[:ROAD {distance: 71.9, name: 'Route 66'}]->(n36),
(n37)-[:ROAD {distance: 76.2, name: 'Route 85'}]->(n38),
(n37)-[:ROAD {distance: 21.9, name: 'Road 12'}]->(n35),
(n37)-[:ROAD {distance: 124.6, name: 'Route 87'}]->(n39),
(n38)-[:ROAD {distance: 53.5, name: 'Route 19'}]->(n37),
(n38)-[:ROAD {distance: 13.4, name: 'Route 61'}]->(n39),
(n38)-[:ROAD {distance: 95.6, name: 'Highway 59'}]->(n36),
(n38)-[:ROAD {distance: 68.0, name: 'Route 92'}]->(n40),
(n39)-[:ROAD {distance: 107.5, name: 'Expressway 52'}]->(n38),
(n39)-[:ROAD {distance: 44.2, name: 'Highway 97'}]->(n37),
(n39)-[:ROAD {distance: 130.5, name: 'Highway 55'}]->(n41),
(n39)-[:ROAD {distance: 40.6, name: 'Expressway 7'}]->(n29),
(n40)-[:ROAD {distance: 88.0, name: 'Highway 59'}]->(n41),
(n40)-[:ROAD {distance: 28.7, name: 'Expressway 86'}]->(n38),
(n40)-[:ROAD {distance: 84.4, name: 'Road 97'}]->(n42),
(n40)-[:ROAD {distance: 134.8, name: 'Expressway 71'}]->(n64),
(n41)-[:ROAD {distance: 72.4, name: 'Route 96'}]->(n40),
(n41)-[:ROAD {distance: 130.6, name: 'Expressway 34'}]->(n42),
(n41)-[:ROAD {distance: 115.2, name: 'Road 99'}]->(n39),
(n41)-[:ROAD {distance: 118.9, name: 'Expressway 81'}]->(n43),
(n41)-[:ROAD {distance: 43.5, name: 'Expressway 10'}]->(n18),
(n42)-[:ROAD {distance: 109.9, name: 'Route 35'}]->(n41),
(n42)-[:ROAD {distance: 57.0, name: 'Highway 18'}]->(n43),
(n42)-[:ROAD {distance: 31.1, name: 'Expressway 89'}]->(n40),
(n42)-[:ROAD {distance: 31.4, name: 'Route 9'}]->(n44),
(n43)-[:ROAD {distance: 68.1, name: 'Road 70'}]->(n42),
(n43)-[:ROAD {distance: 75.2, name: 'Highway 27'}]->(n44),
(n43)-[:ROAD {distance: 126.6, name: 'Expressway 99'}]->(n41),
(n43)-[:ROAD {distance: 91.8, name: 'Highway 98'}]->(n45),
(n44)-[:ROAD {distance: 90.6, name: 'Expressway 1'}]->(n43),
(n44)-[:ROAD {distance: 142.0, name: 'Road 97'}]->(n45),
(n44)-[:ROAD {distance: 64.6, name: 'Expressway 69'}]->(n42),
(n44)-[:ROAD {distance: 114.7, name: 'Route 63'}]->(n46),
(n45)-[:ROAD {distance: 40.7, name: 'Expressway 63'}]->(n44),
(n45)-[:ROAD {distance: 14.1, name: 'Road 86'}]->(n46),
(n45)-[:ROAD {distance: 105.1, name: 'Expressway 93'}]->(n43),
(n45)-[:ROAD {distance: 33.1, name: 'Expressway 17'}]->(n47),
(n46)-[:ROAD {distance: 147.3, name: 'Highway 51'}]->(n45),
(n46)-[:ROAD {distance: 92.9, name: 'Highway 11'}]->(n47),
(n46)-[:ROAD {distance: 100.0, name: 'Route 60'}]->(n44),
(n46)-[:ROAD {distance: 35.4, name: 'Road 49'}]->(n48),
(n47)-[:ROAD {distance: 55.8, name: 'Expressway 42'}]->(n46),
(n47)-[:ROAD {distance: 57.3, name: 'Expressway 36'}]->(n48),
(n47)-[:ROAD {distance: 115.3, name: 'Expressway 33'}]->(n45),
(n47)-[:ROAD {distance: 126.9, name: 'Expressway 3'}]->(n49),
(n48)-[:ROAD {distance: 114.9, name: 'Highway 45'}]->(n47),
(n48)-[:ROAD {distance: 41.4, name: 'Highway 84'}]->(n49),
(n48)-[:ROAD {distance: 15.6, name: 'Highway 32'}]->(n46),
(n48)-[:ROAD {distance: 37.9, name: 'Highway 80'}]->(n2),
(n48)-[:ROAD {distance: 31.3, name: 'Route 61'}]->(n94),
(n49)-[:ROAD {distance: 103.7, name: 'Route 60'}]->(n48),
(n49)-[:ROAD {distance: 107.9, name: 'Road 22'}]->(n50),
(n49)-[:ROAD {distance: 94.8, name: 'Highway 21'}]->(n47),
(n49)-[:ROAD {distance: 145.1, name: 'Highway 75'}]->(n51),
(n49)-[:ROAD {distance: 13.6, name: 'Road 74'}]->(n95),
(n50)-[:ROAD {distance: 104.8, name: 'Expressway 51'}]->(n49),
(n50)-[:ROAD {distance: 141.8, name: 'Route 10'}]->(n51),
(n50)-[:ROAD {distance: 92.9, name: 'Route 14'}]->(n52),
(n51)-[:ROAD {distance: 107.6, name: 'Road 88'}]->(n50),
(n51)-[:ROAD {distance: 94.1, name: 'Highway 73'}]->(n52),
(n51)-[:ROAD {distance: 119.6, name: 'Road 69'}]->(n49),
(n51)-[:ROAD {distance: 70.0, name: 'Road 9'}]->(n53),
(n51)-[:ROAD {distance: 80.8, name: 'Road 2'}]->(n73),
(n52)-[:ROAD {distance: 128.9, name: 'Expressway 14'}]->(n51),
(n52)-[:ROAD {distance: 70.7, name: 'Road 82'}]->(n50),
(n52)-[:ROAD {distance: 134.8, name: 'Expressway 91'}]->(n81),
(n52)-[:ROAD {distance: 31.4, name: 'Route 94'}]->(n80),
(n53)-[:ROAD {distance: 83.0, name: 'Road 79'}]->(n54),
(n53)-[:ROAD {distance: 123.1, name: 'Expressway 60'}]->(n51),
(n53)-[:ROAD {distance: 71.0, name: 'Road 42'}]->(n8),
(n54)-[:ROAD {distance: 129.2, name: 'Highway 36'}]->(n53),
(n54)-[:ROAD {distance: 133.4, name: 'Route 97'}]->(n55),
(n54)-[:ROAD {distance: 75.1, name: 'Expressway 44'}]->(n56),
(n55)-[:ROAD {distance: 14.0, name: 'Road 24'}]->(n54),
(n55)-[:ROAD {distance: 78.3, name: 'Road 34'}]->(n56),
(n55)-[:ROAD {distance: 57.6, name: 'Road 72'}]->(n57),
(n56)-[:ROAD {distance: 11.4, name: 'Route 11'}]->(n55),
(n56)-[:ROAD {distance: 43.8, name: 'Expressway 63'}]->(n57),
(n56)-[:ROAD {distance: 87.7, name: 'Route 89'}]->(n54),
(n56)-[:ROAD {distance: 76.7, name: 'Expressway 58'}]->(n30),
(n57)-[:ROAD {distance: 121.0, name: 'Highway 38'}]->(n56),
(n57)-[:ROAD {distance: 41.0, name: 'Route 40'}]->(n58),
(n57)-[:ROAD {distance: 103.0, name: 'Road 61'}]->(n55),
(n57)-[:ROAD {distance: 87.5, name: 'Road 55'}]->(n59),
(n58)-[:ROAD {distance: 149.5, name: 'Road 46'}]->(n57),
(n58)-[:ROAD {distance: 108.4, name: 'Road 40'}]->(n59),
(n58)-[:ROAD {distance: 45.2, name: 'Highway 93'}]->(n60),
(n58)-[:ROAD {distance: 37.0, name: 'Highway 96'}]->(n29),
(n59)-[:ROAD {distance: 85.0, name: 'Route 25'}]->(n58),
(n59)-[:ROAD {distance: 40.3, name: 'Expressway 36'}]->(n60),
(n59)-[:ROAD {distance: 111.4, name: 'Road 13'}]->(n57),
(n59)-[:ROAD {distance: 126.6, name: 'Road 30'}]->(n61),
(n60)-[:ROAD {distance: 60.5, name: 'Road 2'}]->(n59),
(n60)-[:ROAD {distance: 109.1, name: 'Route 36'}]->(n61),
(n60)-[:ROAD {distance: 16.4, name: 'Highway 71'}]->(n58),
(n60)-[:ROAD {distance: 50.9, name: 'Route 82'}]->(n62),
(n61)-[:ROAD {distance: 131.6, name: 'Expressway 14'}]->(n60),
(n61)-[:ROAD {distance: 132.2, name: 'Road 61'}]->(n62),
(n61)-[:ROAD {distance: 77.0, name: 'Road 24'}]->(n59),
(n61)-[:ROAD {distance: 145.2, name: 'Road 62'}]->(n63),
(n62)-[:ROAD {distance: 26.0, name: 'Highway 52'}]->(n61),
(n62)-[:ROAD {distance: 78.8, name: 'Highway 20'}]->(n63),
(n62)-[:ROAD {distance: 30.9, name: 'Road 11'}]->(n60),
(n62)-[:ROAD {distance: 149.1, name: 'Highway 72'}]->(n64),
(n63)-[:ROAD {distance: 117.0, name: 'Route 67'}]->(n62),
(n63)-[:ROAD {distance: 63.3, name: 'Expressway 39'}]->(n64),
(n63)-[:ROAD {distance: 130.4, name: 'Expressway 40'}]->(n61),
(n63)-[:ROAD {distance: 89.6, name: 'Highway 79'}]->(n65),
(n64)-[:ROAD {distance: 144.4, name: 'Highway 98'}]->(n63),
(n64)-[:ROAD {distance: 39.1, name: 'Route 34'}]->(n62),
(n64)-[:ROAD {distance: 102.5, name: 'Route 31'}]->(n66),
(n64)-[:ROAD {distance: 34.3, name: 'Highway 21'}]->(n40),
(n65)-[:ROAD {distance: 10.4, name: 'Expressway 89'}]->(n66),
(n65)-[:ROAD {distance: 93.1, name: 'Road 5'}]->(n63),
(n65)-[:ROAD {distance: 42.4, name: 'Road 90'}]->(n67),
(n66)-[:ROAD {distance: 130.3, name: 'Highway 88'}]->(n65),
(n66)-[:ROAD {distance: 42.7, name: 'Road 81'}]->(n64),
(n66)-[:ROAD {distance: 92.6, name: 'Route 55'}]->(n72),
(n66)-[:ROAD {distance: 26.1, name: 'Route 83'}]->(n84),
(n67)-[:ROAD {distance: 30.9, name: 'Road 19'}]->(n68),
(n67)-[:ROAD {distance: 20.0, name: 'Route 40'}]->(n65),
(n67)-[:ROAD {distance: 93.3, name: 'Road 57'}]->(n69),
(n68)-[:ROAD {distance: 27.4, name: 'Road 90'}]->(n67),
(n68)-[:ROAD {distance: 66.4, name: 'Road 65'}]->(n69),
(n68)-[:ROAD {distance: 85.6, name: 'Expressway 11'}]->(n70),
(n69)-[:ROAD {distance: 93.7, name: 'Expressway 95'}]->(n68),
(n69)-[:ROAD {distance: 55.1, name: 'Road 4'}]->(n70),
(n69)-[:ROAD {distance: 22.8, name: 'Highway 98'}]->(n67),
(n69)-[:ROAD {distance: 104.1, name: 'Road 74'}]->(n71),
(n70)-[:ROAD {distance: 15.6, name: 'Route 61'}]->(n69),
(n70)-[:ROAD {distance: 82.7, name: 'Expressway 36'}]->(n71),
(n70)-[:ROAD {distance: 35.4, name: 'Expressway 82'}]->(n68),
(n70)-[:ROAD {distance: 124.0, name: 'Highway 61'}]->(n72),
(n70)-[:ROAD {distance: 58.7, name: 'Road 42'}]->(n83),
(n71)-[:ROAD {distance: 103.8, name: 'Route 43'}]->(n70),
(n71)-[:ROAD {distance: 67.6, name: 'Expressway 37'}]->(n72),
(n71)-[:ROAD {distance: 102.8, name: 'Expressway 98'}]->(n69),
(n71)-[:ROAD {distance: 87.0, name: 'Expressway 12'}]->(n73),
(n72)-[:ROAD {distance: 54.0, name: 'Road 15'}]->(n71),
(n72)-[:ROAD {distance: 145.8, name: 'Expressway 66'}]->(n73),
(n72)-[:ROAD {distance: 125.5, name: 'Highway 85'}]->(n70),
(n72)-[:ROAD {distance: 131.7, name: 'Expressway 53'}]->(n74),
(n72)-[:ROAD {distance: 17.6, name: 'Road 80'}]->(n66),
(n73)-[:ROAD {distance: 115.9, name: 'Expressway 98'}]->(n72),
(n73)-[:ROAD {distance: 17.2, name: 'Road 71'}]->(n74),
(n73)-[:ROAD {distance: 28.3, name: 'Road 57'}]->(n71),
(n73)-[:ROAD {distance: 133.3, name: 'Expressway 16'}]->(n51),
(n74)-[:ROAD {distance: 14.0, name: 'Route 91'}]->(n73),
(n74)-[:ROAD {distance: 32.2, name: 'Highway 71'}]->(n75),
(n74)-[:ROAD {distance: 67.1, name: 'Route 15'}]->(n72),
(n74)-[:ROAD {distance: 74.6, name: 'Highway 83'}]->(n76),
(n75)-[:ROAD {distance: 126.5, name: 'Expressway 92'}]->(n74),
(n75)-[:ROAD {distance: 50.9, name: 'Road 54'}]->(n76),
(n75)-[:ROAD {distance: 126.9, name: 'Expressway 32'}]->(n77),
(n76)-[:ROAD {distance: 74.0, name: 'Route 50'}]->(n75),
(n76)-[:ROAD {distance: 36.7, name: 'Route 9'}]->(n77),
(n76)-[:ROAD {distance: 48.7, name: 'Expressway 44'}]->(n74),
(n76)-[:ROAD {distance: 140.8, name: 'Road 1'}]->(n78),
(n77)-[:ROAD {distance: 49.6, name: 'Road 76'}]->(n76),
(n77)-[:ROAD {distance: 91.2, name: 'Expressway 20'}]->(n78),
(n77)-[:ROAD {distance: 72.5, name: 'Expressway 45'}]->(n75),
(n77)-[:ROAD {distance: 56.5, name: 'Expressway 59'}]->(n79),
(n78)-[:ROAD {distance: 140.6, name: 'Route 90'}]->(n77),
(n78)-[:ROAD {distance: 43.4, name: 'Expressway 30'}]->(n79),
(n78)-[:ROAD {distance: 129.8, name: 'Expressway 6'}]->(n76),
(n78)-[:ROAD {distance: 54.5, name: 'Expressway 91'}]->(n80),
(n79)-[:ROAD {distance: 137.9, name: 'Expressway 50'}]->(n78),
(n79)-[:ROAD {distance: 148.4, name: 'Route 64'}]->(n80),
(n79)-[:ROAD {distance: 145.1, name: 'Route 65'}]->(n77),
(n79)-[:ROAD {distance: 145.1, name: 'Road 13'}]->(n81),
(n80)-[:ROAD {distance: 132.4, name: 'Expressway 13'}]->(n79),
(n80)-[:ROAD {distance: 83.6, name: 'Expressway 2'}]->(n81),
(n80)-[:ROAD {distance: 111.1, name: 'Expressway 84'}]->(n78),
(n80)-[:ROAD {distance: 145.8, name: 'Highway 61'}]->(n82),
(n80)-[:ROAD {distance: 119.4, name: 'Road 44'}]->(n52),
(n81)-[:ROAD {distance: 97.3, name: 'Expressway 84'}]->(n80),
(n81)-[:ROAD {distance: 21.2, name: 'Road 87'}]->(n82),
(n81)-[:ROAD {distance: 130.3, name: 'Expressway 41'}]->(n79),
(n81)-[:ROAD {distance: 97.8, name: 'Expressway 70'}]->(n83),
(n81)-[:ROAD {distance: 15.0, name: 'Highway 31'}]->(n52),
(n82)-[:ROAD {distance: 98.4, name: 'Road 30'}]->(n81),
(n82)-[:ROAD {distance: 114.5, name: 'Expressway 13'}]->(n83),
(n82)-[:ROAD {distance: 116.5, name: 'Highway 57'}]->(n80),
(n82)-[:ROAD {distance: 33.3, name: 'Road 4'}]->(n84),
(n83)-[:ROAD {distance: 16.4, name: 'Highway 38'}]->(n82),
(n83)-[:ROAD {distance: 60.2, name: 'Expressway 19'}]->(n81),
(n83)-[:ROAD {distance: 44.2, name: 'Expressway 73'}]->(n6),
(n83)-[:ROAD {distance: 105.5, name: 'Route 22'}]->(n70),
(n83)-[:ROAD {distance: 34.5, name: 'Expressway 80'}]->(n84),
(n84)-[:ROAD {distance: 105.6, name: 'Expressway 75'}]->(n85),
(n84)-[:ROAD {distance: 30.0, name: 'Expressway 82'}]->(n82),
(n84)-[:ROAD {distance: 45.6, name: 'Road 86'}]->(n66),
(n84)-[:ROAD {distance: 11.3, name: 'Expressway 37'}]->(n83),
(n85)-[:ROAD {distance: 104.9, name: 'Route 10'}]->(n84),
(n85)-[:ROAD {distance: 71.8, name: 'Road 76'}]->(n86),
(n85)-[:ROAD {distance: 51.9, name: 'Expressway 89'}]->(n87),
(n86)-[:ROAD {distance: 45.0, name: 'Road 26'}]->(n85),
(n86)-[:ROAD {distance: 149.9, name: 'Expressway 14'}]->(n87),
(n86)-[:ROAD {distance: 43.2, name: 'Road 74'}]->(n88),
(n87)-[:ROAD {distance: 51.4, name: 'Road 3'}]->(n86),
(n87)-[:ROAD {distance: 146.9, name: 'Expressway 36'}]->(n88),
(n87)-[:ROAD {distance: 11.1, name: 'Highway 78'}]->(n85),
(n87)-[:ROAD {distance: 114.3, name: 'Road 30'}]->(n89),
(n88)-[:ROAD {distance: 95.0, name: 'Road 29'}]->(n87),
(n88)-[:ROAD {distance: 99.1, name: 'Road 87'}]->(n89),
(n88)-[:ROAD {distance: 115.8, name: 'Route 81'}]->(n86),
(n88)-[:ROAD {distance: 23.6, name: 'Highway 40'}]->(n90),
(n89)-[:ROAD {distance: 120.4, name: 'Highway 75'}]->(n88),
(n89)-[:ROAD {distance: 61.1, name: 'Route 12'}]->(n90),
(n89)-[:ROAD {distance: 137.3, name: 'Road 96'}]->(n87),
(n89)-[:ROAD {distance: 68.2, name: 'Route 17'}]->(n91),
(n90)-[:ROAD {distance: 120.1, name: 'Road 68'}]->(n89),
(n90)-[:ROAD {distance: 80.3, name: 'Road 22'}]->(n91),
(n90)-[:ROAD {distance: 46.0, name: 'Expressway 38'}]->(n88),
(n90)-[:ROAD {distance: 114.5, name: 'Road 15'}]->(n92),
(n91)-[:ROAD {distance: 75.6, name: 'Highway 19'}]->(n90),
(n91)-[:ROAD {distance: 115.6, name: 'Route 87'}]->(n92),
(n91)-[:ROAD {distance: 111.4, name: 'Expressway 72'}]->(n89),
(n91)-[:ROAD {distance: 61.2, name: 'Expressway 2'}]->(n93),
(n92)-[:ROAD {distance: 47.0, name: 'Highway 59'}]->(n91),
(n92)-[:ROAD {distance: 61.6, name: 'Road 75'}]->(n93),
(n92)-[:ROAD {distance: 63.3, name: 'Road 14'}]->(n90),
(n92)-[:ROAD {distance: 104.5, name: 'Expressway 4'}]->(n94),
(n92)-[:ROAD {distance: 96.7, name: 'Road 79'}]->(n3),
(n93)-[:ROAD {distance: 41.0, name: 'Highway 82'}]->(n92),
(n93)-[:ROAD {distance: 125.3, name: 'Road 84'}]->(n94),
(n93)-[:ROAD {distance: 67.2, name: 'Route 6'}]->(n91),
(n93)-[:ROAD {distance: 142.5, name: 'Road 64'}]->(n95),
(n93)-[:ROAD {distance: 26.3, name: 'Route 69'}]->(n27),
(n94)-[:ROAD {distance: 29.0, name: 'Expressway 48'}]->(n93),
(n94)-[:ROAD {distance: 103.9, name: 'Expressway 76'}]->(n95),
(n94)-[:ROAD {distance: 113.9, name: 'Route 54'}]->(n92),
(n94)-[:ROAD {distance: 101.7, name: 'Expressway 79'}]->(n48),
(n95)-[:ROAD {distance: 67.1, name: 'Road 5'}]->(n94),
(n95)-[:ROAD {distance: 106.6, name: 'Route 57'}]->(n96),
(n95)-[:ROAD {distance: 72.3, name: 'Route 47'}]->(n93),
(n95)-[:ROAD {distance: 23.9, name: 'Road 70'}]->(n49),
(n96)-[:ROAD {distance: 136.2, name: 'Road 8'}]->(n95),
(n96)-[:ROAD {distance: 65.7, name: 'Route 16'}]->(n97),
(n96)-[:ROAD {distance: 142.9, name: 'Expressway 12'}]->(n98),
(n97)-[:ROAD {distance: 102.8, name: 'Highway 7'}]->(n96),
(n97)-[:ROAD {distance: 120.2, name: 'Route 17'}]->(n98),
(n97)-[:ROAD {distance: 120.2, name: 'Route 9'}]->(n99),
(n97)-[:ROAD {distance: 126.2, name: 'Route 76'}]->(n29),
(n98)-[:ROAD {distance: 40.2, name: 'Route 43'}]->(n97),
(n98)-[:ROAD {distance: 118.4, name: 'Highway 36'}]->(n99),
(n98)-[:ROAD {distance: 130.2, name: 'Route 17'}]->(n96),
(n98)-[:ROAD {distance: 85.6, name: 'Route 15'}]->(n0),
(n99)-[:ROAD {distance: 102.5, name: 'Highway 17'}]->(n98),
(n99)-[:ROAD {distance: 12.1, name: 'Route 76'}]->(n0),
(n99)-[:ROAD {distance: 55.3, name: 'Route 34'}]->(n97),
(n99)-[:ROAD {distance: 17.3, name: 'Expressway 68'}]->(n1)
)";

// Simple graph with negative edge weights for testing exception handling
static const char* NEGATIVE_WEIGHT_GRAPH_CYPHER = R"(
CREATE (a:Node {id: '0'}),
(b:Node {id: '1'}),
(c:Node {id: '2'}),
(d:Node {id: '3'})
CREATE (a)-[:EDGE {weight: 10.0}]->(b),
(b)-[:EDGE {weight: -5.0}]->(c), 
(a)-[:EDGE {weight: -5.0}]->(d)
)";

static const char* DISJOINT_GRAPH_CYPHER = R"(
CREATE (a:Node {id: '0'}),
(b:Node {id: '1'}),
(c:Node {id: '2'}),
(d:Node {id: '3'})
CREATE (a)-[:EDGE {weight: 10.0}]->(b),
(b)-[:EDGE {weight: 5.0}]->(c), 
(a)-[:EDGE {weight: 5.0}]->(d)
)";

class ShortestPathProcessorTest : public ProcessorTester {
public:
    static void SetUpTestSuite() {
        _suiteOutDir = "ShortestPathProcessorTest_suite.out";
        if (FileUtils::exists(_suiteOutDir)) {
            FileUtils::removeDirectory(_suiteOutDir);
        }
        FileUtils::createDirectory(_suiteOutDir);

        _env = TuringTestEnv::create(fs::Path {_suiteOutDir} / "turing");
        _db = &_env->getDB();

        // Create road network graph
        {
            _graph = _env->getSystemManager().createGraph(_graphName);

            auto changeResult = _env->getSystemManager().newChange(_graphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = _db->query(ROAD_NETWORK_CYPHER, _graphName, &_env->getMem(),
                                     [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create road network: " << status.getError();

            auto submitStatus = _db->query("CHANGE SUBMIT", _graphName, &_env->getMem(),
                                           [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(submitStatus.isOk()) << "Failed to submit change: "
                                             << submitStatus.getError();
        }

        // Create negative weight graph
        {
            _negGraph = _env->getSystemManager().createGraph(_negGraphName);

            auto changeResult = _env->getSystemManager().newChange(_negGraphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = _db->query(NEGATIVE_WEIGHT_GRAPH_CYPHER, _negGraphName, &_env->getMem(),
                                     [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create negative weight graph: "
                                       << status.getError();

            auto submitStatus = _db->query("CHANGE SUBMIT", _negGraphName, &_env->getMem(),
                                           [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(submitStatus.isOk()) << "Failed to submit negative weight change: "
                                             << submitStatus.getError();
        }

        // Create disjoint graph
        {
            _disjGraph = _env->getSystemManager().createGraph(_disjGraphName);

            auto changeResult = _env->getSystemManager().newChange(_disjGraphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = _db->query(DISJOINT_GRAPH_CYPHER, _disjGraphName, &_env->getMem(),
                                     [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create disjoint graph: " << status.getError();

            auto submitStatus = _db->query("CHANGE SUBMIT", _disjGraphName, &_env->getMem(),
                                           [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(submitStatus.isOk()) << "Failed to submit disjoint change: "
                                             << submitStatus.getError();
        }
    }

    static void TearDownTestSuite() {
        _graph = nullptr;
        _negGraph = nullptr;
        _disjGraph = nullptr;
        _db = nullptr;
        _env.reset();
    }

    void initialize() override {
        // Create per-test builder and pipeline (don't call ProcessorTester::initialize
        // since we use static _env)
        _builder = std::make_unique<PipelineBuilder>(&_env->getMem(), &_pipeline);
    }

protected:
    // Static members shared across all tests
    static inline std::string _suiteOutDir;
    static inline const std::string _graphName = "roaddb";
    static inline const std::string _negGraphName = "negweightdb";
    static inline const std::string _disjGraphName = "disjdb";
    static inline std::unique_ptr<TuringTestEnv> _env;
    static inline TuringDB* _db = nullptr;
    static inline Graph* _graph = nullptr;
    static inline Graph* _negGraph = nullptr;
    static inline Graph* _disjGraph = nullptr;

    // Helper to find node by ID property using Cypher
    NodeID findNodeByID(int nodeID, const std::string& graphName, const std::string& label) {
        NodeID result = NodeID();

        std::string query = fmt::format(
            "MATCH (n:{}) WHERE n.id = '{}' RETURN n", label, nodeID);

        auto status = _db->query(query, graphName, &_env->getMem(),
                                 [&](const Dataframe* df) -> void {
                                     if (df && !df->cols().empty()) {
                                         const auto* nodeCol = df->cols()[0]->as<ColumnNodeIDs>();
                                         if (nodeCol && !nodeCol->empty()) {
                                             result = (*nodeCol)[0];
                                         }
                                     }
                                 });

        if (!status.isOk() || !result.isValid()) {
            throw std::runtime_error(fmt::format("Node not found: {}", nodeID));
        }

        return result;
    }

    NodeID findCityByID(int cityID) {
        return findNodeByID(cityID, _graphName, "City");
    }

    // Helper to run shortest path and get results
    struct ShortestPathResult {
        bool processorHasRun {false};
        double distance {0.0};
        Path path;
    };

    using ResultCallback = std::function<void(const Dataframe*, LambdaProcessor::Operation, ShortestPathResult&)>;

    ShortestPathResult runShortestPath(
        ColumnVector<NodeID> source,
        ColumnVector<NodeID> target,
        Graph* graph = nullptr,
        const std::string& propName = "distance",
        ResultCallback resultCallback = nullptr) {
        ShortestPathResult result;

        if (graph == nullptr) {
            graph = _graph;
        }

        if (resultCallback == nullptr) {
            resultCallback = [](const Dataframe* df,
                                LambdaProcessor::Operation operation,
                                ShortestPathResult& result) -> void {
                if (operation == LambdaProcessor::Operation::RESET) {
                    return;
                }

                ASSERT_EQ(df->size(), 2);

                auto* fstCol = dynamic_cast<ColumnVector<types::Double::Primitive>*>(
                    df->cols().at(0)->getColumn());
                auto* sndCol = dynamic_cast<ColumnVector<Path>*>(df->cols().at(1)->getColumn());

                ASSERT_EQ(fstCol->size(), 1);
                ASSERT_EQ(sndCol->size(), 1);

                result = {true, fstCol->at(0), sndCol->at(0)};
            };
        }

        const auto transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();
        const auto& metadata = view.metadata();

        auto distanceProp = metadata.propTypes().get(propName);
        if (!distanceProp.has_value()) {
            return result;
        }

        const auto genTargetDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
            if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
                return;
            }

            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());

            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->getRaw() = target.getRaw();

            isFinished = true;
        };

        const auto genSourceDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
            if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
                return;
            }

            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());

            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->getRaw() = source.getRaw();

            isFinished = true;
        };

        auto& targetIF = _builder->addLambdaSource(genTargetDF);
        auto* targetCol = _builder->addColumnToOutput<ColumnNodeIDs>(
            _pipeline.getDataframeManager()->allocTag());

        _builder->addLambdaSource(genSourceDF);
        auto* sourceCol = _builder->addColumnToOutput<ColumnNodeIDs>(
            _pipeline.getDataframeManager()->allocTag());

        NamedColumn* distCol = nullptr;
        NamedColumn* pathCol = nullptr;

        _builder->addShortestPath<types::Double>(&targetIF,
                                                 sourceCol->getTag(),
                                                 targetCol->getTag(),
                                                 *distanceProp,
                                                 distCol,
                                                 pathCol);

        const auto callback = [&](const Dataframe* df,
                                  LambdaProcessor::Operation operation) -> void {
            resultCallback(df, operation, result);
        };
        _builder->addLambda(callback);
        EXECUTE(view, ChunkConfig::CHUNK_SIZE);
        return result;
    }
};

// Test that the graph was loaded correctly
TEST_F(ShortestPathProcessorTest, graphLoadedCorrectly) {
    // Verify node count
    uint64_t nodeCount = 0;
    auto status = _db->query("MATCH (n:City) RETURN COUNT(n) AS cnt", _graphName, &_env->getMem(),
                             [&](const Dataframe* df) -> void {
                                 if (df && !df->cols().empty()) {
                                     const auto* col = df->cols()[0]->as<ColumnConst<uint64_t>>();
                                     if (col) {
                                         nodeCount = col->getRaw();
                                     }
                                 }
                             });

    ASSERT_TRUE(status.isOk());
    ASSERT_EQ(nodeCount, 100) << "Expected 100 cities in the graph";

    std::cout << "Graph loaded with " << nodeCount << " cities" << std::endl;
}

// Test that edges were loaded correctly
TEST_F(ShortestPathProcessorTest, edgesLoadedCorrectly) {
    // Count edges
    uint64_t edgeCount = 0;
    auto status = _db->query("MATCH ()-[r:ROAD]->() RETURN COUNT(r) AS cnt", _graphName, &_env->getMem(),
                             [&](const Dataframe* df) -> void {
                                 if (df && !df->cols().empty()) {
                                     const auto* col = df->cols()[0]->as<ColumnConst<uint64_t>>();
                                     if (col) {
                                         edgeCount = col->getRaw();
                                     }
                                 }
                             });

    ASSERT_TRUE(status.isOk());
    ASSERT_EQ(edgeCount, 400) << "Expected 400 roads in the graph";

    std::cout << "Graph loaded with " << edgeCount << " roads" << std::endl;
}

TEST_F(ShortestPathProcessorTest, directEdgePath) {
    // Test a direct edge path: city 0 to city 99
    // n0 -> n99 has distance 48.5 (Route 18)

    NodeID city0 = findCityByID(0);
    NodeID city99 = findCityByID(99);

    ColumnVector<NodeID> sourceCol;
    ColumnVector<NodeID> targetCol;
    sourceCol.push_back(city0);
    targetCol.push_back(city99);

    auto result = runShortestPath(sourceCol, targetCol);

    ASSERT_TRUE(result.processorHasRun);
    EXPECT_NEAR(result.distance, 48.5, 0.1) << "Expected direct path via Route 18";

    std::cout << "Distance from Springfield (0) to Cleveland (99): " << result.distance << std::endl;
}

TEST_F(ShortestPathProcessorTest, sameSourceAndTarget) {
    // Test shortest path from a city to itself (should be distance 0)

    NodeID city0 = findCityByID(0);

    ColumnVector<NodeID> sourceCol;
    ColumnVector<NodeID> targetCol;
    sourceCol.push_back(city0);
    targetCol.push_back(city0);

    auto result = runShortestPath(sourceCol, targetCol);

    ASSERT_TRUE(result.processorHasRun);
    ASSERT_DOUBLE_EQ(result.distance, 0.0);

    std::cout << "Distance from Springfield (0) to itself: " << result.distance << std::endl;
}


TEST_F(ShortestPathProcessorTest, uniquePathVerification) {
    // Test a path that has exactly ONE shortest path (verified via NetworkX)
    // This allows us to verify both distance AND path nodes
    // Path from Troy (28) to Jackson (78)
    // NetworkX verified: distance 646.0, unique path with 14 nodes:
    // [28, 29, 58, 60, 62, 63, 65, 66, 84, 83, 81, 52, 80, 78]
    // Note: our path is returned as target->source, so reversed

    NodeID city28 = findCityByID(28);
    NodeID city78 = findCityByID(78);

    ColumnVector<NodeID> sourceCol;
    ColumnVector<NodeID> targetCol;
    sourceCol.push_back(city28);
    targetCol.push_back(city78);

    auto result = runShortestPath(sourceCol, targetCol);

    ASSERT_TRUE(result.processorHasRun);
    EXPECT_NEAR(result.distance, 646.0, 0.1) << "NetworkX verified distance";

    // Since this is the unique shortest path, we can verify the exact path length
    EXPECT_EQ((result.path.size() + 1) / 2, 14) << "NetworkX verified: exactly 14 nodes in unique path";

    // Verify path endpoints (path is target->source order)
    // First element should be target (78), last should be source (28)
    // Path is [node, edge, node, edge, ...] so front and back are nodes
    ASSERT_GE(result.path.size(), 2);
    EXPECT_EQ(result.path.front().getValue(), city78.getValue()) << "Path should start at target";
    EXPECT_EQ(result.path.back().getValue(), city28.getValue()) << "Path should end at source";

    std::cout << "Distance from Troy (28) to Jackson (78): " << result.distance << std::endl;
    std::cout << "Path contains " << (result.path.size() + 1) / 2 << " nodes (unique path)" << std::endl;
}

TEST_F(ShortestPathProcessorTest, noPath) {
    // Test that the processor returns nothing when there is no path between the
    // source and target set
    // Graph has no path from node 1 to node 3
    //
    auto resultCallback = [](const Dataframe* df,
                             LambdaProcessor::Operation operation,
                             ShortestPathResult& result) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        ASSERT_EQ(df->size(), 2);

        auto* fstCol = dynamic_cast<ColumnVector<types::Double::Primitive>*>(
            df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnVector<Path>*>(df->cols().at(1)->getColumn());

        ASSERT_EQ(fstCol->size(), 0);
        ASSERT_EQ(sndCol->size(), 0);
    };

    NodeID node1 = findNodeByID(1, _disjGraphName, "Node");
    NodeID node3 = findNodeByID(3, _disjGraphName, "Node");

    ColumnVector<NodeID> sourceCol;
    ColumnVector<NodeID> targetCol;
    sourceCol.push_back(node1);
    targetCol.push_back(node3);

    auto result = runShortestPath(sourceCol, targetCol, _disjGraph, "weight", resultCallback);

    ASSERT_FALSE(result.processorHasRun);
}

TEST_F(ShortestPathProcessorTest, throwsOnNegativeWeight) {
    // Test that shortest path throws an exception when encountering negative weights
    // Graph: (0) --10.0--> (1) --(-5.0)--> (2)

    NodeID node0 = findNodeByID(0, _negGraphName, "Node");
    NodeID node2 = findNodeByID(2, _negGraphName, "Node");

    ColumnVector<NodeID> sourceCol;
    ColumnVector<NodeID> targetCol;
    sourceCol.push_back(node0);
    targetCol.push_back(node2);

    auto noopCallback = [](const Dataframe*, LambdaProcessor::Operation, ShortestPathResult&) {};

    EXPECT_THROW(runShortestPath(sourceCol, targetCol, _negGraph, "weight", noopCallback),
                 PipelineException);
}
