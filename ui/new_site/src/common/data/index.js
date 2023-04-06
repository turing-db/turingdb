import { topPages, allData, monthData, halfyearData, allaudiencesMetricsData, monthaudiencesMetricsData, halfyearaudiencesMetricsData, yaeraudiencesMetricsData, todayDeviceData, lastWeekDeviceData, lastMonthDeviceData, currentYearDeviceData, todayaudiencesCountryData, lastWeekaudiencesCountryData, lastMonthaudiencesCountryData, currentyearaudiencesCountryData } from "./analytics";
import {
  crmWidgets,
  dealsStatus,
  tasks,
  activities,
  closingDeals,
  todayBalanceData, lastWeekBalanceData, lastMonthBalanceData, currentYearBalanceData, todayDealData, weeklyDealData, monthlyDealData, yealyDealData, octData, novData, decData, janData
} from "./dashboardcrm";
import {
  ecomWidgets,
  bestSellingProducts,
  topSellers,
  recentOrders,
  topCategories,
  allRevenueData, monthRevenueData, halfYearRevenueData, yearRevenueData
} from "./dashboardEcommerce";
import {
  cyptoWidgets,
  currencies,
  recentActivity,
  topPerformers,
  newsFeed,
  cryptoSlider,
  btcPortfolioData, usdPortfolioData, euroPortfolioData, MarketGraphAll, MarketGraphYear, MarketGraphMonth, MarketGraphWeek, MarketGraphHour
} from "./crypto";
import {
  projectsWidgets,
  activeProjects,
  projectTasks,
  teamMembers,
  overviewCounter, allProjectData, monthProjectData, halfyearProjectData, yearProjectData, allTimeData, lastWeekData, lastMonthData, lastquarterData, dashboardChat
} from "./dashboardProjects";
import { topartWork, featuredNFTData, popularityData, recentNFTsData, topCollectionData, popularCreatorsData, allMarketplaceData, monthMarketplaceData, halfyearMarketplaceData, yearMarketplaceData } from "./dashboardNFT";
import {
  tileBoxs1,
  tileBoxs2,
  tileBoxs3,
  tileBoxes4,
  tileBoxes5,
  widgetsActivities,
  widgetsAudiences,
  widgetsPortfolio,
  widgetsTasks,
  otherWidgets2
} from "./widgets";
import { taskWidgets, allTask, kanbanBoardData } from "./taskList";
import { invoiceWidgets, invoiceTable } from "./invoiceList";
import {
  projectList,
} from "./projectList";

import { calenderDefaultCategories, events, defaultevent } from "./calender";
import { direactContact, channelsList, messages, chatContactData } from "./chat";
import { mailbox } from "./mailbox";
import { transactions, buysellWidgets, market, CryptoOrders, watchlist, marketStatus, CryptoicoWidgets, icoWidgetsList } from "./cryptoPage";
import { ticketsWidgets, ticketsTable } from "./supportTickets";

// Pages
import { gallery, pricing1, pricing2, pricing3, projects, document, SearchGallery, news, video, swiper, team } from "./pagesData";

//Ecommerce
import {
  productsData,
  productDetailsWidgets,
  reviews,
  orders,
  productDetails,
  customerList,
  shoppingCart,
  orderSummary,
  sellersList,
  revenueWidgets,
  productsReview,
} from "./ecommerce";

import { crmcontacts, companies, leads, deals } from "./crm";

import {
  expolreNow, aution, NFTRanking, creatorsData, creatorsListData, walletConnectData, topDrop, topCreator, topCollection, tradingArtworkData,
  nftArtworkData,
  popularCreatorsNFT, marketPlacewidget
} from "./NFTMarketplace";

import { connectData, discoverItemsData, featuresData, productData, topCreatorData } from "./LandingNFT";

import { recentFile, folderList } from "./fileManager";

import { todoTaskList, todoCollapse } from "./todoData";

import { jobApplication } from "./appsJobs";
import { apiKey } from "./apiKey";

export {
  topPages,
  crmWidgets,
  cryptoSlider,
  dealsStatus,
  tasks,
  activities,
  closingDeals,
  ecomWidgets,
  bestSellingProducts,
  topSellers,
  recentOrders,
  topCategories,
  cyptoWidgets,
  currencies,
  topPerformers,
  recentActivity,
  newsFeed,
  projectsWidgets,
  activeProjects,
  projectTasks,
  teamMembers,
  overviewCounter,
  tileBoxs1,
  tileBoxs2,
  tileBoxs3,
  tileBoxes4,
  tileBoxes5,
  widgetsActivities,
  widgetsAudiences,
  widgetsPortfolio,
  widgetsTasks,
  taskWidgets,
  allTask,
  kanbanBoardData,
  invoiceWidgets,
  invoiceTable,
  projectList,
  direactContact,
  channelsList,
  messages,
  calenderDefaultCategories,
  events,
  defaultevent,
  mailbox,
  productsData,
  productDetailsWidgets,
  reviews,
  orders,
  productDetails,
  customerList,
  shoppingCart,
  orderSummary,
  sellersList,
  revenueWidgets,
  productsReview,
  crmcontacts,
  companies,
  leads,
  deals,
  transactions,
  buysellWidgets,
  market,
  CryptoOrders,
  watchlist,
  marketStatus,
  CryptoicoWidgets,
  icoWidgetsList,
  ticketsWidgets,
  ticketsTable,
  otherWidgets2,
  expolreNow,
  aution,
  NFTRanking,
  creatorsData,
  creatorsListData,
  walletConnectData,
  topDrop,
  topCreator,
  topCollection,
  topartWork, featuredNFTData, popularityData, recentNFTsData, topCollectionData, popularCreatorsData, tradingArtworkData,
  nftArtworkData,
  popularCreatorsNFT, marketPlacewidget,
  connectData, discoverItemsData, featuresData, productData, topCreatorData, allData, monthData, halfyearData, allaudiencesMetricsData, monthaudiencesMetricsData, halfyearaudiencesMetricsData, yaeraudiencesMetricsData, todayDeviceData, lastWeekDeviceData, lastMonthDeviceData, currentYearDeviceData, todayBalanceData, lastWeekBalanceData, lastMonthBalanceData, currentYearBalanceData, todayDealData, weeklyDealData, monthlyDealData, yealyDealData, octData, novData, decData, janData, allRevenueData, monthRevenueData, halfYearRevenueData, yearRevenueData, btcPortfolioData, usdPortfolioData, euroPortfolioData, MarketGraphAll, MarketGraphYear, MarketGraphMonth, MarketGraphWeek, MarketGraphHour, allProjectData, monthProjectData, halfyearProjectData, yearProjectData, allTimeData, lastWeekData, lastMonthData, lastquarterData, allMarketplaceData, monthMarketplaceData, halfyearMarketplaceData, yearMarketplaceData, todayaudiencesCountryData, lastWeekaudiencesCountryData, lastMonthaudiencesCountryData, currentyearaudiencesCountryData, dashboardChat, gallery, pricing1, pricing2, pricing3, projects, document, SearchGallery, news, video, swiper, team,
  recentFile, folderList, todoTaskList, chatContactData, todoCollapse,
  jobApplication,apiKey
};
