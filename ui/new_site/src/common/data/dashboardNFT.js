// Import Images
import Img1 from "../../assets/images/nft/img-01.jpg";
import Img2 from "../../assets/images/nft/img-02.jpg";
import Img3 from "../../assets/images/nft/img-03.jpg";
import Img4 from "../../assets/images/nft/img-04.jpg";
import Img5 from "../../assets/images/nft/img-05.jpg";
import Img6 from "../../assets/images/nft/img-06.jpg";

// import ImgGif5 from "../../assets/images/nft/gif/img-5.gif";
// import ImgGif1 from "../../assets/images/nft/gif/img-1.gif";
// import ImgGif3 from "../../assets/images/nft/gif/img-3.gif";
// import ImgGif4 from "../../assets/images/nft/gif/img-4.gif";

import avatar8 from "../../assets/images/users/avatar-8.jpg";
import avatar10 from "../../assets/images/users/avatar-10.jpg";
import avatar1 from "../../assets/images/users/avatar-1.jpg";
import btcCoin from "../../assets/images/svg/crypto-icons/btc.svg";
import ltcCoin from "../../assets/images/svg/crypto-icons/ltc.svg";
import etcCoin from "../../assets/images/svg/crypto-icons/etc.svg";
import dashCoin from "../../assets/images/svg/crypto-icons/dash.svg";
import bnbCoin from "../../assets/images/svg/crypto-icons/bnb.svg";
import usdtCoin from "../../assets/images/svg/crypto-icons/usdt.svg";
const topartWork = [
    {
        id: 1,
        img: Img1,
        title: "One shop destination on",
        sales: "13,450",
        collection: "235,000",
        chartsColor: "#f06548",
        series: [{ data: [25, 66, 41, 89, 63, 25, 44, 12] }],
    },
    {
        id: 2,
        img: Img2,
        title: "Coin Journal is dedicated",
        sales: "11,752",
        collection: "632,000",
        chartsColor: "#f06548",
        series: [{ data: [50, 15, 35, 62, 23, 56, 44, 12] }],
    },
    {
        id: 3,
        img: Img3,
        title: "The Bitcoin-holding U.S.",
        sales: "7,526",
        collection: "468,000",
        chartsColor: "#f06548",
        series: [{ data: [25, 35, 35, 89, 63, 25, 44, 12] }],
    },
    {
        id: 4,
        img: Img4,
        title: "Cryptocurrency Price Bitcoin",
        sales: "15,521",
        collection: "265,000",
        chartsColor: "#0ab39c",
        series: [{ data: [50, 15, 20, 34, 23, 56, 65, 41] }],
    },
    {
        id: 5,
        img: Img5,
        title: "Dash, Ripple and Litecoin",
        sales: "12,652",
        collection: "456,000",
        chartsColor: "#0ab39c",
        series: [{ data: [45, 53, 24, 89, 63, 60, 36, 50] }],
    },
    {
        id: 6,
        img: Img6,
        title: "The Cat X Takashi",
        sales: "11,745",
        collection: "256,000",
        chartsColor: "#f06548",
        series: [{ data: [50, 15, 35, 62, 23, 56, 44, 12] }],
    },
    {
        id: 7,
        img: Img1,
        title: "Long-tailed Macaque",
        sales: "41,032",
        collection: "745,000",
        chartsColor: "#0ab39c",
        series: [{ data: [50, 15, 20, 34, 23, 56, 65, 41] }],
    },
    {
        id: 8,
        img:"https://img.themesbrand.com/velzon/images/img-5.gif",
        title: "Evolved Reality",
        sales: "513,794",
        collection: "870,000",
        chartsColor: "#f06548",
        series: [{ data: [45, 53, 24, 89, 63, 60, 36, 50] }],
    },
];
const featuredNFTData = [
    {
        id: 1,
        img: "https://img.themesbrand.com/velzon/images/img-3.gif",
        likes: "37.41k",
        title: "Walking On Air",
        category: "Artwork",
        highest: "10.35ETH",
        price: "14.167 ETH",
    },
    {
        id: 2,
        img: Img3,
        likes: "19.29k",
        title: "Filtered Portrait",
        category: "Photography",
        highest: "75.3ETH",
        price: "67.36 ETH",
    },
    {
        id: 3,
        img: "https://img.themesbrand.com/velzon/images/img-1.gif",
        likes: "8.42k",
        title: "Patterns Arts & Culture",
        category: "Artwork",
        highest: "9.64ETH",
        price: "14.167 ETH",
    },
    {
        id: 4,
        img: "https://img.themesbrand.com/velzon/images/img-4.gif",
        likes: "15.93k",
        title: "Evolved Reality",
        category: "Video",
        highest: "2.75ETH",
        price: "3.167 ETH",
    },
];
const popularityData = [
    {
        id: 1,
        img: avatar10,
        author: "Herbert Stokes",
        username: "@herbert10",
        price: "174.36 ETH",           
    },
    {
        id: 2,
        img: Img1,
        author: "Nancy Martino",
        username: "@nancyMt",
        price: "346.47 ETH",
    },
    {
        id: 3,
        img: Img4,
        author: "Timothy Smith",
        username: "@timothy",
        price: "349.08 ETH",
    },
    {
        id: 4,
        img: Img6,
        author: "Glen Matney",
        username: "@matney10",
        price: "852.34 ETH",
    },
    {
        id: 5,
        img: avatar8,
        author: "Michael Morris",
        username: "@michael",
        price: "4.071 ETH",
    },
    {
        id: 6,
        img: Img3,
        author: "Alexis Clarke",
        username: "@alexis_30",
        price: "30.749 ETH",
    },
    {
        id: 7,
        img: Img5,
        author: "Timothy Smith",
        username: "@timothy",
        price: "349.08 ETH",
    },        
];
const recentNFTsData = [
    {
        id: 1, 
        img: Img1,
        title: "Abstract Face Painting",
        category: "Artworks",
        icon: btcCoin,
        volume: "48,568.025",           
        hours: 5.26,
        creators: "6.8K",
        items: "18.0K",
    },
    {
        id: 2, 
        img: "https://img.themesbrand.com/velzon/images/img-5.gif",
        title: "Long-tailed Macaque",
        category: "Games",
        icon: ltcCoin,
        volume: "87,142.027",           
        hours: 3.07,
        creators: "2.6K",
        items: "6.3K",
        textColor: true,
    },
    {
        id: 3, 
        img: Img6,
        title: "Robotic Body Art",
        category: "Photography",
        icon: etcCoin,
        volume: "33,847.961",           
        hours: 7.13,
        creators: "7.5K",
        items: "14.6K",
    },
    {
        id: 4, 
        img: Img4,
        title: "Smillevers Crypto",
        category: "Artworks",
        icon: dashCoin,
        volume: "73,654.421",           
        hours: 0.97,
        creators: "5.3K",
        items: "36.4K",
    },
    {
        id: 5, 
        img: Img3,
        title: "Creative Filtered Portrait",
        category: "3d Style",
        icon: bnbCoin,
        volume: "66,742.077",           
        hours: 1.08,
        creators: "3.1K",
        items: "12.4K",
        textColor: true,
    },
    {
        id: 6, 
        img: Img2,
        title: "The Chirstoper",
        category: "Crypto Card",
        icon: usdtCoin,
        volume: "34,736.209",           
        hours: 4.52,
        creators: "7.2K",
        items: "25.0K",
    },
];
const topCollectionData = [
    {
        id: 1,
        img: Img3,
        category: "Artworks",
        items: "4700+",
    },
    {
        id: 2,
        img: Img4,
        category: "Crypto Card",
        items: "743+",
    },
    {
        id: 3,
        img:"https://img.themesbrand.com/velzon/images/img-5.gif",
        category: "3d Style",
        items: "4781+",
    },
    {
        id: 4,
        img: Img6,
        category: "Collectibles",
        items: "3468+",
    },
];
const popularCreatorsData = [
    {
        id: 1,
        img: Img2,
        author: "Alexis Clarke",
        price: "81,369 ETH",
    },
    {
        id: 2,
        img: Img1,
        author: "Timothy Smith",
        price: "4,754 ETH",
    },
    {
        id: 3,
        img: Img4,
        author: "Herbert Stokes",
        price: "68,945 ETH",
    },
    {
        id: 4,
        img: avatar1,
        author: "Glen Matney",
        price: "49,031 ETH",
    },
];

const allMarketplaceData = [{
    name: "Artwork",
    data: [10, 41, 35, 51, 49, 62, 69, 91, 148]
},
{
    name: "Auction",
    data: [40, 120, 83, 45, 31, 74, 35, 34, 78]
},
{
    name: "Creators",
    data: [95, 35, 20, 130, 64, 22, 43, 45, 31]
}];

const monthMarketplaceData = [{
    name: "Artwork",
    data: [10, 21, 25, 31, 29, 42, 59, 51, 48]
},
{
    name: "Auction",
    data: [20, 20, 63, 25, 21, 54, 15, 14, 48]
},
{
    name: "Creators",
    data: [55, 55, 30, 150, 84, 52, 53, 65, 51]
}];

const halfyearMarketplaceData = [{
    name: "Artwork",
    data: [30, 51, 45, 71, 89, 82, 79, 81, 158]
},
{
    name: "Auction",
    data: [90, 140, 123, 75, 61, 94, 65, 64, 98]
},
{
    name: "Creators",
    data: [125, 95, 60, 150, 74, 32, 63, 65, 71]
}];

const yearMarketplaceData = [{
    name: "Artwork",
    data: [20, 61, 65, 71, 69, 82, 89, 121, 168]
},
{
    name: "Auction",
    data: [60, 140, 103, 65, 51, 84, 75, 94, 98]
},
{
    name: "Creators",
    data: [145, 75, 60, 150, 74, 52, 63, 65, 61]
}];

export { topartWork, featuredNFTData, popularityData, recentNFTsData, topCollectionData, popularCreatorsData, allMarketplaceData, monthMarketplaceData, halfyearMarketplaceData, yearMarketplaceData };