// Import Images
import Img1 from "../../assets/images/nft/img-01.jpg";
import Img2 from "../../assets/images/nft/img-02.jpg";
import Img3 from "../../assets/images/nft/img-03.jpg";
import Img4 from "../../assets/images/nft/img-04.jpg";
import Img5 from "../../assets/images/nft/img-05.jpg";
import Img6 from "../../assets/images/nft/img-06.jpg";

import ImgGif1 from "../../assets/images/nft/gif/img-1.gif";
import ImgGif2 from "../../assets/images/nft/gif/img-2.gif";
import ImgGif3 from "../../assets/images/nft/gif/img-3.gif";
import ImgGif4 from "../../assets/images/nft/gif/img-4.gif";
import ImgGif5 from "../../assets/images/nft/gif/img-5.gif";

import avatar1 from "../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../assets/images/users/avatar-3.jpg";
import avatar5 from "../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../assets/images/users/avatar-6.jpg";
import avatar8 from "../../assets/images/users/avatar-8.jpg";
import avatar9 from "../../assets/images/users/avatar-9.jpg";
import avatar10 from "../../assets/images/users/avatar-10.jpg";

import metaMask from "../../assets/images/nft/wallet/metamask.png";
import coinbase from "../../assets/images/nft/wallet/coinbase.png";
import kukai from "../../assets/images/nft/wallet/kukai.png";
import binance from "../../assets/images/nft/wallet/binance.png";
import enjin from "../../assets/images/nft/wallet/enjin.png";
import alpha from "../../assets/images/nft/wallet/alpha.png";
import math from "../../assets/images/nft/wallet/math.png";

const expolreNow = [
    {
        id: 1,
        img: ImgGif3,
        likes: "37.41",
        title: "Walking On Air",
        category: "Artwork",
        highestBid: "10.35",
        price: "14.617",
        isActive: false,
        fileType: "gif",
        sales: "On Auction"
    },
    {
        id: 2,
        img: Img3,
        likes: "19.29",
        title: "Creative Filtered Portrait",
        category: "Photography",
        highestBid: "75.3",
        price: "67.36",
        isActive: true,
        fileType: "jpg",
        sales: "Has Offers"
    },
    {
        id: 3,
        img: Img2,
        likes: "23.63",
        title: "The Chirstoper",
        category: "Music",
        highestBid: "412.30",
        price: "394.7",
        isActive: true,
        fileType: "jpg",
        sales: "On Auction"
    },
    {
        id: 4,
        img: ImgGif4,
        likes: "15.93",
        title: "Evolved Reality",
        category: "Video",
        highestBid: "2.75",
        price: "3.167",
        isActive: false,
        fileType: "gif",
        sales: "Has Offers"
    },
    {
        id: 5,
        img: Img1,
        likes: "14.85",
        title: "Abstract Face Painting",
        category: "Collectibles",
        highestBid: "122.34",
        price: "97.8",
        isActive: true,
        fileType: "jpg",
        sales: "On Auction"
    },
    {
        id: 6,
        img: Img5,
        likes: "64.10",
        title: "Long-tailed Macaque",
        category: "Artwork",
        highestBid: "874.01",
        price: "745.14",
        isActive: false,
        fileType: "jpg",
        sales: "Has Offers"
    },
    {
        id: 7,
        img: Img6,
        likes: "36.42",
        title: "Robotic Body Art",
        category: "Artwork",
        highestBid: "41.65",
        price: "34.81",
        isActive: true,
        fileType: "jpg",
        sales: "On Auction"
    },
    {
        id: 8,
        img: ImgGif2,
        likes: "94.1",
        title: "Trendy Fashion Portraits",
        category: "3d Style",
        highestBid: "674.92",
        price: "563.81",
        isActive: true,
        fileType: "gif",
        sales: "On Auction"
    },
    {
        id: 9,
        img: Img4,
        likes: "34.12",
        title: "Smillevers Crypto",
        category: "Crypto Card",
        highestBid: "41.658",
        price: "34.81",
        isActive: true,
        fileType: "jpg",
        sales: "Has Offers"
    },
    {
        id: 10,
        img: ImgGif1,
        likes: "8.42",
        title: "Patterns Arts & Culture",
        category: "Artwork",
        highestBid: "9.64",
        price: "14.167",
        isActive: false,
        fileType: "gif",
        sales: "Has Offers"
    },
    {
        id: 11,
        img: Img3,
        likes: "19.29",
        title: "Creative Filtered Portrait",
        category: "Video",
        highestBid: "75.3",
        price: "267.36",
        isActive: true,
        fileType: "mp4",
        sales: "Has Offers"
    },
    {
        id: 12,
        img: ImgGif4,
        likes: "15.93",
        title: "Evolved Reality",
        category: "Artwork",
        highestBid: "2.75",
        price: "324.26",
        isActive: true,
        fileType: "gif",
        sales: "On Auction"
    },
    {
        id: 13,
        img: Img1,
        likes: "14.85",
        title: "Abstract Face Painting",
        category: "Photography",
        highestBid: "122.34",
        price: "245.8",
        isActive: false,
        fileType: "jpg",
        sales: "On Auction"
    },
    {
        id: 14,
        img: Img5,
        likes: "64.10",
        title: "Long-tailed Macaque",
        category: "Collectibles",
        highestBid: "874.01",
        price: "340.14",
        isActive: true,
        fileType: "mp4",
        sales: "On Auction"
    },
    {
        id: 15,
        img: Img6,
        likes: "36.42",
        title: "Robotic Body Art",
        category: "Photography",
        highestBid: "121.81",
        price: "41.658",
        isActive: false,
        fileType: "mp3",
        sales: "Has Offers"
    },
];
const aution = [
    {
        id: 1,
        img: Img1,
        date: "2024/3/19",
        likes: "19.29",
        price: "97.8",
        title: "Abstract Face Painting",
        available: "436",
        sold: "4187",
        progressClass: "warning",
        size: "67%",
        category: "upto-15",
    },
    {
        id: 2,
        img: ImgGif1,
        date: "2023/4/16",
        likes: "8.42",
        price: "245.23",
        title: "Patterns Arts & Culture",
        available: "8974",
        sold: "13",
        progressClass: "primary",
        size: "10%",
        category: "upto-30",
    },
    {
        id: 3,
        img: Img3,
        date: "2023/12/1",
        likes: "19.29",
        price: "67.36",
        title: "Creative Filtered Portrait",
        available: "3620",
        sold: "345",
        progressClass: "warning",
        size: "45%",
        category: "upto-40",
    },
    {
        id: 4,
        img: Img4,
        date: "2024/11/26",
        likes: "34.12",
        price: "34.81",
        title: "Smillevers Crypto",
        available: "3521",
        sold: "1457",
        progressClass: "warning",
        size: "67%",
        category: "upto-15",
    },
    {
        id: 5,
        img: ImgGif5,
        date: "2023/5/27",
        likes: "47.12",
        price: "245.23",
        title: "Long-tailed Macaque",
        available: "30",
        sold: "1369",
        progressClass: "danger",
        size: "95%",
        category: "upto-40",
    },
    {
        id: 6,
        img: Img2,
        date: "2023/5/20",
        likes: "23.63",
        price: "394.7",
        title: "The Chirstoper",
        available: "1474",
        sold: "7451",
        progressClass: "warning",
        size: "83%",
        category: "upto-15",
    },
    {
        id: 7,
        img: Img6,
        date: "2023/6/10",
        likes: "36.42",
        price: "745.14",
        title: "Robotic Body Art",
        available: "4563",
        sold: "1024",
        progressClass: "primary",
        size: "24%",
        category: "upto-30",
    },
    {
        id: 8,
        img: ImgGif4,
        date: "2023/6/25",
        likes: "94.1",
        price: "245.23",
        title: "Evolved Reality",
        available: "26",
        sold: "9974",
        progressClass: "danger",
        size: "97%",
        category: "upto-15",
    },
];

const NFTRanking = [
    {
        ranking: 11,
        img: Img4,
        collection: "Highstreet IHO Part",
        volume_price: "35,750",
        hours: 23.1,
        day: -64.36,
        item: "12.3k",
        floor_price: "367.39",
    },
    {
        ranking: 10,
        img: Img3,
        collection: "Walking On Air",
        volume_price: "3,35,750",
        hours: 238.13,
        day: -64.3,
        item: "8.3k",
        floor_price: "143.39",
    },
    {
        ranking: 9,
        img: Img5,
        collection: "The Chirstoper",
        volume_price: "74,950",
        hours: -31.49,
        day: -26.07,
        item: "7.9k",
        floor_price: "101.12",
    },
    {
        ranking: 8,
        img: "https://img.themesbrand.com/velzon/images/img-3.gif",
        collection: "Walking On Air",
        volume_price: "4,62,000",
        hours: 238.13,
        day: 183.4,
        item: "8.3k",
        floor_price: "143.39",
    },
    {
        ranking: 7,
        img: "https://img.themesbrand.com/velzon/images/img-5.gif",
        collection: "The Chirstoper",
        volume_price: "1,87,600",
        hours: -31.49,
        day: 26.07,
        item: "18k",
        floor_price: "101.12",
    },
    {
        ranking: 6,
        img: Img6,
        collection: "Smillevers Crypto",
        volume_price: "37,100",
        hours: 1.42,
        day: 0.64,
        item: "31k",
        floor_price: "1.87",
    },
    {
        ranking: 5,
        img: Img4,
        collection: "Robotic Body Art",
        volume_price: "25,800",
        hours: -347.42,
        day: -321.17,
        item: "17k",
        floor_price: "343.75",
    },
    {
        ranking: 4,
        img: "https://img.themesbrand.com/velzon/images/img-2.gif",
        collection: "Long-tailed Macaque",
        volume_price: "3,63,000",
        hours: 709.13,
        day: -645.1,
        item: "21k",
        floor_price: "137.09",
    },
    {
        ranking: 3,
        img: Img2,
        collection: "Creative Filtered Portrait",
        volume_price: "1,36,000",
        hours: 170.31,
        day: -415.21,
        item: "15k",
        floor_price: "67.16",
    },
    {
        ranking: 2,
        img: "https://img.themesbrand.com/velzon/images/img-1.gif",
        collection: "Patterns Arts & Culture",
        volume_price: "32,850",
        hours: -42.03,
        day: 20.14,
        item: "8k",
        floor_price: "74.69",
    },
    {
        ranking: 1,
        img: Img1,
        collection: "Abstract Face Painting",
        volume_price: "7,50,000",
        hours: 342.35,
        day: 312.35,
        item: "10k",
        floor_price: "33.02",
    },
];

const creatorsData = [
    {
        id: 1,
        img: Img1,
        title: "Timothy Smith",
        price: "4,754 ETH",
    },
    {
        id: 2,
        img: avatar5,
        title: "Alexis Clarke",
        price: "81,369 ETH",
    },
    {
        id: 3,
        img: Img6,
        title: "Glen Matney",
        price: "13,156 ETH",
    },
    {
        id: 4,
        img: "https://img.themesbrand.com/velzon/images/img-5.gif",
        title: "Herbert Stokes",
        price: "34,754 ETH",
    },
    {
        id: 5,
        img: avatar8,
        title: "Michael Morris",
        price: "13,841 ETH",
    },
    {
        id: 6,
        img: Img4,
        title: "Nancy Martino",
        price: "26,834 ETH",
    },
    {
        id: 7,
        img: Img5,
        title: "Mary Cousar",
        price: "18,034 ETH",
    },
    {
        id: 8,
        img: Img2,
        title: "James Morris",
        price: "63,710 ETH",
    },
];
const creatorsListData = [
    {
        id: 1,
        cardImg: Img3,
        img: avatar2,
        title: "Michael Morris",
        products: "9784",
        isFollowBtn: false,
    },
    {
        id: 2,
        cardImg: "https://img.themesbrand.com/velzon/images/img-2.gif",
        img: avatar6,
        title: "Charles Kubik",
        products: "4678",
        isFollowBtn: true,
    },
    {
        id: 3,
        cardImg: Img5,
        img: Img6,
        title: "Alexis Clarke",
        products: "861",
        isFollowBtn: false,
    },
    {
        id: 4,
        cardImg: Img6,
        img: "https://img.themesbrand.com/velzon/images/img-1.gif",
        title: "James Morris",
        products: "30174",
        isFollowBtn: false,
    },
    {
        id: 5,
        cardImg: "https://img.themesbrand.com/velzon/images/img-3.gif",
        img: avatar8,
        title: "Herbert Stokes",
        products: "6487",
        isFollowBtn: true,
    },
    {
        id: 6,
        cardImg: Img3,
        img: avatar2,
        title: "Michael Morris",
        products: "10137",
        isFollowBtn: false,
    },
    {
        id: 7,
        cardImg: "https://img.themesbrand.com/velzon/images/img-2.gif",
        img: avatar6,
        title: "Tonya Noble",
        products: "364",
        isFollowBtn: true,
    },
    {
        id: 8,
        cardImg: Img5,
        img: Img6,
        title: "Nancy Martino",
        products: "9513",
        isFollowBtn: false,
    },
    {
        id: 9,
        cardImg: Img1,
        img: "https://img.themesbrand.com/velzon/images/img-1.gif",
        title: "Kevin Dawson",
        products: "6374",
        isFollowBtn: true,
    },
    {
        id: 10,
        cardImg: "https://img.themesbrand.com/velzon/images/img-4.gif",
        img: avatar10,
        title: "Glen Matney",
        products: "7809",
        isFollowBtn: false,
    },
];

const walletConnectData = [
    {
        id: 1,
        img: metaMask,
        title: "Metamask",
        textContent:
            "MetaMask is a software cryptocurrency wallet used to interact with the Ethereum blockchain.",
    },
    {
        id: 2,
        img: coinbase,
        title: "Coinbase Wallet",
        textContent:
            "Coinbase Wallet is a software product that gives you access to a wide spectrum.",
    },
    {
        id: 3,
        img: kukai,
        title: "Kukai Wallet",
        textContent:
            "Kukai is a seamless browser-based wallet that allows users to store, transfer, and delegate.",
    },
    {
        id: 4,
        img: binance,
        title: "Binance",
        textContent:
            "Binance offers a relatively secure, versatile way to invest in and trade cryptocurrencies.",
    },
    {
        id: 5,
        img: enjin,
        title: "Enjin Wallet",
        textContent:
            "Enjin is a store of value that can be used in the non-fungible token (NFT) marketplace.",
    },
    {
        id: 6,
        img: alpha,
        title: "Alpha Wallet",
        textContent:
            "AlphaWallet uses the TokenScript framework, which makes tokens become “smart”.",
    },
    {
        id: 7,
        img: math,
        title: "Math Wallet",
        textContent:
            "Math DApp Factory gives users tools that can simplify the development of exchanges, games",
    },
];

const topDrop = [
    {
        id: 1,
        img: Img3,
        title: "Creative filtered portrait",
        price: "34.81",
        time: "Just Now",
    },
    {
        id: 2,
        img: ImgGif4,
        title: "Patterns arts & culture",
        price: "147.83",
        time: "3 sec ago",
    },
    {
        id: 3,
        img: ImgGif3,
        title: "Evolved Reality",
        price: "34.81",
        time: "2 min ago",
    },
    {
        id: 4,
        img: Img4,
        title: "Smillevers Crypto",
        price: "47.9",
        time: "26 min ago",
    },
    {
        id: 5,
        img: Img5,
        title: "Robotic Body Art",
        price: "134.32",
        time: "1 hrs ago",
    },
    {
        id: 6,
        img: Img2,
        title: "Trendy fashion portraits",
        price: "643.19",
        time: "3 hrs ago",
    },
];

const topCreator = [
    {
        id: 1,
        img: avatar1,
        name: "Herbert Stokes",
        products: "23",
        isFollow: true,
    },
    {
        id: 2,
        img: avatar3,
        name: "Thomas Taylor",
        products: "123",
        isFollow: false,
    },
    { id: 3, img: avatar5, name: "Henry Baird", products: "46", isFollow: true },
    {
        id: 4,
        img: avatar10,
        name: "Nancy Martino",
        products: "845",
        isFollow: true,
    },
    {
        id: 5,
        img: avatar8,
        name: "James Price",
        products: "318",
        isFollow: false,
    },
];

const topCollection = [
    {
        id: 1,
        category: "Artwork",
        quantity: "206",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img6 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-2.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-5.gif", imgClass: "mb-1" },
                    { id: 2, img: Img3 },
                ],
            },
        ],
    },
    {
        id: 2,
        category: "Crypto Card",
        quantity: "743",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img5 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-1.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-4.gif", imgClass: "mb-1" },
                    { id: 2, img: Img4 },
                ],
            },
        ],
    },
    {
        id: 3,
        category: "Music",
        quantity: "679",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img2 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-3.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-1.gif", imgClass: "mb-1" },
                    { id: 2, img: Img1 },
                ],
            },
        ],
    },
    {
        id: 4,
        category: "Games",
        quantity: "341",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img3 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-5.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-2.gif", imgClass: "mb-1" },
                    { id: 2, img: Img5 },
                ],
            },
        ],
    },
    {
        id: 5,
        category: "Photography",
        quantity: "1452",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img2 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-3.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-1.gif", imgClass: "mb-1" },
                    { id: 2, img: Img1 },
                ],
            },
        ],
    },
    {
        id: 6,
        category: "3d Style",
        quantity: "4781",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img5 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-1.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-4.gif", imgClass: "mb-1" },
                    { id: 2, img: Img4 },
                ],
            },
        ],
    },
    {
        id: 7,
        category: "Collectibles",
        quantity: "3468",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img6 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-2.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-5.gif", imgClass: "mb-1" },
                    { id: 2, img: Img3 },
                ],
            },
        ],
    },
    {
        id: 8,
        category: "Videos",
        quantity: "1674",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img3 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-5.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-2.gif", imgClass: "mb-1" },
                    { id: 2, img: Img5 },
                ],
            },
        ],
    },
];

const marketPlacewidget = [
    {
        id: 1,
        category: "Crypto Card",
        quantity: "743",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img5 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-1.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-4.gif", imgClass: "mb-1" },
                    { id: 2, img: Img4 },
                ],
            },
        ],
    },
    {
        id: 2,
        category: "Artwork",
        quantity: "206",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img6 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-2.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-5.gif", imgClass: "mb-1" },
                    { id: 2, img: Img3 },
                ],
            },
        ],
    },
    {
        id: 3,
        category: "Music",
        quantity: "679",
        subItem: [
            {
                id: 1,
                isChildren: [
                    { id: 1, img: Img2 },
                    { id: 2, img: "https://img.themesbrand.com/velzon/images/img-3.gif", imgClass: "mt-1" },
                ],
            },
            {
                id: 2,
                isChildren: [
                    { id: 1, img: "https://img.themesbrand.com/velzon/images/img-1.gif", imgClass: "mb-1" },
                    { id: 2, img: Img1 },
                ],
            },
        ],
    },
];

const tradingArtworkData = [
    {
        id: 1,
        img: avatar2,
        author: "Nancy Martino",
        title: "Owners",
        cardImg: Img5,
        likes: "19.29k",
        price: "97.8 ETH",
        category: "Patterns Arts & Culture",
        isActive: false,
    },
    {
        id: 2,
        img: avatar9,
        author: "Henry Baird",
        title: "Creators",
        cardImg: Img3,
        likes: "31.64k",
        price: "475.23 ETH",
        category: "Evolved Reality",
        isActive: false,
    },
    {
        id: 3,
        img: avatar10,
        author: "Diana Kohler",
        title: "Owners",
        cardImg: "https://img.themesbrand.com/velzon/images/img-1.gif",
        likes: "8.34k",
        price: "701.38 ETH",
        category: "Long-tailed Macaque",
        isActive: true,
    },
    {
        id: 4,
        img: avatar8,
        author: "James Price",
        title: "Owners",
        cardImg: Img6,
        likes: "29.20k",
        price: "917.83 ETH",
        category: "Robotic Body Art",
        isActive: false,
    },
    {
        id: 5,
        img: avatar2,
        author: "Rickey Teran",
        title: "Creators",
        cardImg: "https://img.themesbrand.com/velzon/images/img-3.gif",
        likes: "78.34k",
        price: "571.24 ETH",
        category: "Creative Filtered Portrait",
        isActive: false,
    },
];

const nftArtworkData = [
    {
        id: 1,
        img: Img2,
        likes: "23.63k",
        title: "The Chirstoper",
        category: "Music",
        highest: "412.30ETH",
        price: "394.7 ETH",
    },
    {
        id: 2,
        img: "https://img.themesbrand.com/velzon/images/img-2.gif",
        likes: "94.1k",
        title: "Trendy Fashion Portraits",
        category: "3d Style",
        highest: "674.92 ETH",
        price: "563.81 ETH",
    },
    {
        id: 3,
        img: Img4,
        likes: "34.12k",
        title: "Smillevers Crypto",
        category: "Crypto Card",
        highest: "41.658 ETH",
        price: "34.81 ETH",
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
    {
        id: 5,
        img: Img1,
        likes: "14.85k",
        title: "Abstract Face Painting",
        category: "Collectibles",
        highest: "122.34ETH",
        price: "97.8 ETH",
    },
];
const popularCreatorsNFT = [
    {
        id: 1,
        cardImg: Img3,
        img: avatar1,
        author: "Tonya Noble",
        products: 5048,
        isFollow: true,
    },
    {
        id: 2,
        cardImg: "https://img.themesbrand.com/velzon/images/img-5.gif",
        img: Img4,
        author: "Kevin Dawson",
        products: 649,
    },
    {
        id: 3,
        cardImg: Img1,
        img: Img6,
        author: "Herbert Stokes",
        products: 918,
    },
    {
        id: 4,
        cardImg: Img5,
        img: "https://img.themesbrand.com/velzon/images/img-2.gif",
        author: "Thomas Taylor",
        products: 345,
        isFollow: true,
    },
    {
        id: 2,
        cardImg: Img2,
        img: "https://img.themesbrand.com/velzon/images/img-1.gif",
        author: "James Price",
        products: 745,
        isFollow: true,
    },
];
export {
    expolreNow,
    aution,
    NFTRanking,
    creatorsData,
    creatorsListData,
    walletConnectData,
    topDrop,
    topCreator,
    topCollection,
    tradingArtworkData,
    nftArtworkData,
    popularCreatorsNFT,
    marketPlacewidget,
};
