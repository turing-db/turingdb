import flagus from "../assets/images/flags/us.svg";
import flagspain from "../assets/images/flags/spain.svg";
import flaggermany from "../assets/images/flags/germany.svg";
import flagitaly from "../assets/images/flags/italy.svg";
import flagrussia from "../assets/images/flags/russia.svg";
import flagchina from "../assets/images/flags/china.svg";
import flagfrench from "../assets/images/flags/french.svg";
import flagarabic from "../assets/images/flags/ar.svg";

const languages = {
  sp: {
    label: "Española",
    flag: flagspain,
  },
  gr: {
    label: "Deutsche",
    flag: flaggermany,
  },
  it: {
    label: "Italiana",
    flag: flagitaly,
  },
  rs: {
    label: "русский",
    flag: flagrussia,
  },
  en: {
    label: "English",
    flag: flagus,
  },
  cn: {
    label: "中国人",
    flag: flagchina,
  },
  fr: {
    label: "français",
    flag: flagfrench,
  },
  ar: {
    label: "Arabic",
    flag: flagarabic,
  },
}

export default languages
