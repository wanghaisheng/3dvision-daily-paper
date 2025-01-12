// consts.js
export const SITE_TITLE = `Heisenberg Github Activity daily track`;
export const SITE_DESCRIPTION = '海生在GitHub上的蛛丝马迹';
export const SITE_EMAIL = 'admin@borninsea.com';
export const SITE_NAME = 'daily.borninsea.com';
export const SITE_URL = "https://daily.borninsea.com";
export const SITE_LANG = "zh-CN";

// Author, used only when author is not specified on the page, 
// has lower priority than the author specified in the md file.
// 作者信息，仅用在文章没有指定作者的时候，优先显示在 md 文件中指定的作者。
export const SITE_AUTHOR = "Wanghaisheng";

export const NAV_LINKS = {
  products: {
    title: "作品",
    links: [
      { name: "本站博客", url: "/" },
      { name: "Heisenberg Link", url: "https://borninsea.com" }
    ]
  },
  social: {
    title: "社媒",
    links: [
      { name: "Twitter", url: "https://twitter.com/edwin_uestc" },
      { name: "Github", url: "https://github.com/wanghaisheng" },
      { name: "Telegram", url: "https://t.me/xxx" }
    ]
  },
  friends: {
    title: "友链",
    links: [
      { name: "TiktokaStudio", url: "https://tiktokastudio.com" },
       { name: "HeyTCM", url: "https://heytcm.com" }
    ]
  }
};

export const COPYRIGHT_LINK = "https://borninsea.com";
export const COPYRIGHT_TEXT = "Made by Heisenberg";
