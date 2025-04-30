const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { Client } = require("pg");
require("dotenv").config();

//prod cred:
// const strapiUrl = process.env.PROD_STRAPI_URL;
//stage cred:
const strapiUrl = process.env.STAGE_STRAPI_URL;

const TWITCH_AUTH_URL = process.env.TWITCH_AUTH_URL;
const CHUNK_SIZE = 2;
const CHECKPOINT_FILE = path.resolve(__dirname, "checkpointProd.json");
const UPDATED_GAMES_FILE = path.resolve(__dirname, "updatedGamesSeasions.json");
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;

//production database cred
// const dbClient = new Client({
// user: process.env.PROD_DB_USERNAME,
// host: process.env.PROD_DB_HOST,
// database: process.env.PROD_DB_NAME,
// password: process.env.PROD_DB_PASSWORD,
// port: process.env.PROD_DB_PORT,
// });

//stage cred
const dbClient = new Client({
  user: process.env.DB_USERNAME,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});
let accessToken = null;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchAccessToken = async () => {
  const { data } = await axios.post(TWITCH_AUTH_URL, null, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    params: {
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: "client_credentials",
    },
  });
  accessToken = data.access_token;
};
console.log(accessToken, "accessToken");
const readFile = (filePath) =>
  fs.existsSync(filePath)
    ? JSON.parse(fs.readFileSync(filePath, "utf-8"))
    : null;

const writeFile = (filePath, data) =>
  fs.writeFileSync(filePath, JSON.stringify(data), "utf-8");

const loadCheckpoint = () => readFile(CHECKPOINT_FILE) || 0;

const saveCheckpoint = (id) => writeFile(CHECKPOINT_FILE, { id });

const logUpdatedGame = (gameId) => {
  const logs = readFile(UPDATED_GAMES_FILE) || [];
  logs.push(gameId);
  writeFile(UPDATED_GAMES_FILE, logs);
};

const fetchGames = async (start, limit) => {
  const result = await dbClient.query(
    "SELECT * FROM games ORDER BY created_at ASC LIMIT $1 OFFSET $2",
    [limit, start]
  );
  return result.rows;
};

const updateGame = async (gameId, updatedData) => {
  const headerFromApi = {
    "Client-ID": CLIENT_ID,
    Authorization: `Bearer ${accessToken}`,
  };
  const objData = await objectForUpdateGame(updatedData, headerFromApi);
  await updateGameDataWithNewFeilds(objData, gameId);
};

const processGames = async () => {
  const start = loadCheckpoint();
  let hasMoreGames = true;

  while (hasMoreGames) {
    const games = await fetchGames(start.id, CHUNK_SIZE);
    // const games = [
    //   {
    //     id: 5002,
    //     title: "Arma 3",
    //     description:
    //       "Arma 3 presents an expansive military simulation experience that immerses players in a vast sandbox world filled with both realistic and futuristic combat scenarios. Whether engaging in intense battles solo or teaming up with others in thrilling multiplayer encounters, the game offers diverse gameplay that keeps every mission fresh and exciting. At its core, Arma 3 features a robust editor, empowering players to craft their own unique scenarios, campaigns, and map modes to share with the community. The richly detailed environments and strategic depth ensure that every firefight and tactical decision feels consequential. With a dynamic blend of storytelling and player-driven content, Arma 3 remains a standout title for military simulation enthusiasts. Explore the boundaries of creativity and strategy in this unparalleled virtual battlefield.",
    //     slug: "arma-3",
    //     rating: "0.00",
    //     played: "0",
    //     queued: "0",
    //     hours_to_complete: "0.00",
    //     beat: "0",
    //     created_by_id: null,
    //     updated_by_id: 4,
    //     followers: 0,
    //     critic_rating: null,
    //     genres: ["Shooter", "Simulator", "Strategy", "Tactical"],
    //     game_modes: ["Single player", "Multiplayer", "Co-operative"],
    //     site_url: "https://www.igdb.com/games/arma-3",
    //     player_perspective: [
    //       "First person",
    //       "Third person",
    //       "Bird view / Isometric",
    //     ],
    //     themes: ["Action", "Sandbox", "Open world", "Warfare"],
    //     franchises: [],
    //     game_engines: ["Real Virtuality 4"],
    //     alternative_names: [
    //       "아르마 3",
    //       "アルマ 3",
    //       "ArmA III",
    //       "Arma 3",
    //       "ArmA: Armed Assault ",
    //     ],
    //     keywords: [
    //       "modern warfare",
    //       "helicopter",
    //       "vehicular combat",
    //       "surrealism",
    //       "invasion",
    //       "military",
    //       "skydiving",
    //       "war",
    //       "steam",
    //       "special forces",
    //       "online",
    //       "steam trading cards",
    //       "scuba diving",
    //       "tank",
    //       "sequel",
    //       "user generated content",
    //       "vehicle combat",
    //       "digital distribution",
    //       "xbox controller support for pc",
    //       "platform exclusive",
    //       "modern military",
    //       "customization",
    //       "steam workshop",
    //       "polygonal 3d",
    //       "motion blur",
    //       "wasd movement",
    //       "retail games with steam activation",
    //       "sprinting mechanics",
    //       "sniping",
    //       "ragdoll physics",
    //       "downloadable content",
    //       "useable vehicles",
    //       "underwater gameplay",
    //       "fuel management",
    //       "gamescom 2013",
    //       "head tracking",
    //       "mission editor",
    //       "realism",
    //       "gamescom 2012",
    //       "squad tactics",
    //       "multiplayer lan",
    //       "voice chat",
    //       "squad based shooter",
    //       "weapon modification",
    //       "dedicated servers",
    //       "air strike",
    //       "text dialogue",
    //       "e3 2012",
    //       "movement penalty",
    //       "real time game clock",
    //       "world war iii",
    //       "realistic ballistics",
    //       "military jargon",
    //       "anti-cheat system",
    //       "acronym titles",
    //       "e3 2005",
    //     ],
    //     language_supports: [
    //       "Czech",
    //       "Italian",
    //       "French",
    //       "Russian",
    //       "English",
    //       "Chinese (Simplified)",
    //       "Chinese (Traditional)",
    //       "German",
    //       "Portuguese (Brazil)",
    //       "Korean",
    //       "Japanese",
    //       "Turkish",
    //       "Polish",
    //       "Spanish (Spain)",
    //     ],
    //     external_games: ["Arma 3", "ArmA III", "Arma 3"],
    //     involved_companies: [],
    //     collections: ["ArmA"],
    //     platforms: ["Linux", "PC (Microsoft Windows)", "Mac"],
    //     website_links: [
    //       { url: "http://www.arma3.com", category: "official" },
    //       {
    //         url: "http://armedassault.wikia.com/wiki/ARMA_3",
    //         category: "wikia",
    //       },
    //       {
    //         url: "https://www.youtube.com/user/Arma3official",
    //         category: "youtube",
    //       },
    //       {
    //         url: "https://store.steampowered.com/app/107410",
    //         category: "steam",
    //       },
    //       {
    //         url: "https://en.wikipedia.org/wiki/ARMA_3",
    //         category: "wikipedia",
    //       },
    //       {
    //         url: "https://www.facebook.com/armaplatform",
    //         category: "facebook",
    //       },
    //       {
    //         url: "https://www.twitter.com/armaplatform",
    //         category: "twitter",
    //       },
    //       {
    //         url: "https://www.twitch.tv/directory/game/Arma%203",
    //         category: "twitch",
    //       },
    //       { url: "https://www.reddit.com/r/arma3/", category: "reddit" },
    //     ],
    //     videos: [
    //       {
    //         url: "og-MkfnOZmE",
    //         name: "Bootcamp update Trailer",
    //         title: "ArmA III - Bootcamp update Trailer (Official)",
    //       },
    //       {
    //         url: "1VcfRVTIRwU",
    //         name: "Launch Trailer",
    //         title: "ArmA III - Launch Trailer (Official)",
    //       },
    //       {
    //         url: "VRp_1UWWFv0",
    //         name: "Ground vehicle Trailer",
    //         title: "Arma 3 - Community Guide: Ground Vehicle Crew",
    //       },
    //     ],
    //     user_ratings: null,
    //     critic_ratings: null,
    //     want_to_play: null,
    //     playing_users: null,
    //     played_users: null,
    //     aggregate_rating: 72.8,
    //     series: "",
    //     is_spin_off: "",
    //     is_season: null,
    //     is_expansion: null,
    //     is_updated_from_script: true,
    //     total_hot_score: "0.00",
    //     editorial_anticipated_release: null,
    //     game_category: null,
    //     average_rating: null,
    //   },
    //   {
    //     id: 30304,
    //     title: "Apex Legends: Season 1",
    //     description:
    //       "Prepare for an exhilarating battle where survival is the ultimate goal. In Apex Legends: Season 1, you'll find yourself in a dynamic arena filled with unique characters, each equipped with extraordinary abilities that can turn the tide of combat. Engage in intense team-based gameplay, strategizing with your squad to outsmart and outmaneuver your foes. The Wild Frontier offers a vibrant landscape, rich with hidden secrets and tactical opportunities just waiting to be discovered. As legends clash, forge your legend in a relentless pursuit of glory. It's time to prove your mettle in this fast-paced and unpredictable world.",
    //     slug: "apex-legends-season-1",
    //     rating: "5.00",
    //     played: "0",
    //     queued: "0",
    //     hours_to_complete: "0.00",
    //     beat: "0",
    //     created_by_id: null,
    //     updated_by_id: null,
    //     followers: 0,
    //     critic_rating: null,
    //     genres: ["Shooter"],
    //     game_modes: ["Battle Royale"],
    //     site_url: "https://www.igdb.com/games/apex-legends-season-1",
    //     player_perspective: ["First person"],
    //     themes: ["Action", "Science fiction"],
    //     franchises: [],
    //     game_engines: [],
    //     alternative_names: ["Apex Legends: Season 1 - Wild Frontier"],
    //     keywords: ["free-to-play"],
    //     language_supports: [],
    //     external_games: [],
    //     involved_companies: [],
    //     collections: [],
    //     platforms: ["PC (Microsoft Windows)", "PlayStation 4", "Xbox One"],
    //     website_links: [
    //       {
    //         url: "https://www.ea.com/games/apex-legends/season-1",
    //         category: "official",
    //       },
    //       {
    //         url: "https://titanfall.fandom.com/wiki/Apex_Legends",
    //         category: "wikia",
    //       },
    //       {
    //         url: "https://en.wikipedia.org/wiki/Apex_Legends",
    //         category: "wikipedia",
    //       },
    //       { url: "https://www.facebook.com/playapex", category: "facebook" },
    //       { url: "https://twitter.com/PlayApex", category: "twitter" },
    //       { url: "https://www.twitch.tv/playapex", category: "twitch" },
    //       {
    //         url: "https://www.instagram.com/playapex",
    //         category: "instagram",
    //       },
    //       {
    //         url: "https://www.youtube.com/channel/UC0ZV6M2THA81QT9hrVWJG3A",
    //         category: "youtube",
    //       },
    //       { url: "https://www.reddit.com/r/apexlegends", category: "reddit" },
    //     ],
    //     videos: [
    //       {
    //         url: "7M4Vds3fI9E",
    //         name: "Trailer",
    //         title: "Apex Legends Season 1 – Wild Frontier Trailer",
    //       },
    //       {
    //         url: "aOdTqvcEaZ8",
    //         name: "Trailer",
    //         title: "Meet Octane – Apex Legends Character Trailer",
    //       },
    //       {
    //         url: "mrvIaAYZYVw",
    //         name: "Trailer",
    //         title: "Apex Legends Season 1 Battle Pass Trailer",
    //       },
    //     ],
    //     user_ratings: null,
    //     critic_ratings: null,
    //     want_to_play: null,
    //     playing_users: null,
    //     played_users: null,
    //     aggregate_rating: null,
    //     series: "",
    //     is_spin_off: "",
    //     is_season: "",
    //     is_expansion: null,
    //     is_updated_from_script: true,
    //     total_hot_score: "0.00",
    //     editorial_anticipated_release: null,
    //     game_category: null,
    //     average_rating: null,
    //   },
    // ];
    if (games.length === 0) {
      console.log("No more games to process. Exiting...");
      hasMoreGames = false;
      break;
    }

    // Step 1: Separate games based on the presence of `site_url` and `slug`
    const gamesWithSiteUrl = games.filter((game) => game.site_url);
    const gamesWithSlug = games.filter((game) => game.slug && !game.site_url);

    // Step 2: Collect site_urls and slugs to create two separate queries
    const siteUrls = gamesWithSiteUrl.map((game) => game.site_url);
    const slugs = gamesWithSlug.map((game) => game.slug);

    let updatedDataWithSiteUrl = [];
    let updatedDataWithSlug = [];

    // Step 3: Prepare and make API requests for both `site_url` and `slug`
    if (siteUrls.length > 0) {
      const siteUrlQuery = `fields *,genres.name,game_modes.name,player_perspectives.name,game_engines.name,involved_companies.developer,involved_companies.publisher,involved_companies.company.name,keywords.name,platforms.name,release_dates.*,screenshots.url,themes.name,videos.video_id,videos.name,websites.category,websites.url,language_supports.language.name,game_localizations.*,similar_games.*,external_games.name,cover.url,artworks.url,age_ratings.*,franchises.name,collections.name,release_dates.platform.*,alternative_names.name,similar_games.genres.name,similar_games.game_modes.name,similar_games.player_perspectives.name,similar_games.game_engines.name,similar_games.involved_companies.developer,similar_games.involved_companies.publisher,similar_games.involved_companies.company.name,similar_games.keywords.name,similar_games.platforms.name,similar_games.release_dates.*,similar_games.screenshots.url,similar_games.themes.name,similar_games.videos.video_id,similar_games.videos.name,similar_games.websites.category,similar_games.websites.url,similar_games.language_supports.language.name,similar_games.game_localizations.*,similar_games.similar_games.*,similar_games.external_games.name,similar_games.cover.url,similar_games.artworks.url,similar_games.age_ratings.*,similar_games.franchises.name,similar_games.collections.name,similar_games.release_dates.platform.*,expansions.*,similar_games.alternative_names.name,expansions.genres.name,expansions.game_modes.name,expansions.player_perspectives.name,expansions.game_engines.name,expansions.involved_companies.developer,expansions.involved_companies.publisher,expansions.involved_companies.company.name,expansions.keywords.name,expansions.platforms.name,expansions.release_dates.*,expansions.screenshots.url,expansions.themes.name,expansions.videos.video_id,expansions.videos.name,expansions.websites.category,expansions.websites.url,expansions.language_supports.language.name,expansions.game_localizations.*,expansions.similar_games.*,expansions.external_games.name,expansions.cover.url,expansions.artworks.url,expansions.age_ratings.*,expansions.franchises.name,expansions.collections.name,expansions.release_dates.platform.*,expansions.expansions.*,expansions.alternative_names.name; site_url; where url = (${siteUrls
        .map((url) => `"${url}"`)
        .join(",")}); limit ${siteUrls.length};`;

      updatedDataWithSiteUrl = await fetchFromIGDB(siteUrlQuery);
      console.log(
        updatedDataWithSiteUrl,
        "Updated Data for games with site_url"
      );
    }

    if (slugs.length > 0) {
      const slugQuery = `fields *,genres.name,game_modes.name,player_perspectives.name,game_engines.name,involved_companies.developer,involved_companies.publisher,involved_companies.company.name,keywords.name,platforms.name,release_dates.*,screenshots.url,themes.name,videos.video_id,videos.name,websites.category,websites.url,language_supports.language.name,game_localizations.*,similar_games.*,external_games.name,cover.url,artworks.url,age_ratings.*,franchises.name,collections.name,release_dates.platform.*,alternative_names.name,similar_games.genres.name,similar_games.game_modes.name,similar_games.player_perspectives.name,similar_games.game_engines.name,similar_games.involved_companies.developer,similar_games.involved_companies.publisher,similar_games.involved_companies.company.name,similar_games.keywords.name,similar_games.platforms.name,similar_games.release_dates.*,similar_games.screenshots.url,similar_games.themes.name,similar_games.videos.video_id,similar_games.videos.name,similar_games.websites.category,similar_games.websites.url,similar_games.language_supports.language.name,similar_games.game_localizations.*,similar_games.similar_games.*,similar_games.external_games.name,similar_games.cover.url,similar_games.artworks.url,similar_games.age_ratings.*,similar_games.franchises.name,similar_games.collections.name,similar_games.release_dates.platform.*,expansions.*,similar_games.alternative_names.name,expansions.genres.name,expansions.game_modes.name,expansions.player_perspectives.name,expansions.game_engines.name,expansions.involved_companies.developer,expansions.involved_companies.publisher,expansions.involved_companies.company.name,expansions.keywords.name,expansions.platforms.name,expansions.release_dates.*,expansions.screenshots.url,expansions.themes.name,expansions.videos.video_id,expansions.videos.name,expansions.websites.category,expansions.websites.url,expansions.language_supports.language.name,expansions.game_localizations.*,expansions.similar_games.*,expansions.external_games.name,expansions.cover.url,expansions.artworks.url,expansions.age_ratings.*,expansions.franchises.name,expansions.collections.name,expansions.release_dates.platform.*,expansions.expansions.*,expansions.alternative_names.name; slug; where slug = (${slugs
        .map((slug) => `"${slug}"`)
        .join(",")}); limit ${slugs.length};`;

      updatedDataWithSlug = await fetchFromIGDB(slugQuery);
      console.log(updatedDataWithSlug, "Updated Data for games with slug");
    }

    // Step 4: Merge and update games with the data from both queries
    for (let i = 0; i < games.length; i++) {
      const game = games[i];
      let updatedData;

      // Step 5: Check if data from `site_url` query is available
      if (game.site_url) {
        updatedData = updatedDataWithSiteUrl.find(
          (data) => data.url === game.site_url
        );
      }

      if (!updatedData && game.slug) {
        updatedData = updatedDataWithSlug.find(
          (data) => data.slug === game.slug
        );
      }

      console.log(
        updatedData,
        `updatedData for game ID: ${game.id}`,
        `Script ending succesfully`
      );

      if (updatedData) {
        await updateGame(game.id, updatedData);
        logUpdatedGame(game.id);
      }
    }

    const lastProcessedGame = games[games.length - 1];
    saveCheckpoint(lastProcessedGame.id);
    start.id = lastProcessedGame.id + 1;

    // Add a delay to avoid rate-limiting issues
    await delay(1000);
  }
};

const fetchFromIGDB = async (query) => {
  console.log(query, "query");
  const response = await fetch("https://api.igdb.com/v4/games", {
    method: "POST",
    headers: {
      "Client-ID": CLIENT_ID,
      Authorization: `Bearer ${accessToken}`,
    },
    body: query,
  });
  const data = await response.json();
  return data;
};

const startProcess = async () => {
  try {
    await dbClient.connect();
    console.log("Database connected successfully!");
    await fetchAccessToken();
    if (accessToken) {
      await processGames();
    }
  } catch (err) {
    console.error("Error connecting to the database:", err);
  }
};

const getOrCreateSeason = async (game, headerFromApi) => {
  console.log(game.category === 7, "gamemememmememem", game?.category);
  let seasonGame;
  try {
    if (game.category === 7) {
      const parentGame = game.parent_game
        ? await findOrCreateParentGame(game.parent_game, headerFromApi)
        : null;
      console.log("inside category", parentGame);
      const existingSeason = await checkIfGameExists(game.slug);
      if (existingSeason) {
        seasonGame = existingSeason;
      }
      //   const gameData = {
      //     data: {
      //       isSeason: "true",
      //     },
      //   };
      //   const seasionId =
      //     seasonGame && seasonGame.id
      //       ? seasonGame.id
      //       : seasonGame.data && seasonGame.data.id;
      //   const updatedGame = await updateGameEntryInStrapi(seasionId, gameData);
      // Update parent game with the season ID
      if (parentGame && seasonGame) {
        await updateParentGameWithSeasonId(
          parentGame && parentGame.id
            ? parentGame.id
            : parentGame.data && parentGame.data.id,
          seasonGame && seasonGame.id
            ? seasonGame.id
            : seasonGame.data && seasonGame.data.id,
          parentGame
        );
      }
      return seasonGame && seasonGame.id
        ? seasonGame.id
        : seasonGame.data
        ? seasonGame.data.id
        : null;
    } else {
      console.log(`Skipping game: ${game.name} as it's not a season.`);
      return null;
    }
  } catch (error) {
    console.error(`Failed to process game "${game.name}": ${error.message}`);
    return null;
  }
};

const getParentGameById = async (parentGameId, headerFromApi) => {
  try {
    const igdbEndpoint = `https://api.igdb.com/v4/games`;
    const response = await axios.post(
      igdbEndpoint,
      `fields *,genres.*,game_modes.*,player_perspectives.*,game_engines.*,involved_companies.*,involved_companies.company.*,keywords.*,platforms.*,release_dates.*,screenshots.*,themes.*,videos.*,websites.*,language_supports.*,language_supports.language.*,game_localizations.*,similar_games.*,external_games.*,cover.*,artworks.*,age_ratings.*,franchises.*,collections.*,release_dates.platform.*,alternative_names.*,similar_games.genres.*,similar_games.game_modes.*,similar_games.player_perspectives.*,similar_games.game_engines.*,similar_games.involved_companies.*,similar_games.involved_companies.company.*,similar_games.keywords.*,similar_games.platforms.*,similar_games.release_dates.*,similar_games.screenshots.*,similar_games.themes.*,similar_games.videos.*,similar_games.websites.*,similar_games.language_supports.*,similar_games.language_supports.language.*,similar_games.game_localizations.*,similar_games.similar_games.*,similar_games.external_games.*,similar_games.cover.*,similar_games.artworks.*,similar_games.age_ratings.*,similar_games.franchises.*,similar_games.collections.*,similar_games.release_dates.platform.*,expansions.*,similar_games.alternative_names.*,expansions.genres.*,expansions.game_modes.*,expansions.player_perspectives.*,expansions.game_engines.*,expansions.involved_companies.*,expansions.involved_companies.company.*,expansions.keywords.*,expansions.platforms.*,expansions.release_dates.*,expansions.screenshots.*,expansions.themes.*,expansions.videos.*,expansions.websites.*,expansions.language_supports.*,expansions.language_supports.language.*,expansions.game_localizations.*,expansions.similar_games.*,expansions.external_games.*,expansions.cover.*,expansions.artworks.*,expansions.age_ratings.*,expansions.franchises.*,expansions.collections.*,expansions.release_dates.platform.*,expansions.expansions.*,expansions.alternative_names.*; where id = ${parentGameId};`,
      {
        headers: headerFromApi,
      }
    );
    if (response.data && response.data.length > 0) {
      const parentGame = response.data[0];
      console.log(`Found parent game: ${parentGame.name}`);
      return parentGame; // Return the parent game object
    } else {
      console.warn(`Parent game with ID ${parentGameId} not found.`);
      return null;
    }
  } catch (error) {
    console.error(
      `Failed to fetch parent game with ID ${parentGameId}:`,
      error.message
    );
    return null;
  }
};

const findOrCreateParentGame = async (parentGameId, headerFromApi) => {
  try {
    const parentGame = await getParentGameById(parentGameId, headerFromApi);
    if (!parentGame) {
      console.error(`Parent game with ID: ${parentGameId} not found.`);
      return null;
    }
    const existingParentGame = await checkIfGameExists(parentGame.slug);
    if (existingParentGame) {
      return existingParentGame;
    }
  } catch (error) {
    console.error(`Failed to find or create parent game: ${error.message}`);
    return null;
  }
};
const updateParentGameWithSeasonId = async (parentId, seasonId, parentGame) => {
  try {
    const existingSeasonIds = parentGame?.seasons?.map((season) => season.id);

    // Step 3: Add the new season ID if it's not already included
    const updatedSeasonIds = Array.from(
      new Set([...existingSeasonIds, seasonId])
    );
    const updateData = {
      data: {
        seasons: updatedSeasonIds,
      },
    };
    const response = await axios.put(
      `${strapiUrl}/api/games/${parentId}`,
      updateData
    );
    if (response.status === 200) {
      console.log(
        `Updated parent game ID ${parentId} with season ID ${seasonId}`
      );
    } else {
      console.error(`Failed to update parent game ID ${parentId}`);
    }
  } catch (error) {
    console.error(`Error updating parent game: ${error}`);
  }
};
const objectForUpdateGame = async (parsedData, headerFromApi) => {
  try {
    const expansionGames = await handleGameExpansions(
      parsedData,
      headerFromApi
    );
    const sessionGames = await getOrCreateSeason(parsedData, headerFromApi);
    const gameData = {
      expansions: expansionGames || [],
      isSeason: sessionGames ? "true" : null,
    };
    return gameData;
  } catch (error) {
    console.error(`Failed to parse file`, error);
  }
};

const fetchWithRetry = async (
  url,
  data,
  headers,
  retries = 5,
  retryCount = 1
) => {
  try {
    return await axios.post(url, data, { headers });
  } catch (error) {
    if (error.response && error.response.status === 429 && retries > 0) {
      const retryAfter = 2 ** retryCount * 2000;
      console.log(`Retrying request in ${retryAfter} ms...`);
      await delay(retryAfter);
      return fetchWithRetry(url, data, headers, retries - 1, retryCount + 1);
    }
    throw error;
  }
};

const checkIfGameExists = async (slug) => {
  try {
    const strapiApiUrl = `${strapiUrl}/api/games/${slug}`;
    const response = await axios.get(strapiApiUrl);
    return response.data;
  } catch (error) {
    console.error(`Error checking for game with slug "${slug}":`, error);
    return null;
  }
};

const updateGameEntryInStrapi = async (gameId, updateData) => {
  try {
    const response = await axios.put(
      `${strapiUrl}/api/games/${gameId}`,
      updateData
    );
    console.log("Game updated successfully:");
    return response.data;
  } catch (error) {
    console.error(
      "Error updating game entry:",
      error.response ? error.response.data : error
    );
  }
};

//Handle Expansions
const handleGameExpansions = async (parsedData, headerFromApi) => {
  if (parsedData.expansions) {
    let expansionGamesArray = [];
    if (
      Array.isArray(parsedData.expansions) &&
      parsedData.expansions.length > 0
    ) {
      try {
        for (const expansionGame of parsedData.expansions) {
          const existingGame = await checkIfGameExists(expansionGame.slug);
          let gameId;
          if (existingGame) {
            gameId = existingGame.id;
            const newGame = {
              data: {
                isExpansion: "true",
              },
            };
            const updatedGame = await updateGameEntryInStrapi(gameId, newGame);

            expansionGamesArray.push(...expansionGamesArray, gameId);
          }
        }
        // Return the array of expansion game IDs
        return expansionGamesArray;
      } catch (error) {
        console.error(`Failed to handle expansion games: ${error.message}`);
        return [];
      }
    }
  }
  return [];
};

const updateGameDataWithNewFeilds = async (dataObj, gameId) => {
  console.log(dataObj, "dataObjecttctctct");
  try {
    const updateData = {
      data: {
        expansions: dataObj.expansions || [],
        isSeason: dataObj?.isSeason,
      },
    };
    const response = await axios.put(
      `${strapiUrl}/api/games/${gameId}`,
      updateData
    );
    if (response.status === 200) {
      console.log(`Updated game data ID ${gameId}`);
    } else {
      console.error(`Failed to update game data ID ${gameId}`);
    }
  } catch (error) {
    console.error(`Error updating game data: ${error}`);
  }
};
startProcess();
