require("dotenv").config();
const { fetch } = require("undici");
const fs = require("fs");

const KLAVIYO_API_KEY = process.env.KLAVIYO_API_KEY;
const KLAVIYO_SEGMENT_UUID = process.env.KLAVIYO_SEGMENT_UUID;
const KLAVIYO_API_VERSION = "2025-04-15";
const PROFILE_CACHE_PATH = "./klaviyo_profiles.json";

const KLAVIYO_HEADERS = {
  Authorization: `Klaviyo-API-Key ${KLAVIYO_API_KEY}`,
  Accept: "application/json",
  Revision: KLAVIYO_API_VERSION,
};

async function fetchAllKlaviyoSegmentProfiles(limit = 110000, maxRetries = 3) {
  const profiles = [];
  const seenEmails = new Set();
  let cursor = null;
  let page = 1;

  console.log("⏳ Fetching Klaviyo segment profiles (deduping by email)...");

  while (profiles.length < limit) {
    let url = `https://a.klaviyo.com/api/segments/${KLAVIYO_SEGMENT_UUID}/profiles/?page[size]=100`;
    if (cursor) url += `&page[cursor]=${encodeURIComponent(cursor)}`;

    let attempt = 0;
    let json = null;

    while (attempt < maxRetries) {
      try {
        const res = await fetch(url, { headers: KLAVIYO_HEADERS });
        json = await res.json();

        if (!json.data || !Array.isArray(json.data)) {
          throw new Error("Invalid data format");
        }

        break;
      } catch (err) {
        attempt++;
        console.warn(`⚠️ Retry ${attempt}/${maxRetries} on page ${page}: ${err.message}`);
        await new Promise((r) => setTimeout(r, 500 * attempt));
      }
    }

    if (!json || !Array.isArray(json.data)) {
      console.warn(`❌ Failed after ${maxRetries} retries on page ${page}`);
      break;
    }

    let newCount = 0;

    for (const profile of json.data) {
      const email = profile.attributes?.email?.toLowerCase().trim();
      if (email && !seenEmails.has(email)) {
        seenEmails.add(email);
        profiles.push({
          email,
          profileId: profile.id,
        });
        newCount++;
      }
    }

    console.log(`📥 Page ${page++}: Added ${newCount} new → Total unique = ${profiles.length}/${limit}`);

    // ✅ Incremental write to disk after each page
    try {
      const limited = profiles.slice(0, limit);
      fs.writeFileSync(PROFILE_CACHE_PATH, JSON.stringify(limited, null, 2));
      console.log(`💾 Incrementally saved ${limited.length} profiles to ${PROFILE_CACHE_PATH}`);
    } catch (err) {
      console.error("❌ Error during incremental save:", err);
    }

    // ✅ Properly parse next cursor from full URL
    if (json.links?.next) {
      try {
        const nextUrl = new URL(json.links.next);
        cursor = nextUrl.searchParams.get("page[cursor]");
      } catch (err) {
        console.error("❌ Failed to parse next cursor:", err);
        break;
      }
    } else {
      console.log("🚫 No more pages. Reached end of pagination.");
      break;
    }

    await new Promise((r) => setTimeout(r, 150)); // throttle
  }

  console.log(`✅ Finished fetching. Final total: ${profiles.length} unique profiles`);
}

fetchAllKlaviyoSegmentProfiles();
