import dotenv from 'dotenv';
dotenv.config();
import { fetch } from 'undici';
import fs from 'fs';

const SHOPIFY_API_TOKEN = process.env.SHOPIFY_API_TOKEN;
const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-04';
const KLAVIYO_API_KEY = process.env.KLAVIYO_API_KEY;

const SHOPIFY_HEADERS = {
  'X-Shopify-Access-Token': SHOPIFY_API_TOKEN,
  'Content-Type': 'application/json',
};

const ENRICHED_FILE = 'enriched_profiles2.ndjson';

// ✅ Load processed emails from NDJSON instead of processed_profiles.json
function loadProcessedEmailsFromNDJSON(filePath) {
  if (!fs.existsSync(filePath)) return [];

  const lines = fs.readFileSync(filePath, 'utf-8').split('\n').filter(Boolean);
  const emails = new Set();

  for (const line of lines) {
    try {
      const json = JSON.parse(line);
      if (json.email) {
        emails.add(json.email);
      }
    } catch (err) {
      console.warn(`⚠️ Skipping invalid NDJSON line: ${line}`);
    }
  }

  return Array.from(emails);
}

let processedEmails = loadProcessedEmailsFromNDJSON(ENRICHED_FILE);

// 🚫 No longer needed
function updateProcessedProfiles(email) {
  // no-op, we're tracking via NDJSON now
}

// 📬 Get Shopify customer by email
async function getShopifyCustomerIdByEmail(email) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/customers/search.json?query=email:${encodeURIComponent(email)}&fields=id,email`;

  try {
    const res = await fetch(url, { method: 'GET', headers: SHOPIFY_HEADERS });

    if (res.status === 429) {
      console.warn('🚨 Rate limit reached. Retrying...');
      await new Promise(resolve => setTimeout(resolve, 1000));
      return await getShopifyCustomerIdByEmail(email);
    }

    const json = await res.json();

    if (json.customers && json.customers.length > 0) {
      return json.customers[0];
    } else {
      console.warn(`❌ No customer found for ${email}`);
    }
  } catch (error) {
    console.error(`❌ Error fetching customer for ${email}:`, error);
  }

  return null;
}

// 📦 Get most recent Shopify order
async function getMostRecentOrder(customerId) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/orders.json?customer_id=${customerId}&status=any&limit=1&order=created_at desc`;

  try {
    const res = await fetch(url, { method: 'GET', headers: SHOPIFY_HEADERS });
    const json = await res.json();

    if (json.orders && json.orders.length > 0) {
      const order = json.orders[0];
      const lineItem = order.line_items[0];

      return {
        title: lineItem?.title || 'N/A',
        sku: lineItem?.sku || 'N/A',
        orderDate: order.created_at || 'N/A',
      };
    }
  } catch (error) {
    console.error(`❌ Error fetching most recent order for customer ID ${customerId}:`, error);
  }

  return null;
}

// 💾 Save enriched profile to NDJSON
function saveEnrichedProfileIncrementally(profile) {
  const stream = fs.createWriteStream(ENRICHED_FILE, { flags: 'a' });
  stream.write(JSON.stringify(profile) + '\n');
  stream.end();
}

// 🧠 Enrich profiles
async function enrichKlaviyoProfiles(profiles) {
  const enrichedProfiles = [];
  let processedCount = 0;

  for (const profile of profiles) {
    const { email } = profile;

    if (processedEmails.includes(email)) {
      processedCount++;
      console.log(`[SKIP] ${email} already enriched. Skipping...`);
      continue;
    }

    try {
      console.log(`[PROCESSING] Fetching Shopify data for ${email}...`);

      const customer = await getShopifyCustomerIdByEmail(email);
      if (!customer) {
        console.warn(`[ERROR] No Shopify customer found for ${email}`);
        processedCount++;
        continue;
      }

      const order = await getMostRecentOrder(customer.id);
      if (!order) {
        console.warn(`[ERROR] No order found for ${email}`);
        processedCount++;
        continue;
      }

      const enrichedProfile = {
        profileId: profile.profileId,
        email,
        mostRecentOrder: order,
      };

      saveEnrichedProfileIncrementally(enrichedProfile);
      updateProcessedProfiles(email); // no-op

      enrichedProfiles.push(enrichedProfile);
      processedCount++;

      console.log(`[SUCCESS] Enriched ${email} - Order: ${order.title} | SKU: ${order.sku}`);
      console.log(`Processed ${processedCount}/${profiles.length} profiles...`);
    } catch (error) {
      console.error(`[ERROR] Failed to enrich ${email}:`, error);
      processedCount++;
    }
  }

  return enrichedProfiles;
}

// 🚀 Main
async function main() {
  try {
    const profiles = JSON.parse(fs.readFileSync('klaviyo_profiles.json', 'utf-8'));
    console.log(`🚀 Starting enrichment of ${profiles.length} Klaviyo profiles...`);

    await enrichKlaviyoProfiles(profiles);

    console.log('🎉 Enrichment complete! Profiles saved to enriched_profiles2.ndjson');
  } catch (error) {
    console.error('❌ Error in main:', error);
  }
}

main().catch(console.error);
