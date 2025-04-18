require('dotenv').config();
const { fetch } = require("undici");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const KLAVIYO_API_KEY = process.env.KLAVIYO_API_KEY;
const KLAVIYO_SEGMENT_UUID = process.env.KLAVIYO_SEGMENT_UUID;
const KLAVIYO_API_VERSION = "2025-04-15";
const SHOPIFY_API_TOKEN = process.env.SHOPIFY_API_TOKEN;  // Use SHOPIFY_API_TOKEN here
const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;

const KLAVIYO_HEADERS = {
  "Authorization": `Klaviyo-API-Key ${KLAVIYO_API_KEY}`,
  "Accept": "application/json",
  "Revision": KLAVIYO_API_VERSION
};

const SHOPIFY_HEADERS = {
  "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,  // Correct token usage
  "Content-Type": "application/json"
};

// 🔍 Step 1: Fetch profiles from Klaviyo segment
async function fetchKlaviyoProfiles(limit = 110000) {
  const url = `https://a.klaviyo.com/api/segments/${KLAVIYO_SEGMENT_UUID}/profiles/`;
  let customers = [];
  let nextPageUrl = url;

  while (nextPageUrl && customers.length < limit) {
    const res = await fetch(nextPageUrl, { method: "GET", headers: KLAVIYO_HEADERS });
    const json = await res.json();
    if (!json?.data) break;

    customers = customers.concat(json.data);
    nextPageUrl = json?.links?.next || null;
  }

  return customers.slice(0, limit).map(profile => ({
    email: profile.attributes?.email,
    profileId: profile.id,
  }));
}

// 🔍 Step 1.5: Fetch Klaviyo Segment Name
async function fetchKlaviyoSegmentName() {
  const url = `https://a.klaviyo.com/api/segments/${KLAVIYO_SEGMENT_UUID}/`;
  const res = await fetch(url, { method: "GET", headers: KLAVIYO_HEADERS });
  const json = await res.json();
  
  return json?.data?.attributes?.name || "Unknown Segment";
}

// 🛒 Step 2: Find Shopify customer ID by email
async function getShopifyCustomerIdByEmail(email) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-10/customers/search.json?query=email:"${encodeURIComponent(email)}"`;
  const res = await fetch(url, { method: "GET", headers: SHOPIFY_HEADERS });
  const json = await res.json();

  if (json.customers && json.customers.length > 0) {
    return json.customers[0];
  }

  return null;
}

// 📦 Step 3: Fetch most recent order from Shopify
async function getMostRecentOrder(customerId) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-10/orders.json?customer_id=${customerId}&status=any&limit=110000&order=created_at desc`;
  const res = await fetch(url, { method: "GET", headers: SHOPIFY_HEADERS });
  const json = await res.json();

  if (json.orders && json.orders.length > 0) {
    const order = json.orders[0];
    const lineItem = order.line_items[0];

    return {
      title: lineItem?.title || "N/A",
      sku: lineItem?.sku || "N/A",
      orderDate: order.created_at || "N/A"
    };
  }

  return null;
}

// 🔁 Step 4: Match profiles to Shopify customers and get order info
async function enrichProfilesWithShopifyOrders(profiles, segmentName) {
  const results = [];

  for (const profile of profiles) {
    const { email, profileId } = profile;

    if (!email) {
      console.warn(`❌ Skipping profile with no email: ${profileId}`);
      continue;
    }

    const customer = await getShopifyCustomerIdByEmail(email);
    if (!customer) {
      console.warn(`❌ No Shopify customer for email: ${email}`);
      continue;
    }

    const order = await getMostRecentOrder(customer.id);

    results.push({
      email,
      firstName: customer.first_name || "N/A",
      lastName: customer.last_name || "N/A",
      segmentName,
      productTitle: order?.title || "None",
      sku: order?.sku || "None",
      orderDate: order?.orderDate || "None"
    });
  }

  return results;
}

// 🚀 Run it all and write results to CSV
async function run() {
  const klaviyoProfiles = await fetchKlaviyoProfiles();
  if (klaviyoProfiles.length === 0) {
    console.log("❌ No Klaviyo profiles found.");
    return;
  }

  // Fetch segment name
  const segmentName = await fetchKlaviyoSegmentName();
  console.log(`Segment Name: ${segmentName}`);

  const enriched = await enrichProfilesWithShopifyOrders(klaviyoProfiles, segmentName);

  if (enriched.length === 0) {
    console.log("❌ No enriched data to write to CSV.");
    return;
  }

  // Define the CSV writer and output path
  const csvWriter = createCsvWriter({
    path: 'enriched_klaviyo_shopify_data.csv',
    header: [
      { id: "email", title: "Customer Email" },
      { id: "firstName", title: "First Name" },
      { id: "lastName", title: "Last Name" },
      { id: "segmentName", title: "Segment Name" },
      { id: "productTitle", title: "Most Recently Purchased" },
      { id: "sku", title: "Most Recent SKU" },
      { id: "orderDate", title: "Most Recent Order Date" },
    ]
  });

  console.log("📝 Writing enriched data to CSV...");
  await csvWriter.writeRecords(enriched);
  console.log(`✅ Saved to enriched_klaviyo_shopify_data.csv (${enriched.length} rows)`);
}

run().catch(console.error);
