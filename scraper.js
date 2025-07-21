const fs = require("fs");
const axios = require("axios");
const csv = require("csv-parser");
const path = require("path");

const locations = [];
const categories = [];

const OUTPUT_FILE = "output.csv";

// Step 1: Read CSV
function readCSV(filePath, targetArray) {
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on("data", (row) => targetArray.push(row))
            .on("end", resolve)
            .on("error", reject);
    });
}

// Step 2: Write CSV header once
function writeCSVHeaderIfNotExists() {
    if (!fs.existsSync(OUTPUT_FILE)) {
        const headers = [
            "date", "l1_category", "l1_category_id", "l2_category", "l2_category_id",
            "store_id", "variant_id", "variant_name", "group_id", "selling_price", "mrp",
            "in_stock", "inventory", "is_sponsored", "image_url", "brand_id", "brand"
        ];
        fs.writeFileSync(OUTPUT_FILE, headers.join(",") + "\n");
    }
}

// Step 3: Append one row to CSV
function appendToCSV(row) {
    const values = [
        row.date, row.l1_category, row.l1_category_id, row.l2_category, row.l2_category_id,
        row.store_id, row.variant_id, `"${row.variant_name.replace(/"/g, '""')}"`,
        row.group_id, row.selling_price, row.mrp, row.in_stock, row.inventory,
        row.is_sponsored, row.image_url, row.brand_id, row.brand
    ];
    fs.appendFileSync(OUTPUT_FILE, values.join(",") + "\n");
}

// Step 4: Send API request and parse
async function sendRequest(lat, lon, m1_cat, m2_cat, l1, l2) {
    const url = `https://blinkit.com/v1/layout/listing_widgets?m1_cat=${m1_cat}&m2_cat=${m2_cat}`;
    try {
        const response = await axios.post(
            url, {}, {
                headers: {
                    "lat": lat,
                    "lon": lon,
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                    "Cookie": "__cf_bm=...your values...; __cfruid=...; _cfuvid=...",
                },
                timeout: 10000,
            }
        );

        const dataWrapper = response.data;
        const snippets = dataWrapper && dataWrapper.response && Array.isArray(dataWrapper.response.snippets) ?
            dataWrapper.response.snippets : [];

        for (const item of snippets) {
            if (!item || !item.data) continue;
            const data = item.data;

            if (
                data.atc_action &&
                data.atc_action.add_to_cart &&
                data.atc_action.add_to_cart.cart_item
            ) {
                const cartItem = data.atc_action.add_to_cart.cart_item;

                const row = {
                    date: new Date().toISOString().split("T")[0],
                    l1_category: l1,
                    l1_category_id: m1_cat,
                    l2_category: l2,
                    l2_category_id: m2_cat,
                    store_id: cartItem.merchant_id || "",
                    variant_id: cartItem.product_id || "",
                    variant_name: cartItem.product_name || "",
                    group_id: cartItem.group_id || "",
                    selling_price: cartItem.price || "",
                    mrp: cartItem.mrp || "",
                    in_stock: cartItem.inventory > 0 ? 1 : 0,
                    inventory: cartItem.inventory || 0,
                    is_sponsored: item.tracking && item.tracking.impression_map && typeof item.tracking.impression_map.is_sponsored !== "undefined" ?
                        item.tracking.impression_map.is_sponsored : 0,
                    image_url: cartItem.image_url || "",
                    brand_id: cartItem.brand || "",
                    brand: cartItem.brand || "",
                };

                appendToCSV(row);
            }
        }

        console.log(`✅ Processed: ${l1} → ${l2} @ (${lat}, ${lon})`);
    } catch (err) {
        const errorStatus = err.response && err.response.status ? err.response.status : err.message;
        console.error(`❌ Failed: ${l1} → ${l2} @ (${lat}, ${lon}) →`, errorStatus);
    }
}

// Utility to sleep
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Step 5: Main
async function main() {
    await readCSV("blinkit_locations.csv", locations);
    await readCSV("blinkit_categories.csv", categories);

    writeCSVHeaderIfNotExists();

    for (const loc of locations) {
        const { latitude: lat, longitude: lon } = loc;

        for (const cat of categories) {
            const {
                l1_category: l1,
                l2_category: l2,
                l1_category_id: m1_cat,
                l2_category_id: m2_cat
            } = cat;

            await sendRequest(lat, lon, m1_cat, m2_cat, l1, l2);

            // Delay after each API call to avoid 429
            await sleep(1200); // Adjust to 500 or 1000 if still throttled
        }
    }
}


main();