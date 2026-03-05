import Imap from "imap"
import HtmlTblToJson from "html-table-to-json"
import cron from "node-cron"
import { Low } from "lowdb"
import { JSONFile } from "lowdb/node"
import path from "path"
import { fileURLToPath } from "url"
import { BigQuery } from "@google-cloud/bigquery"

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename)

const PROJECT_ID = "polished-bridge-398122";
const DATASET_ID = "NewSolvableFFPro";
const TABLE_ID = "bronze_everquote_revenue";

const imapConfig = {
    user: 'analytics@adquadrant.com',
    password: 'qrktnxrfinmxslxb',
    host: 'imap.gmail.com',
    port: 993,
    tls: true,
    tlsOptions: { rejectUnauthorized: false },
};
const imap = new Imap(imapConfig);
const adapter = new JSONFile("./storage.json");
const storage = new Low(adapter, { sentList: [] });

const bigquery = new BigQuery({
    projectId: PROJECT_ID,
    keyFilename: path.join(__dirname, "service-accounts.json")
});

const table = bigquery.dataset(DATASET_ID).table(TABLE_ID);

const getSentList = async () => {
    await storage.read();
    return storage.data.sentList;
}

const addToSentList = async (id) => {
    await storage.read();

    storage.data.sentList ||= [];
    if (!storage.data.sentList.includes(id)) {
        storage.data.sentList.push(id);
        await storage.write();
    }
}

const insertBatch = async (rows) => {
    if (!rows.length) return;
    try {
        await table.insert(rows);
        console.log(`Inserted ${rows.length} rows`);
    } catch (err) {
        console.error("BigQuery insert error:", err.errors || err);
    }
}

const getDataFromEmail = async (id) => {
    return new Promise((resolve, reject) => {
        const f = imap.fetch([id], { bodies: "", struct: true });

        f.on("message", (msg) => {
            let buffer = "";

            msg.on("body", (stream) => {
                stream.on("data", (chunk) => {
                    buffer += chunk.toString("utf8");
                });

                stream.once("end", () => {
                    try {
                        const tables =
                            HtmlTblToJson.parse(buffer)?.results?.flat() || [];

                        const rows = tables
                            .map((item) => {
                                return {
                                    Date: item.date, 
                                    SubID: item.subid,
                                    S2: item.uri_s2,
                                    Hour: item.hour_of_day,
                                    DeviceType: item.device_type,
                                    GeoState: item.acapi_geo_state_id,
                                    TrafficVertical: item.vertical,
                                    FormsU: Number(item.form_users || 0),
                                    CostA: Number(item.arriva || 0),
                                };
                            })
                            

                        resolve(rows);
                    } catch (e) {
                        reject(e);
                    }
                });
            })
        });

        f.once("error", reject);
        f.once("end", () => {
            console.log("Message Ended")
        });

    });
}

const checkGmailBox = (count) => {
    imap.once("ready", () => {
        imap.openBox("[Gmail]/All Mail", false, async (err) => {
            if (err) {
                return;
            }
            imap.search([
                "All",
                ["FROM", "goat-scheduler@everquote.com"],
                [
                    "HEADER",
                    "SUBJECT",
                    `GOAT Scheduled Report "AdQuadrant Daily Report"`
                ]
            ], async function (err, results) {
                if (!results || !results.length || err) {
                    console.log("⨉ No unread mails");
                    imap.end();
                    return;
                }
                const emailIndexes = results.slice(-count);

                const sentList = await getSentList() || [];

                for (const emailIndex of emailIndexes) {
                    if (sentList.includes(emailIndex)) {
                        continue;
                    }
                    const conversionData = await getDataFromEmail(emailIndex);
                    console.log("Index:", emailIndex);
                    for (let i = 0; i < conversionData.length; i = i + 100) {
                        await insertBatch(conversionData.slice(i, i + 100));
                    }
                    addToSentList(emailIndex);
                }
                imap.end();
            });
        });
    });
    imap.once("error", (err) => {
        console.log("⨉ Cannot fetch gmails!", err);
    });
    imap.connect();
}
const main = () => {
    const args = globalThis.process.argv.slice(2, globalThis.process.argv.length);
    const countIndex = args.findIndex(item => item.includes("--count="));
    console.log("Count index: >>", countIndex);
    if (countIndex == -1) {
        console.log("Please input count");
        return;
    }

    const count = args[countIndex].slice("--count=".length, args[countIndex].length);
    checkGmailBox(Number(count));
    if (args.includes("---cron")) {
        cron.schedule("0 0 */1 * * *", () => {
            checkGmailBox(Number(count))
        })
    }
}
main();