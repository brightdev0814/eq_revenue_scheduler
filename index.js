import Imap from "imap"
import { promises as fs } from "fs"
import cron from "node-cron"
import { Low } from "lowdb"
import { JSONFile } from "lowdb/node"
import os from "os"
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
let hasLoggedBigQueryIdentity = false;

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

const logBigQueryIdentity = async () => {
    try {
        const credentials = await bigquery.authClient.getCredentials();
        const projectId = await bigquery.authClient.getProjectId();

        console.log("BigQuery auth email:", credentials.client_email || "unknown");
        console.log("BigQuery auth project:", projectId);
        hasLoggedBigQueryIdentity = true;
    } catch (err) {
        console.error("Unable to read BigQuery auth identity:", err);
    }
}

const ensureBigQueryAccess = async () => {
    if (!hasLoggedBigQueryIdentity) {
        await logBigQueryIdentity();
    }

    await table.getMetadata();
}

const debugBigQueryAccess = async () => {
    try {
        await ensureBigQueryAccess();
        console.log(`BigQuery table access OK: ${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}`);
    } catch (err) {
        console.error("BigQuery table access check failed:", err);
        console.error("BigQuery response body:", err.response?.body || err.message);
        throw err;
    }
}

const getTempJsonPath = () => {
    return path.join(
        os.tmpdir(),
        `everquote-revenue-${Date.now()}-${Math.random().toString(36).slice(2)}.json`
    );
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const waitForBigQueryJob = async (job) => {
    while (true) {
        const [metadata] = await job.getMetadata();
        const jobStatus = metadata.status || {};

        if (jobStatus.errorResult) {
            throw new Error(jobStatus.errorResult.message || "BigQuery job failed");
        }

        if (jobStatus.state === "DONE") {
            return metadata;
        }

        await sleep(1000);
    }
}

const insertBatch = async (rows) => {
    if (!rows.length) return;

    try {
        const tempJsonPath = getTempJsonPath();

        await fs.writeFile(
            tempJsonPath,
            rows.map((row) => JSON.stringify(row)).join("\n"),
            "utf8"
        );

        try {
            const [job] = await table.load(tempJsonPath, {
                sourceFormat: "NEWLINE_DELIMITED_JSON",
                writeDisposition: "WRITE_APPEND",
            });

            await waitForBigQueryJob(job);
            console.log(`Inserted ${rows.length} rows with load job ${job.id}`);
        } finally {
            await fs.unlink(tempJsonPath).catch(() => { });
        }
    } catch (err) {
        if (!hasLoggedBigQueryIdentity) {
            await logBigQueryIdentity();
        }

        console.error("BigQuery insert error:", err);
        console.error("BigQuery response body:", err.response?.body || err.message);
        throw err;
    }
}

const parseMimeHeaders = (headerText) => {
    const headers = {};
    const foldedLines = headerText.replace(/\n[ \t]+/g, " ");

    for (const line of foldedLines.split("\n")) {
        const separatorIndex = line.indexOf(":");
        if (separatorIndex === -1) {
            continue;
        }

        const key = line.slice(0, separatorIndex).trim().toLowerCase();
        const value = line.slice(separatorIndex + 1).trim();
        headers[key] = value;
    }

    return headers;
}

const getMimeParameter = (headerValue = "", parameterName) => {
    const match = headerValue.match(new RegExp(`${parameterName}\\*?=(?:"([^"]+)"|([^;\\s]+))`, "i"));
    return match?.[1] || match?.[2] || "";
}

const decodeQuotedPrintable = (content) => {
    return content
        .replace(/=\n/g, "")
        .replace(/=([A-Fa-f0-9]{2})/g, (_, hex) =>
            String.fromCharCode(Number.parseInt(hex, 16))
        );
}

const decodeMimeBody = (body, encoding = "") => {
    const normalizedEncoding = encoding.toLowerCase();

    if (normalizedEncoding.includes("base64")) {
        return Buffer.from(body.replace(/\s/g, ""), "base64").toString("utf8");
    }

    if (normalizedEncoding.includes("quoted-printable")) {
        return decodeQuotedPrintable(body);
    }

    return body;
}

const splitMimeEntity = (entity) => {
    const normalizedEntity = entity.replace(/\r\n/g, "\n");
    const separator = normalizedEntity.indexOf("\n\n");

    if (separator === -1) {
        return {
            headers: {},
            body: normalizedEntity,
        };
    }

    return {
        headers: parseMimeHeaders(normalizedEntity.slice(0, separator)),
        body: normalizedEntity.slice(separator + 2),
    };
}

const splitMultipartBody = (body, boundary) => {
    const parts = [];
    const marker = `--${boundary}`;
    const normalizedBody = body.replace(/\r\n/g, "\n");

    for (const section of normalizedBody.split(marker).slice(1)) {
        const trimmedSection = section.trim();

        if (!trimmedSection || trimmedSection === "--") {
            continue;
        }

        parts.push(trimmedSection.endsWith("--")
            ? trimmedSection.slice(0, -2).trim()
            : trimmedSection);
    }

    return parts;
}

const extractAttachmentContent = (entity, expectedFilename) => {
    const { headers, body } = splitMimeEntity(entity);
    const contentType = headers["content-type"] || "";
    const boundary = getMimeParameter(contentType, "boundary");

    if (contentType.toLowerCase().includes("multipart/") && boundary) {
        for (const part of splitMultipartBody(body, boundary)) {
            const content = extractAttachmentContent(part, expectedFilename);
            if (content) {
                return content;
            }
        }

        return "";
    }

    const contentDisposition = headers["content-disposition"] || "";
    const attachmentName =
        getMimeParameter(contentDisposition, "filename") ||
        getMimeParameter(contentType, "name");

    if (attachmentName.toLowerCase() !== expectedFilename.toLowerCase()) {
        return "";
    }

    return decodeMimeBody(body, headers["content-transfer-encoding"]);
}

const parseCsvRows = (csvContent) => {
    const rows = [];
    let currentCell = "";
    let currentRow = [];
    let isInsideQuotes = false;
    const normalizedCsv = csvContent.replace(/^\uFEFF/, "").replace(/\r\n/g, "\n");

    for (let index = 0; index < normalizedCsv.length; index++) {
        const character = normalizedCsv[index];
        const nextCharacter = normalizedCsv[index + 1];

        if (character === "\"") {
            if (isInsideQuotes && nextCharacter === "\"") {
                currentCell += "\"";
                index += 1;
            } else {
                isInsideQuotes = !isInsideQuotes;
            }
            continue;
        }

        if (character === "," && !isInsideQuotes) {
            currentRow.push(currentCell);
            currentCell = "";
            continue;
        }

        if (character === "\n" && !isInsideQuotes) {
            currentRow.push(currentCell);
            if (currentRow.some((value) => value.trim() !== "")) {
                rows.push(currentRow);
            }
            currentCell = "";
            currentRow = [];
            continue;
        }

        currentCell += character;
    }

    if (currentCell.length || currentRow.length) {
        currentRow.push(currentCell);
        if (currentRow.some((value) => value.trim() !== "")) {
            rows.push(currentRow);
        }
    }

    return rows;
}

const normalizeHeader = (value = "") => {
    return value.toLowerCase().replace(/[^a-z0-9]/g, "");
}

const parseNumberValue = (value = "") => {
    const normalizedValue = String(value)
        .trim()
        .replace(/[$,%\s]/g, "")
        .replace(/,/g, "");

    if (!normalizedValue) {
        return 0;
    }

    const parsedValue = Number(normalizedValue);
    return Number.isFinite(parsedValue) ? parsedValue : 0;
}

const mapCsvRowsToReportRows = (csvContent) => {
    const [headerRow = [], ...records] = parseCsvRows(csvContent);
    const headerIndex = new Map(
        headerRow.map((header, index) => [normalizeHeader(header), index])
    );

    const getValue = (record, ...possibleHeaders) => {
        for (const header of possibleHeaders) {
            const valueIndex = headerIndex.get(normalizeHeader(header));
            if (valueIndex !== undefined) {
                return record[valueIndex] || "";
            }
        }

        return "";
    }

    return records.map((record) => ({
        Date: getValue(record, "date_est"),
        SubID: getValue(record, "subid"),
        S2: getValue(record, "s2"),
        Hour: getValue(record, "hour_of_day_est"),
        DeviceType: getValue(record, "device_type"),
        GeoState: getValue(record, "state"),
        TrafficVertical: getValue(record, "vertical"),
        FormsU: parseNumberValue(getValue(record, "Forms")),
        CostA: parseNumberValue(getValue(record, "Partner Revenue")),
    })).filter((row) =>
        row.Date ||
        row.SubID ||
        row.S2 ||
        row.Hour ||
        row.DeviceType ||
        row.GeoState ||
        row.TrafficVertical ||
        row.FormsU ||
        row.CostA
    );
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
                        const csvContent = extractAttachmentContent(
                            buffer,
                            "AdQuadrant_Daily_Report.csv"
                        );

                        if (!csvContent) {
                            throw new Error("Attachment AdQuadrant_Daily_Report.csv not found");
                        }

                        const rows = mapCsvRowsToReportRows(csvContent);
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
            try {
                await ensureBigQueryAccess();
            } catch (accessError) {
                console.error("Stopping email processing because BigQuery access failed.");
                imap.end();
                return;
            }
            imap.search([
                "All",
                ["FROM", "noreply@adverity.com"],
                [
                    "HEADER",
                    "SUBJECT",
                    "AdQuadrant Daily Report"
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
                    for (let i = 0; i < conversionData.length; i = i + 100) {
                        await insertBatch(conversionData.slice(i, i + 100));
                    }
                    await addToSentList(emailIndex);
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
const main = async () => {
    const args = globalThis.process.argv.slice(2, globalThis.process.argv.length);
    const countIndex = args.findIndex(item => item.includes("--count="));
    if (args.includes("--debug-bq-auth")) {
        await debugBigQueryAccess();
        if (countIndex === -1) {
            return;
        }
    }
    console.log("Count index: >>", countIndex);
    if (countIndex == -1) {
        console.log("Please input count");
        return;
    }

    const count = args[countIndex].slice("--count=".length, args[countIndex].length);
    checkGmailBox(Number(count));
    if (args.includes("--cron")) {
        cron.schedule("0 0 */1 * * *", () => {
            checkGmailBox(Number(count))
        })
    }
}
main().catch((err) => {
    console.error("Fatal error:", err);
    process.exitCode = 1;
});