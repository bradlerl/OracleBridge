require("dotenv").config();

const express = require("express");
const oracledb = require("oracledb");
const { MongoClient } = require("mongodb");
const crypto = require("crypto");
const bcrypt = require("bcryptjs");

const app = express();

app.use(express.json({ limit: "1mb" }));

// CONFIGURATION

const PORT = Number(process.env.PORT || 4000);

const requiredEnvironmentVariables = [
    "ORACLE_USER",
    "ORACLE_PASSWORD",
    "ORACLE_CONNECT_STRING",
    "MONGODB_URI",
    "MONGODB_DATABASE",
    "MONGODB_REVIEWS_COLLECTION",
    "MONGODB_ANALYSIS_COLLECTION",
    "API_SECRET"
];

for (const variable of requiredEnvironmentVariables) {
    if (!process.env[variable]) {
        console.error(`Missing environment variable: ${variable}`);
        process.exit(1);
    }
}

const API_SECRET = process.env.API_SECRET;

const oracleConfig = {
    user: process.env.ORACLE_USER,
    password: process.env.ORACLE_PASSWORD,
    connectString: process.env.ORACLE_CONNECT_STRING
};

const mongoClient = new MongoClient(process.env.MONGODB_URI);

const mongoDatabaseName = process.env.MONGODB_DATABASE;

let oraclePool;
let reviewsCollection;
let analysisCollection;

// AUTHENTICATION

function authenticate(req, res, next) {
    const authorization = req.headers.authorization || "";

    if (!authorization.startsWith("Bearer ")) {
        return res.status(401).json({
            error: "Authorization token required"
        });
    }

    const suppliedToken = authorization.slice(7);

    const expected = Buffer.from(API_SECRET);
    const supplied = Buffer.from(suppliedToken);

    if (expected.length !== supplied.length || !crypto.timingSafeEqual(expected, supplied)) {
        return res.status(401).json({
            error: "Invalid authorization token"
        });
    }

    next();
}

// VALIDATION HELPERS

function parsePositiveInteger(value) {
    const parsed = Number(value);

    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
        return null;
    }

    return parsed;
}

function parsePagination(value, defaultValue, maximum) {
    if (value === undefined || value === null || value === "") {
        return defaultValue;
    }

    const parsed = Number(value);

    if (!Number.isSafeInteger(parsed) || parsed < 0) {
        return null;
    }

    return Math.min(parsed, maximum);
}

function isValidText(value, maximumLength) {
    return typeof value === "string" && value.trim().length > 0 && value.trim().length <= maximumLength;
}

function isValidPolarity(value) {
    return typeof value === "number" && Number.isFinite(value) && value >= -1 && value <= 1;
}

function sendDatabaseError(res, error, message) {
    console.error(message, error);

    // In case of any foreign key violations
    if (error.errorNum === 2292) {
        return res.status(409).json({
            error: "This record cannot be deleted because it is still being used."
        });
    }

    if (error.errorNum === 1) {
        return res.status(409).json({
            error: "A record with that unique value already exists."
        });
    }

    return res.status(500).json({
        error: message
    });
}

// ORACLE DATABASE HELPERS

async function executeQuery(sql, binds = {}) {
    let connection;

    try {
        connection = await oraclePool.getConnection();

        const result = await connection.execute(sql, binds, {
            outFormat: oracledb.OUT_FORMAT_OBJECT
        });

        return result.rows;
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

async function executeDML(sql, binds = {}) {
    let connection;

    try {
        connection = await oraclePool.getConnection();

        const result = await connection.execute(sql, binds, {
            autoCommit: true
        });

        return result.rowsAffected;
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

async function generateTableId(tableName, columnName, sequenceCandidates) {
    const sequences = await executeQuery(`SELECT SEQUENCE_NAME FROM USER_SEQUENCES`);

    const availableSequences = sequences.map(row => row.SEQUENCE_NAME.toUpperCase());

    const sequenceName = sequenceCandidates.find(candidate => availableSequences.includes(candidate.toUpperCase()));

    if (sequenceName) {
        const rows = await executeQuery(`SELECT ${sequenceName}.NEXTVAL AS "id" FROM DUAL`);

        return Number(rows[0].id);
    }

    console.warn(`No matching sequence found for ${tableName}. Using MAX(ID) + 1. Configure a sequence before production use.`);

    const rows = await executeQuery(`SELECT NVL(MAX(${columnName}), 0) + 1 AS "id" FROM ${tableName}`);

    return Number(rows[0].id);
}

// MONGODB HOTEL ANALYSIS HELPER

async function getHotelAnalysis(hotelId) {
    const results = await analysisCollection.aggregate([
        {
            $match: {
                hotelId: hotelId
            }
        },
        {
            $facet: {
                metadata: [
                    {
                        $count: "reviewCount"
                    }
                ],
                featureRatings: [
                    {
                        $unwind: "$featureResults"
                    },
                    {
                        $match: {
                            "featureResults.feature": {
                                $type: "string"
                            },
                            "featureResults.sentimentScore": {
                                $type: "number"
                            }
                        }
                    },
                    {
                        $group: {
                            _id: "$featureResults.feature",
                            averageSentiment: {
                                $avg: "$featureResults.sentimentScore"
                            },
                            reviewCount: {
                                $sum: 1
                            }
                        }
                    },
                    {
                        $sort: {
                            _id: 1
                        }
                    }
                ]
            }
        }
    ]).toArray();

    const result = results[0] || {
        metadata: [],
        featureRatings: []
    };

    const reviewCount = result.metadata.length ? result.metadata[0].reviewCount : 0;

    const ratings = result.featureRatings.map(item => {
        const calculatedRating = 3 + (2 * item.averageSentiment);

        const featureRating = Math.max(1, Math.min(5, calculatedRating));

        return {
            feature: item._id,
            sentimentScore: Number(item.averageSentiment.toFixed(4)),
            featureRating: Number(featureRating.toFixed(2)),
            reviewCount: item.reviewCount
        };
    });

    let overallRating = null;

    if (ratings.length > 0) {
        const totalRating = ratings.reduce((total, item) => total + item.featureRating, 0);

        overallRating = Number((totalRating / ratings.length).toFixed(2));
    }

    return {
        overallRating,
        reviewCount,
        ratings
    };
}

// PUBLIC ROOT AND HEALTH CHECK

app.get("/", (req, res) => {
    res.json({
        name: "Hotel Review API",
        status: "running",
        healthEndpoint: "/health"
    });
});

app.get("/health", async (req, res) => {
    try {
        await executeQuery("SELECT 1 AS DATABASE_STATUS FROM DUAL");

        await mongoClient.db(mongoDatabaseName).command({
            ping: 1
        });

        res.json({
            status: "online",
            oracle: "connected",
            mongodb: "connected"
        });
    } catch (error) {
        console.error("Health check failed:", error.message);

        res.status(503).json({
            status: "offline",
            error: "One or more database connections failed"
        });
    }
});

// All API routes below this point require the shared API secret.
app.use("/api", authenticate);

// AUTHENTICATION ROUTES

app.post("/api/auth/login", async (req, res) => {
    const username = typeof req.body.username === "string" ? req.body.username.trim() : "";
    const password = typeof req.body.password === "string" ? req.body.password : "";

    if (!username || !password) {
        return res.status(400).json({
            error: "Username and password are required"
        });
    }

    try {
        const rows = await executeQuery(
            `SELECT
                USERID AS "userId",
                USERNAME AS "username",
                PASSWORDHASH AS "passwordHash",
                ROLE AS "role"
            FROM USER_ACCOUNT
            WHERE UPPER(USERNAME) = UPPER(:username)`,
            { username }
        );

        if (rows.length === 0) {
            return res.status(401).json({
                error: "Invalid username or password"
            });
        }

        const account = rows[0];

        let passwordMatches = false;

        try {
            passwordMatches = await bcrypt.compare(password, account.passwordHash);
        } catch {
            passwordMatches = false;
        }

        if (!passwordMatches) {
            return res.status(401).json({
                error: "Invalid username or password"
            });
        }

        const role = String(account.role).trim().toUpperCase();

        if (!["EMPLOYEE", "ADMIN"].includes(role)) {
            return res.status(403).json({
                error: "This account does not have an authorized role"
            });
        }

        res.json({
            user: {
                userId: account.userId,
                username: account.username,
                role
            }
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to authenticate user");
    }
});

// CITY ROUTES

app.get("/api/cities", async (req, res) => {
    try {
        const rows = await executeQuery(
            `SELECT
                CITYID AS "cityId", 
                CITYNAME AS "cityName",
                STATE AS "state",
                COUNTRY AS "country"
            FROM CITY
            ORDER BY CITYNAME`
        );

        res.json({
            count: rows.length,
            cities: rows
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to retrieve cities");
    }
});

// HOTEL ROUTES

app.get("/api/hotels", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim();
        const cityId = req.query.cityId ? parsePositiveInteger(req.query.cityId) : null;

        if (req.query.cityId && cityId === null) {
            return res.status(400).json({
                error: "Invalid cityId"
            });
        }

        const limit = parsePagination(req.query.limit, 25, 100);
        const offset = parsePagination(req.query.offset, 0, 1000000);

        if (limit === null || offset === null) {
            return res.status(400).json({
                error: "Invalid pagination parameters"
            });
        }

        const conditions = [];

        const binds = {
            limit,
            offset
        };

        if (search) {
            conditions.push("UPPER(H.HOTELNAME) LIKE UPPER(:search)");
            binds.search = `%${search}%`;
        }

        if (cityId !== null) {
            conditions.push("H.CITYID = :cityId");
            binds.cityId = cityId;
        }

        const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

        const rows = await executeQuery(
            `SELECT
                H.HOTELID AS "hotelId",
                H.HOTELNAME AS "hotelName",
                C.CITYID AS "cityId",
                C.CITYNAME AS "cityName",
                C.STATE AS "state",
                C.COUNTRY AS "country"
            FROM HOTEL H
            JOIN CITY C
                ON H.CITYID = C.CITYID
            ${whereClause}
            ORDER BY H.HOTELNAME
            OFFSET :offset ROWS
            FETCH NEXT :limit ROWS ONLY`,
            binds
        );

        res.json({
            count: rows.length,
            limit,
            offset,
            hotels: rows
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to search hotels");
    }
});

app.get("/api/hotels/:hotelId", async (req, res) => {
    const hotelId = parsePositiveInteger(req.params.hotelId);

    if (hotelId === null) {
        return res.status(400).json({
            error: "Invalid hotel ID"
        });
    }

    try {
        const hotelRows = await executeQuery(
            `SELECT
                H.HOTELID AS "hotelId",
                H.HOTELNAME AS "hotelName",
                C.CITYID AS "cityId",
                C.CITYNAME AS "cityName",
                C.STATE AS "state",
                C.COUNTRY AS "country"
            FROM HOTEL H
            JOIN CITY C
                ON H.CITYID = C.CITYID
            WHERE H.HOTELID = :hotelId`,
            { hotelId }
        );

        if (hotelRows.length === 0) {
            return res.status(404).json({
                error: "Hotel not found"
            });
        }

        const analysis = await getHotelAnalysis(hotelId);

        res.json({
            hotel: hotelRows[0],
            overallRating: analysis.overallRating,
            reviewCount: analysis.reviewCount,
            ratings: analysis.ratings
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to retrieve hotel details");
    }
});

// HOTEL ADMINISTRATION

app.post("/api/admin/hotels", async (req, res) => {
    const hotelName = req.body.hotelName;
    const cityId = parsePositiveInteger(req.body.cityId);

    if (!isValidText(hotelName, 500) || cityId === null) {
        return res.status(400).json({
            error: "A valid hotelName and cityId are required"
        });
    }

    try {
        const hotelId = await generateTableId("HOTEL", "HOTELID", ["HOTEL_SEQ", "HOTELID_SEQ", "HOTEL_SEQUENCE"]);
        
        await executeDML(`INSERT INTO HOTEL (HOTELID, HOTELNAME, CITYID) VALUES (:hotelId, :hotelName, :cityId)`, {
            hotelId,
            hotelName: hotelName.trim(),
            cityId
        });

        res.status(201).json({
            message: "Hotel created successfully",
            hotel: {
                hotelId,
                hotelName: hotelName.trim(),
                cityId
            }
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to create hotel");
    }
});

app.put("/api/admin/hotels/:hotelId", async (req, res) => {
    const hotelId = parsePositiveInteger(req.params.hotelId);

    if (hotelId === null) {
        return res.status(400).json({
            error: "Invalid hotel ID"
        });
    }

    const updates = [];
    const binds = { hotelId };

    if (req.body.hotelName !== undefined) {
        if (!isValidText(req.body.hotelName, 500)) {
            return res.status(400).json({
                error: "Invalid hotelName"
            });
        }

        updates.push("HOTELNAME = :hotelName");
        binds.hotelName = req.body.hotelName.trim();
    }

    if (req.body.cityId !== undefined) {
        const cityId = parsePositiveInteger(req.body.cityId);

        if (cityId === null) {
            return res.status(400).json({
                error: "Invalid cityId"
            });
        }

        updates.push("CITYID = :cityId");
        binds.cityId = cityId;
    }

    if (updates.length === 0) {
        return res.status(400).json({
            error: "No valid fields supplied for update"
        });
    }

    try {
        const rowsAffected = await executeDML(`UPDATE HOTEL SET ${updates.join(", ")} WHERE HOTELID = :hotelId`, binds);

        if (rowsAffected === 0) {
            return res.status(404).json({
                error: "Hotel not found"
            });
        }

        res.json({
            message: "Hotel updated successfully"
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to update hotel");
    }
});

app.delete("/api/admin/hotels/:hotelId", async (req, res) => {
    const hotelId = parsePositiveInteger(req.params.hotelId);

    if (hotelId === null) {
        return res.status(400).json({
            error: "Invalid hotel ID"
        });
    }

    try {
        const rowsAffected = await executeDML(`DELETE FROM HOTEL WHERE HOTELID = :hotelId`, { hotelId });

        if (rowsAffected === 0) {
            return res.status(404).json({
                error: "Hotel not found"
            });
        }

        res.json({
            message: "Hotel deleted successfully"
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to delete hotel");
    }
});

// FEATURE ROUTES

app.get("/api/features", async (req, res) => {
    try {
        const features = await executeQuery(
            `SELECT
                FEATUREID AS "featureId",
                FEATURENAME AS "featureName",
                FEATUREDESC AS "featureDesc"
            FROM FEATURE
            ORDER BY FEATURENAME`
        );

        res.json({
            count: features.length,
            features
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to retrieve features");
    }
});

app.post("/api/admin/features", async (req, res) => {
    const featureName = req.body.featureName;
    const featureDesc = req.body.featureDesc ?? null;

    if (!isValidText(featureName, 100)) {
        return res.status(400).json({
            error: "A valid featureName is required"
        });
    }

    if (featureDesc !== null && (typeof featureDesc !== "string" || featureDesc.length > 500)) {
        return res.status(400).json({
            error: "featureDesc must be 500 characters or fewer"
        });
    }

    try {
        const featureId = await generateTableId("FEATURE", "FEATUREID", ["FEATURE_SEQ", "FEATUREID_SEQ", "FEATURE_SEQUENCE"]);

        await executeDML(`INSERT INTO FEATURE (FEATUREID, FEATURENAME, FEATUREDESC) VALUES (:featureId, :featureName, :featureDesc)`, {
            featureId,
            featureName: featureName.trim(),
            featureDesc
        });

        res.status(201).json({
            message: "Feature created successfully",
            feature: {
                featureId,
                featureName: featureName.trim(),
                featureDesc
            }
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to create feature");
    }
});

app.put("/api/admin/features/:featureId", async (req, res) => {
    const featureId = parsePositiveInteger(req.params.featureId);

    if (featureId === null) {
        return res.status(400).json({
            error: "Invalid feature ID"
        });
    }

    const updates = [];
    const binds = { featureId };

    if (req.body.featureName !== undefined) {
        if (!isValidText(req.body.featureName, 100)) {
            return res.status(400).json({
                error: "Invalid featureName"
            });
        }

        updates.push("FEATURENAME = :featureName");
        binds.featureName = req.body.featureName.trim();
    }

    if (req.body.featureDesc !== undefined) {
        if (req.body.featureDesc !== null && (typeof req.body.featureDesc !== "string" || req.body.featureDesc.length > 500)) {
            return res.status(400).json({
                error: "Invalid featureDesc"
            });
        }

        updates.push("FEATUREDESC = :featureDesc");
        binds.featureDesc = req.body.featureDesc;
    }

    if (updates.length === 0) {
        return res.status(400).json({
            error: "No valid fields supplied for update"
        });
    }

    try {
        const rowsAffected = await executeDML(`UPDATE FEATURE SET ${updates.join(", ")} WHERE FEATUREID = :featureId`, binds);

        if (rowsAffected === 0) {
            return res.status(404).json({
                error: "Feature not found"
            });
        }

        res.json({
            message: "Feature updated successfully"
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to update feature");
    }
});

app.delete("/api/admin/features/:featureId", async (req, res) => {
    const featureId = parsePositiveInteger(req.params.featureId);

    if (featureId === null) {
        return res.status(400).json({
            error: "Invalid feature ID"
        });
    }

    try {
        const rowsAffected = await executeDML(`DELETE FROM FEATURE WHERE FEATUREID = :featureId`, { featureId });

        if (rowsAffected === 0) {
            return res.status(404).json({
                error: "Feature not found"
            });
        }

        res.json({
            message: "Feature deleted successfully"
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to delete feature");
    }
});

// SEED WORD ROUTES

app.get("/api/seed-words", async (req, res) => {
    try {
        const seedWords = await executeQuery(
            `SELECT
                SW.SEEDWORDID AS "seedWordId",
                SW.FEATUREID AS "featureId",
                F.FEATURENAME AS "featureName",
                SW.SEEDTEXT AS "seedText",
                SW.POLARITY AS "polarity"
            FROM SEED_WORD SW
            JOIN FEATURE F
                ON SW.FEATUREID = F.FEATUREID
            ORDER BY F.FEATURENAME, SW.SEEDTEXT`
        );

        res.json({
            count: seedWords.length,
            seedWords
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to retrieve seed words");
    }
});

app.post("/api/admin/seed-words", async (req, res) => {
    const featureId = parsePositiveInteger(req.body.featureId);
    const seedText = req.body.seedText;
    const polarity = req.body.polarity;

    if (featureId === null || !isValidText(seedText, 500) || !isValidPolarity(polarity)) {
        return res.status(400).json({
            error: "A valid featureId, seedText and polarity are required"
        });
    }

    try {
        const seedWordId = await generateTableId("SEED_WORD", "SEEDWORDID", ["SEED_WORD_SEQ", "SEEDWORD_SEQ", "SEEDWORDID_SEQ", "SEED_WORD_SEQUENCE"]);

        await executeDML(`INSERT INTO SEED_WORD (SEEDWORDID, FEATUREID, SEEDTEXT, POLARITY) VALUES (:seedWordId, :featureId, :seedText, :polarity)`, {
            seedWordId,
            featureId,
            seedText: seedText.trim(),
            polarity
        });

        res.status(201).json({
            message: "Seed word created successfully",
            seedWord: {
                seedWordId,
                featureId,
                seedText: seedText.trim(),
                polarity
            }
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to create seed word");
    }
});

app.put("/api/admin/seed-words/:seedWordId", async (req, res) => {
    const seedWordId = parsePositiveInteger(req.params.seedWordId);

    if (seedWordId === null) {
        return res.status(400).json({
            error: "Invalid seed word ID"
        });
    }

    const updates = [];
    const binds = { seedWordId };

    if (req.body.featureId !== undefined) {
        const featureId = parsePositiveInteger(req.body.featureId);

        if (featureId === null) {
            return res.status(400).json({
                error: "Invalid featureId"
            });
        }

        updates.push("FEATUREID = :featureId");
        binds.featureId = featureId;
    }

    if (req.body.seedText !== undefined) {
        if (!isValidText(req.body.seedText, 500)) {
            return res.status(400).json({
                error: "Invalid seedText"
            });
        }

        updates.push("SEEDTEXT = :seedText");
        binds.seedText = req.body.seedText.trim();
    }

    if (req.body.polarity !== undefined) {
        if (!isValidPolarity(req.body.polarity)) {
            return res.status(400).json({
                error: "Invalid polarity. It must be between -1 and 1."
            });
        }

        updates.push("POLARITY = :polarity");
        binds.polarity = req.body.polarity;
    }

    if (updates.length === 0) {
        return res.status(400).json({
            error: "No valid fields supplied for update"
        });
    }

    try {
        const rowsAffected = await executeDML(`UPDATE SEED_WORD SET ${updates.join(", ")} WHERE SEEDWORDID = :seedWordId`, binds);

        if (rowsAffected === 0) {
            return res.status(404).json({
                error: "Seed word not found"
            });
        }

        res.json({
            message: "Seed word updated successfully"
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to update seed word");
    }
});

app.delete("/api/admin/seed-words/:seedWordId", async (req, res) => {
    const seedWordId = parsePositiveInteger(req.params.seedWordId);

    if (seedWordId === null) {
        return res.status(400).json({
            error: "Invalid seed word ID"
        });
    }

    try {
        const rowsAffected = await executeDML(`DELETE FROM SEED_WORD WHERE SEEDWORDID = :seedWordId`, { seedWordId });

        if (rowsAffected === 0) {
            return res.status(404).json({
                error: "Seed word not found"
            });
        }

        res.json({
            message: "Seed word deleted successfully"
        });
    } catch (error) {
        sendDatabaseError(res, error, "Unable to delete seed word");
    }
});

// HOTEL REVIEW ROUTES

app.get("/api/hotels/:hotelId/reviews", async (req, res) => {
    const hotelId = parsePositiveInteger(req.params.hotelId);

    if (hotelId === null) {
        return res.status(400).json({
            error: "Invalid hotel ID"
        });
    }

    const limit = parsePagination(req.query.limit, 25, 100);
    const offset = parsePagination(req.query.offset, 0, 1000000);

    if (limit === null || offset === null) {
        return res.status(400).json({
            error: "Invalid pagination parameters"
        });
    }

    try {
        const [reviews, total] = await Promise.all([
            reviewsCollection
                .find({ hotelId })
                .sort({ reviewDate: -1 })
                .skip(offset)
                .limit(limit)
                .toArray(),
            reviewsCollection.countDocuments({ hotelId })
        ]);

        res.json({
            hotelId,
            totalReviews: total,
            limit,
            offset,
            reviews
        });
    } catch (error) {
        console.error("Error retrieving reviews:", error);

        res.status(500).json({
            error: "Unable to retrieve hotel reviews"
        });
    }
});

// 404 AND ERROR HANDLING

app.use((req, res) => {
    res.status(404).json({
        error: "Endpoint not found"
    });
});

app.use((error, req, res, next) => {
    console.error("Unhandled API error:", error);

    if (res.headersSent) {
        return next(error);
    }

    res.status(500).json({
        error: "An unexpected server error occurred"
    });
});

// START SERVER

async function startServer() {
    try {
        // Connect to Oracle.
        oraclePool = await oracledb.createPool({
            ...oracleConfig,
            poolMin: 1,
            poolMax: 5,
            poolIncrement: 1,
            poolTimeout: 60
        });

        console.log("Connected to Oracle connection pool.");

        // Connect to MongoDB Atlas.
        await mongoClient.connect();

        const mongoDatabase = mongoClient.db(mongoDatabaseName);

        reviewsCollection = mongoDatabase.collection(process.env.MONGODB_REVIEWS_COLLECTION);
        analysisCollection = mongoDatabase.collection(process.env.MONGODB_ANALYSIS_COLLECTION);

        await mongoDatabase.command({
            ping: 1
        });

        console.log("Connected to MongoDB Atlas.");
        console.log("Connected to reviews collection.");
        console.log("Connected to review_analysis collection.");

        // Bind locally. Cloudflare Tunnel forwards traffic here.
        app.listen(PORT, "127.0.0.1", () => {
            console.log(`Hotel Review API running at http://127.0.0.1:${PORT}`);
        });
    } catch (error) {
        console.error("Could not start API:", error);

        if (oraclePool) {
            await oraclePool.close(10).catch(() => {});
        }

        await mongoClient.close().catch(() => {});

        process.exit(1);
    }
}

// SHUTDOWN

let isShuttingDown = false;

async function shutDown() {
    if (isShuttingDown) {
        return;
    }

    isShuttingDown = true;

    console.log("Shutting down API...");

    try {
        if (oraclePool) {
            await oraclePool.close(10);
        }

        await mongoClient.close();

        console.log("Database connections closed.");
        process.exit(0);
    } catch (error) {
        console.error("Error during shutdown:", error);
        process.exit(1);
    }
}

process.on("SIGINT", shutDown);
process.on("SIGTERM", shutDown);

startServer();