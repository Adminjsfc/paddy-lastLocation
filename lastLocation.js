require("dotenv").config({ path: require("node:path").join(__dirname, ".env"), quiet: true });

const mqtt = require("mqtt");

const Redis = require("ioredis");
const db = require("./Database");
const { fetchPaddyVehicles, updatePaddyLocations } = require("./paddyLocations.repository");
// ================= CONFIG =================

const MQTT_URL = process.env.MQTT_URL || "ws://18.60.155.145:20012";

const SUBSCRIPTION_REFRESH_MS = 3 * 60 * 1000; // 3 minutes
const DB_BATCH_UPDATE_MS = 60 * 1000; // 1 minute

// Redis Channel Name (matches backend redis.config)
const VEHICLE_STATUS_CHANNEL ="jsfc:vehicle:status";


// ================= REDIS CLIENT =================

const redisPublisher = process.env.REDIS_HOST ? new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
  password: process.env.REDIS_PASSWORD || undefined,
  lazyConnect: false,
}) : null;

redisPublisher?.on("connect", () => console.log("[REDIS] Connected successfully"));
redisPublisher?.on("error", (err) => console.error("[REDIS] Error:", err.message));

// ================= MYSQL READ POOL =================

const pool = db.pool;

// ================= MEMORY STORAGE =================

// Active mapped vehicles from DB
// vehicle_no => imei_no
const activeVehicleMap = new Map();

// Currently subscribed topics
const subscribedTopics = new Set();

// Latest received location data
// vehicle_no => latest packet object
const latestLocationMap = new Map();

// ================= HELPER FUNCTIONS =================

function normalizeVehicleNo(vehicleNo) {
  return String(vehicleNo || "")
    .trim()
    .toUpperCase();
}

function topicForVehicle(vehicleNo) {
  return `gps/${vehicleNo}/data`;
}

function getVehicleNoFromTopic(topic) {
  const match = topic.match(/^gps\/(.+)\/data$/);
  return match ? normalizeVehicleNo(match[1]) : null;
}

function isValidLatLng(lat, lng) {
  const latitude = Number(lat);
  const longitude = Number(lng);

  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    latitude >= -90 &&
    latitude <= 90 &&
    longitude >= -180 &&
    longitude <= 180
  );
}

function formatISTDateTime(dateInput = new Date()) {
  const date = new Date(dateInput);

  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const obj = {};
  for (const part of parts) {
    obj[part.type] = part.value;
  }

  return `${obj.year}-${obj.month}-${obj.day} ${obj.hour}:${obj.minute}:${obj.second}`;
}

function getPacketUpdateTimeIST(packet) {
  if (packet.server_date_time && !Number.isNaN(Number(packet.server_date_time))) {
    return formatISTDateTime(Number(packet.server_date_time));
  }

  return formatISTDateTime(new Date());
}

// ================= DB: LOAD VEHICLE MAPPING =================

// ================= MQTT SUBSCRIPTION REFRESH =================

async function refreshSubscriptions(client) {
  try {
    const rows = await fetchPaddyVehicles(pool);

    const newVehicleMap = new Map();

    for (const row of rows) {
      const vehicleNo = normalizeVehicleNo(row.vehicle_no);
      const imeiNo = String(row.imei_no || "").trim();

      if (!vehicleNo) continue;
      newVehicleMap.set(vehicleNo, {
        imeiNo,
        districtId: row.district_id || null,
        onTrip: Number(row.on_trip || 0),
      });
    }

    // Subscribe new vehicles
    for (const [vehicleNo, metadata] of newVehicleMap.entries()) {
      const topic = topicForVehicle(vehicleNo);

      activeVehicleMap.set(vehicleNo, metadata);

      if (!subscribedTopics.has(topic)) {
        client.subscribe(topic, { qos: 0 }, (err) => {
          if (err) {
            console.error(`[MQTT] Subscribe failed: ${topic}`, err.message);
          } else {
            subscribedTopics.add(topic);
            // console.log(`[MQTT] Subscribed: ${topic}`);
          }
        });
      }
    }

    // Unsubscribe removed vehicles
    for (const topic of subscribedTopics) {
      const vehicleNo = getVehicleNoFromTopic(topic);

      if (!vehicleNo || !newVehicleMap.has(vehicleNo)) {
        client.unsubscribe(topic, (err) => {
          if (err) {
            console.error(`[MQTT] Unsubscribe failed: ${topic}`, err.message);
          } else {
            subscribedTopics.delete(topic);
            activeVehicleMap.delete(vehicleNo);
            latestLocationMap.delete(vehicleNo);
            // console.log(`[MQTT] Unsubscribed: ${topic}`);
          }
        });
      }
    }

    console.log(
      `[REFRESH] Active vehicles: ${newVehicleMap.size}, subscribed topics: ${subscribedTopics.size}`
    );
  } catch (error) {
    console.error("[REFRESH] Error while refreshing subscriptions:", error.message);
  }
}

// ================= MQTT MESSAGE HANDLER =================

function handleMqttMessage(topic, message) {
  try {
    const topicVehicleNo = getVehicleNoFromTopic(topic);

    if (!topicVehicleNo ) {
      console.warn(`[MQTT] Invalid topic received: ${topic}`);
      return;
    }

    if (!activeVehicleMap.has(topicVehicleNo)) {
      console.warn(`[MQTT] Vehicle not active in mapping: ${topicVehicleNo}`);
      return;
    }

    const packet = JSON.parse(message.toString());

    // Process only packet_type = L
    if (String(packet.packet_type || "").toUpperCase() !== "L") {
      return;
    }

    const vehicleNo = normalizeVehicleNo(
      packet.vehicle_number || packet.vehicle_no || topicVehicleNo
    );

    if (!vehicleNo || !activeVehicleMap.has(vehicleNo)) {
      return;
    }

    if (!activeVehicleMap.has(vehicleNo)) {
      console.warn(`[MQTT] Packet vehicle not mapped: ${vehicleNo}`);
      return;
    }

    const lat = Number(packet.latitude);
    const lng = Number(packet.longitude);

    if (!isValidLatLng(lat, lng)) {
      console.warn(`[MQTT] Invalid lat/lng for ${vehicleNo}:`, packet.latitude, packet.longitude);
      return;
    }

    const metadata = activeVehicleMap.get(vehicleNo);
    const gpsAtIST = getPacketUpdateTimeIST(packet);
    console.log("[GPS RECEIVED]", JSON.stringify({ vehicle: vehicleNo, server_date_time: packet.server_date_time, packet_date_time: packet.date_time, save_time_ist: gpsAtIST, latitude: lat, longitude: lng, speed_kph: Number(packet.speed || 0) }));

     // Matches the schema of the legacy polling payload EXACTLY (No property breaking)
    const livePayload = {
      vehicleNo: vehicleNo,
      lat: parseFloat(lat),
      lng: parseFloat(lng),
      speed: Number(packet.speed || 0),
      heading: Number(packet.heading || 0),
      ignitionStatus: Number(packet.ignition_status || 0),
      last_location_update: gpsAtIST,
      districtId: metadata.districtId,
      onTrip: metadata.onTrip,
    };

    // 1. Direct Publish to Redis Pub/Sub (Deliver to clients in milliseconds)
    redisPublisher?.publish(VEHICLE_STATUS_CHANNEL, JSON.stringify(livePayload));

    latestLocationMap.set(vehicleNo, {
      imei_no: metadata.imeiNo || packet.imei_no || null,
      vehicle_no: vehicleNo,
      lat,
      lng,
      speed: Number(packet.speed || 0),
      heading: Number(packet.heading || 0),
      ignition_status: Number(packet.ignition_status || 0),
      last_location_update: gpsAtIST,
      gps_at: gpsAtIST,
      received_at: Date.now(),
    });

    // console.log(
    //   `[MQTT] L packet cached: ${vehicleNo} | ${lat}, ${lng} | ${latestLocationMap.get(vehicleNo).last_location_update} | Speed: ${packet.speed} | Heading: ${packet.heading} | Ignition: ${packet.ignition_status} `
    // );
  } catch (error) {
    console.error("[MQTT] Message parse/process error:", error.message);
  }
}

// ================= BATCH DB UPDATE =================

async function batchUpdateLocations() {
  const records = Array.from(latestLocationMap.values());

  if (records.length === 0) {
    // console.log("[DB] No new GPS packets; no update performed");
    return;
  }

  try {
    await updatePaddyLocations(db, records);

    // console.log(
    //   `[DB] Batch updated. Vehicles: ${records.length}, affected rows: ${result.affectedRows}`
    // );

    // Remove only successfully flushed records
    for (const record of records) {
      const current = latestLocationMap.get(record.vehicle_no);

      if (current && current.received_at === record.received_at) {
        latestLocationMap.delete(record.vehicle_no);
      }
    }

  } catch (error) {
    console.error("[DB] Batch update error:", error.message);
  }
}

// ================= MQTT CLIENT =================

const client = mqtt.connect(MQTT_URL, {
  reconnectPeriod: 5000,
  connectTimeout: 30 * 1000,
  clean: true,
  clientId: `vehicle_gps_service_${Date.now()}`,
});

client.on("connect", async () => {
  console.log(`[MQTT] Connected: ${MQTT_URL}`);
  await refreshSubscriptions(client);
});

client.on("reconnect", () => {
  console.log("[MQTT] Reconnecting...");
});

client.on("close", () => {
  console.log("[MQTT] Connection closed");
});

client.on("error", (error) => {
  console.error("[MQTT] Error:", error.message);
});

client.on("message", handleMqttMessage);

// ================= INTERVAL JOBS =================

setInterval(() => {
  refreshSubscriptions(client);
}, SUBSCRIPTION_REFRESH_MS);

setInterval(() => {
  batchUpdateLocations();
}, DB_BATCH_UPDATE_MS);

// ================= STARTUP DB CHECK =================

async function startup() {
  try {
    await db.connect();

    const connection = await pool.getConnection();
    await connection.ping();
    connection.release();

    // console.log("[DB] MySQL connected successfully", { host: process.env.DB_HOST, database: process.env.DB_NAME, storage_timezone: "Asia/Kolkata", now_ist: formatISTDateTime() });
    // console.log("[SERVICE] Vehicle GPS MQTT service started");
  } catch (error) {
    console.error("[DB] MySQL connection failed:", error.message);
    process.exit(1);
  }
}

startup();

// ================= SAFE SHUTDOWN =================

process.on("SIGINT", async () => {

  try {
    await batchUpdateLocations();
    client.end(true);
    if (redisPublisher) await redisPublisher.quit();
    await db.close();
  } catch (error) {
    console.error("[SERVICE] Shutdown error:", error.message);
  }

  process.exit(0);
});

process.on("SIGTERM", async () => {

  try {
    await batchUpdateLocations();
    client.end(true);
    if (redisPublisher) await redisPublisher.quit();
    await db.close();
  } catch (error) {
    console.error("[SERVICE] Termination error:", error.message);
  }

  process.exit(0);
});
