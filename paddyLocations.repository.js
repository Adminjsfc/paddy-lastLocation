// This service writes GPS DATETIME values in Asia/Kolkata (IST).
function schemaName() {
  const name = process.env.DB_NAME || 'sfc_cmr';
  if (!/^[a-zA-Z0-9_]+$/.test(name)) throw new Error('Invalid PADDY_DB_NAME');
  return `\`${name}\``;
}

async function fetchPaddyVehicles(pool) {
  const schema = schemaName();
  const [rows] = await pool.execute(`
    SELECT v.registration_no AS vehicle_no, i.imei AS imei_no,
      v.transporter_id, d.district_id,
      EXISTS(SELECT 1 FROM ${schema}.shipments s
        WHERE s.vehicle_id=v.id AND s.trip_status=1) AS on_trip
    FROM ${schema}.gps_devices d
    JOIN ${schema}.vehicles v ON v.id=d.vehicle_id
    JOIN ${schema}.imeis i ON i.id=d.imei_id
    WHERE d.active=1 AND i.active=1 AND v.active=1
  `);
  return rows;
}

async function updatePaddyLocations(db, records) {
  const schema = schemaName();
  // Bound parameters carry every packet value; match both vehicle and current IMEI.
  for (const record of records) {
    const args = [record.gps_at, record.lat, record.lng, record.speed,
      record.vehicle_no, record.imei_no, record.gps_at];
    const deviceResult = await db.writeExecute(`
      UPDATE ${schema}.gps_devices d
      JOIN ${schema}.vehicles v ON v.id=d.vehicle_id
      JOIN ${schema}.imeis i ON i.id=d.imei_id
      SET d.last_gps_at=?, d.latitude=?, d.longitude=?, d.speed_kph=?
      WHERE v.registration_no=? AND i.imei=? AND d.active=1 AND i.active=1
        AND (d.last_gps_at IS NULL OR d.last_gps_at < ?)
    `, args);
    const shipmentResult = await db.writeExecute(`
      UPDATE ${schema}.shipments s
      JOIN ${schema}.vehicles v ON v.id=s.vehicle_id
      JOIN ${schema}.gps_devices d ON d.vehicle_id=v.id
      JOIN ${schema}.imeis i ON i.id=d.imei_id
      SET s.last_gps_at=?
      WHERE v.registration_no=? AND i.imei=? AND d.active=1 AND i.active=1
        AND s.trip_status IN (0,1) AND s.status <> 'CANCELLED'
        AND (s.last_gps_at IS NULL OR s.last_gps_at < ?)
    `, [record.gps_at, record.vehicle_no, record.imei_no, record.gps_at]);
    // console.log('[DB GPS UPDATE]', JSON.stringify({
    //   vehicle: record.vehicle_no,
    //   last_gps_at_ist: record.gps_at,
    //   latitude: record.lat,
    //   longitude: record.lng,
    //   speed_kph: record.speed,
    //   gps_devices_affected: deviceResult?.affectedRows ?? 0,
    //   shipments_affected: shipmentResult?.affectedRows ?? 0,
    //   shipment_rule: 'trip_status 0/1 only; status 2 remains unchanged',
    //   zero_rows: 'No matching eligible row, or stored GPS time is equal/newer'
    // }));
  }
}

module.exports = { fetchPaddyVehicles, updatePaddyLocations };
