const test = require('node:test');
const assert = require('node:assert/strict');
const { fetchPaddyVehicles, updatePaddyLocations } = require('./paddyLocations.repository');

test('vehicle subscriptions use independent vehicle fields after migration 073', async () => {
  const rows = [{ vehicle_no: 'JH01AB1234', imei_no: '123456789012345',
    district_id: 12, on_trip: 1 }];
  let query;
  const result = await fetchPaddyVehicles({ execute: async sql => {
    query = sql;
    return [rows];
  } });
  assert.equal(result, rows);
  assert.doesNotMatch(query, /transporter/i);
  assert.match(query, /v\.district_id/);
  assert.match(query, /d\.active=1 AND i\.active=1 AND v\.active=1/);
  assert.match(query, /s\.vehicle_id=v\.id AND s\.trip_status=1/);
});

test('Paddy location sync binds all four fields and protects completed trips and newer fixes', async () => {
  const calls = [];
  await updatePaddyLocations({ writeExecute: async (sql, args) => calls.push({ sql, args }) }, [{
    vehicle_no: 'JH01AB1234', imei_no: '123456789012345',
    gps_at: '2026-10-22 07:00:00', lat: 23.1, lng: 85.2, speed: 32
  }]);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].args.slice(0, 4), ['2026-10-22 07:00:00', 23.1, 85.2, 32]);
  assert.deepEqual(calls[1].args, ['2026-10-22 07:00:00', 'JH01AB1234', '123456789012345', '2026-10-22 07:00:00']);
  for (const { sql, args } of calls) {
    assert.equal((sql.match(/\?/g) || []).length, args.length);
    assert.match(sql, /last_gps_at < \?/);
    assert.match(sql, /i\.imei=\?/);
  }
  assert.match(calls[1].sql, /s\.trip_status IN \(0,1\)/);
});
