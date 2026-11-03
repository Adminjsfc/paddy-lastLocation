Run npm start in last-location. This service connects directly to the Paddy database using DB_HOST, DB_PORT, DB_USER, DB_PASSWORD and DB_NAME in last-location/.env. These settings have been copied from paddy/.env. No changes to the Paddy application are required.

Active Paddy GPS vehicles are subscribed on gps/<vehicle>/data. Location packets update gps_devices.last_gps_at, latitude, longitude and speed_kph every minute. shipments.last_gps_at updates only for trip_status 0 or 1, excluding cancelled shipments. Completed shipment timestamps stay unchanged. Older fixes cannot overwrite newer fixes. This service stores new GPS timestamps in IST (Asia/Kolkata), using 24-hour format. Existing timestamps are not migrated. The Paddy API uses UTC and must account for this storage convention when reading these fields.

Redis publishing is optional and enabled only when REDIS_HOST is configured. MQTT_URL selects the broker. Run npm test for the SQL guard checks.

Compatible with backend migration 073 (independent vehicle registration): subscriptions read district_id from vehicles. Transporter fields are not queried or included in Redis messages. districtId reflects the vehicle's district.
