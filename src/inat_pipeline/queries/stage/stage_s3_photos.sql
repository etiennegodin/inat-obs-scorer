-- photos
CREATE INDEX "idx_photos_photo_uuid" ON "photos" ("photo_uuid");
CREATE INDEX "idx_photos_observation_uuid" ON "photos" ("observation_uuid");
CREATE INDEX "idx_photos_photo_id" ON "photos" ("photo_id");
CREATE INDEX "idx_photos_observer_id" ON "photos" ("observer_id");
CREATE INDEX "idx_photos_license" ON "photos" ("license");
