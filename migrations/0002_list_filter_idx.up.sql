-- Listing by status or channel was walking the whole created_at index and filtering row by row.
CREATE INDEX notifications_status_list_idx ON notifications (status, created_at DESC, id DESC);
CREATE INDEX notifications_channel_list_idx ON notifications (channel, created_at DESC, id DESC);
