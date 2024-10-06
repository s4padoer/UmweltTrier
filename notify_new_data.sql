CREATE OR REPLACE FUNCTION notify_after_insert()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('update_data', 'New data inserted');
    RETURN NULL;
END;
$$
 LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER notify_after_insert_trigger
AFTER INSERT ON temperatur
FOR EACH STATEMENT
EXECUTE FUNCTION notify_after_insert();

CREATE OR REPLACE TRIGGER notify_after_insert_trigger
AFTER INSERT ON wassertemperatur_mosel
FOR EACH STATEMENT
EXECUTE FUNCTION notify_after_insert();

CREATE OR REPLACE TRIGGER notify_after_insert_trigger
AFTER INSERT ON luftqualitaet
FOR EACH STATEMENT
EXECUTE FUNCTION notify_after_insert();
