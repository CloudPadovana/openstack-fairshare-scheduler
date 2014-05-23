/************ Drop: Database ***************/
DROP DATABASE IF EXISTS scheduler_priority_queue;

/************ Create: Database ***************/
CREATE DATABASE scheduler_priority_queue;

/************ Use: Database ***************/
USE scheduler_priority_queue;

/************ Update: Tables ***************/

/******************** Add Table: db_info ************************/
/* Build Table Structure */


CREATE TABLE db_info
(
  version VARCHAR(5) NOT NULL,
  creationTime TIMESTAMP  NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: priority_queue ************************/
/* Build Table Structure */

CREATE TABLE priority_queue
(
  id BIGINT NOT NULL AUTO_INCREMENT,
  message_context TEXT NOT NULL,
  request_spec TEXT NOT NULL,
  admin_password TEXT NULL,
  injected_files TEXT NULL,
  requested_networks TEXT NULL,
  is_first_time BOOL NULL,
  filter_properties TEXT NULL,
  legacy_bdm_in_spec BOOL NULL,
PRIMARY KEY (id)
) ENGINE=InnoDB;


/************ Insert values in db_info ***************/
insert into db_info (version, creationTime) values ('1.0', now());

commit;


